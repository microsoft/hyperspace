/*
 * Copyright (2021) The Hyperspace Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.hyperspace.index.rules

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags, LogicalPlanSignatureProvider}
import com.microsoft.hyperspace.index.IndexLogEntryTags.{HYBRIDSCAN_RELATED_CONFIGS, IS_HYBRIDSCAN_CANDIDATE}
import com.microsoft.hyperspace.index.plananalysis.FilterReasons
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.util.HyperspaceConf

/**
 * Check if an index can leverage source data of the given source plan.
 */
object FileSignatureFilter extends SourcePlanIndexFilter {

  /**
   * Filter the given indexes by matching signatures.
   *
   * If Hybrid Scan is enabled, it compares the file metadata directly, and does not
   * match signatures. By doing that, we could perform file-level comparison between
   * index data files and the input files of the given plan. If appended files and
   * deleted files are less than threshold configs, the index is not filtered out.
   * Also, HYBRIDSCAN_REQUIRED tag is set as true if there is any of appended or deleted files,
   * for the plan transformation function in application step.
   *
   * @param plan Source plan
   * @param indexes Indexes
   * @return Indexes which meet conditions of Filter
   */
  override def apply(plan: LogicalPlan, indexes: Seq[IndexLogEntry]): Seq[IndexLogEntry] = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    val hybridScanEnabled = HyperspaceConf.hybridScanEnabled(spark)
    if (hybridScanEnabled) {
      val relation = provider.getRelation(plan)
      prepareHybridScanCandidateSelection(relation.plan, indexes)
      indexes.flatMap { index =>
        getHybridScanCandidate(relation, index)
      }
    } else {
      val relation = provider.getRelation(plan)
      // Map of a signature provider to a signature generated for the given plan.
      val signatureMap = mutable.Map[String, Option[String]]()
      indexes.filter { index =>
        withFilterReasonTag(plan, index, FilterReasons.SourceDataChanged()) {
          signatureValid(relation, index, signatureMap)
        }
      }
    }
  }

  private def signatureValid(
      relation: FileBasedRelation,
      entry: IndexLogEntry,
      signatureMap: mutable.Map[String, Option[String]]): Boolean = {
    entry.withCachedTag(relation.plan, IndexLogEntryTags.SIGNATURE_MATCHED) {
      val sourcePlanSignatures = entry.source.plan.properties.fingerprint.properties.signatures
      assert(sourcePlanSignatures.length == 1)
      val sourcePlanSignature = sourcePlanSignatures.head

      signatureMap.getOrElseUpdate(
        sourcePlanSignature.provider,
        LogicalPlanSignatureProvider
          .create(sourcePlanSignature.provider)
          .signature(relation.plan)) match {
        case Some(s) => s.equals(sourcePlanSignature.value)
        case None => false
      }
    }
  }

  private def prepareHybridScanCandidateSelection(
      plan: LogicalPlan,
      indexes: Seq[IndexLogEntry]): Unit = {
    assert(HyperspaceConf.hybridScanEnabled(spark))
    val curConfigs = Seq(
      HyperspaceConf.hybridScanAppendedRatioThreshold(spark).toString,
      HyperspaceConf.hybridScanDeletedRatioThreshold(spark).toString)

    indexes.foreach { index =>
      val taggedConfigs = index.getTagValue(plan, HYBRIDSCAN_RELATED_CONFIGS)
      if (taggedConfigs.isEmpty || !taggedConfigs.get.equals(curConfigs)) {
        // Need to reset cached tags as these config changes can change the result.
        index.unsetTagValue(plan, IS_HYBRIDSCAN_CANDIDATE)
        index.setTagValue(plan, HYBRIDSCAN_RELATED_CONFIGS, curConfigs)
      }
    }
  }

  private def getHybridScanCandidate(
      relation: FileBasedRelation,
      index: IndexLogEntry): Option[IndexLogEntry] = {
    // TODO: As in [[PlanSignatureProvider]], Source plan signature comparison is required to
    //  support arbitrary source plans at index creation.
    //  See https://github.com/microsoft/hyperspace/issues/158

    val entry = relation.closestIndex(index)

    val isHybridScanCandidate =
      entry.withCachedTag(relation.plan, IndexLogEntryTags.IS_HYBRIDSCAN_CANDIDATE) {
        // Find the number of common files between the source relation and index source files.
        // The total size of common files are collected and tagged for candidate.
        val (commonCnt, commonBytes) = relation.allFileInfos.foldLeft(0L, 0L) { (res, f) =>
          if (entry.sourceFileInfoSet.contains(f)) {
            (res._1 + 1, res._2 + f.size) // count, total bytes
          } else {
            res
          }
        }

        val appendedBytesRatio = 1 - commonBytes / relation.allFileSizeInBytes.toFloat
        val deletedBytesRatio = 1 - commonBytes / entry.sourceFilesSizeInBytes.toFloat

        // Tag to original index log entry to check the reason string with the given log entry.
        lazy val hasLineageColumnCond =
          withFilterReasonTag(relation.plan, index, FilterReasons.NoDeleteSupport()) {
            entry.derivedDataset.canHandleDeletedFiles
          }
        lazy val hasCommonFilesCond =
          withFilterReasonTag(relation.plan, index, FilterReasons.NoCommonFiles()) {
            commonCnt > 0
          }

        val hybridScanAppendThreshold = HyperspaceConf.hybridScanAppendedRatioThreshold(spark)
        val hybridScanDeleteThreshold = HyperspaceConf.hybridScanDeletedRatioThreshold(spark)
        lazy val appendThresholdCond = withFilterReasonTag(
          relation.plan,
          index,
          FilterReasons.TooMuchAppended(
            appendedBytesRatio.toString,
            hybridScanAppendThreshold.toString)) {
          appendedBytesRatio < hybridScanAppendThreshold
        }
        lazy val deleteThresholdCond = withFilterReasonTag(
          relation.plan,
          index,
          FilterReasons.TooMuchDeleted(
            deletedBytesRatio.toString,
            hybridScanDeleteThreshold.toString)) {
          deletedBytesRatio < HyperspaceConf.hybridScanDeletedRatioThreshold(spark)
        }

        // For append-only Hybrid Scan, deleted files are not allowed.
        val deletedCnt = entry.sourceFileInfoSet.size - commonCnt

        val isCandidate = if (deletedCnt == 0) {
          hasCommonFilesCond && appendThresholdCond
        } else {
          hasLineageColumnCond && hasCommonFilesCond && appendThresholdCond && deleteThresholdCond
        }

        if (isCandidate) {
          entry.setTagValue(
            relation.plan,
            IndexLogEntryTags.COMMON_SOURCE_SIZE_IN_BYTES,
            commonBytes)

          // If there is no change in source dataset, the index will be applied by
          // RuleUtils.transformPlanToUseIndexOnlyScan.
          entry.setTagValue(
            relation.plan,
            IndexLogEntryTags.HYBRIDSCAN_REQUIRED,
            !(commonCnt == entry.sourceFileInfoSet.size
              && commonCnt == relation.allFileInfos.size))
        }
        isCandidate
      }
    if (isHybridScanCandidate) {
      Some(entry)
    } else {
      None
    }
  }
}
