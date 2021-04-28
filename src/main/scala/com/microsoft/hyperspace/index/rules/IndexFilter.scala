/*
 * Copyright (2020) The Hyperspace Project Authors.
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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.index.{IndexConstants, IndexLogEntry, IndexLogEntryTags, LogicalPlanSignatureProvider}
import com.microsoft.hyperspace.index.IndexLogEntryTags.{HYBRIDSCAN_RELATED_CONFIGS, IS_HYBRIDSCAN_CANDIDATE}
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.{PlanToIndexesMap, PlanToSelectedIndexMap}
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.util.{HyperspaceConf, ResolverUtils}

trait IndexFilter {

  /**
   * @return Failure reason for filtered out indexes.
   */
  def reason: String
}

/**
 * IndexFilter used in CandidateIndexCollector.
 */
trait SourcePlanIndexFilter extends IndexFilter with ActiveSparkSession {

  /**
   * Filter out indexes for the given source plan.
   *
   * @param plan Source plan
   * @param indexes Indexes
   * @return Indexes which meet conditions of Filter
   */
  def apply(plan: LogicalPlan, indexes: Seq[IndexLogEntry]): Seq[IndexLogEntry]
}

/**
 * IndexFilter used in HyperspaceRule.
 */
trait QueryPlanIndexFilter extends IndexFilter with ActiveSparkSession {

  /**
   * Filter out candidate indexes for the given query plan.
   *
   * @param plan Query plan
   * @param candidateIndexes Map of source plan to candidate indexes
   * @return Map of source plan to applicable indexes which meet conditions of Filter
   */
  def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap
}

/**
 * IndexFilter used in ranking applicable indexes.
 */
trait IndexRankFilter extends IndexFilter with ActiveSparkSession {

  /**
   * Rank best index for the given query plan.
   *
   * @param plan Query plan
   * @param applicableIndexes Map of source plan to applicable indexes
   * @return Map of source plan to selected index
   */
  def apply(plan: LogicalPlan, applicableIndexes: PlanToIndexesMap): PlanToSelectedIndexMap
}

/**
 * Check if an index contains all output columns of the given source plan.
 */
object ColumnSchemaFilter extends SourcePlanIndexFilter {
  override def apply(plan: LogicalPlan, indexes: Seq[IndexLogEntry]): Seq[IndexLogEntry] = {
    val relationColumnsNames = plan.output.map(_.name)
    indexes.filter { index =>
      ResolverUtils
        .resolve(spark, index.indexedColumns ++ index.includedColumns, relationColumnsNames)
        .isDefined
    }
  }
  override def reason: String = {
    "Column schema does not match."
  }
}

/**
 * Check if an index can leverage source data of the given source plan.
 */
object FileSignatureFilter extends SourcePlanIndexFilter {
  override def apply(plan: LogicalPlan, indexes: Seq[IndexLogEntry]): Seq[IndexLogEntry] = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    val hybridScanEnabled = HyperspaceConf.hybridScanEnabled(spark)
    val hybridScanDeleteEnabled = HyperspaceConf.hybridScanDeleteEnabled(spark)

    if (hybridScanEnabled) {
      val relation = provider.getRelation(plan)
      prepareHybridScanCandidateSelection(spark, relation.plan, indexes)
      indexes.flatMap { index =>
        getHybridScanCandidate(relation, index, hybridScanDeleteEnabled)
      }
    } else {
      val relation = provider.getRelation(plan)
      // Map of a signature provider to a signature generated for the given plan.
      val signatureMap = mutable.Map[String, Option[String]]()
      indexes.filter(index => signatureValid(relation, index, signatureMap))
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
      spark: SparkSession,
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
      index: IndexLogEntry,
      hybridScanDeleteEnabled: Boolean): Option[IndexLogEntry] = {
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

        val deletedCnt = entry.sourceFileInfoSet.size - commonCnt
        val isAppendAndDeleteCandidate = hybridScanDeleteEnabled && entry.hasLineageColumn &&
          commonCnt > 0 &&
          appendedBytesRatio < HyperspaceConf.hybridScanAppendedRatioThreshold(spark) &&
          deletedBytesRatio < HyperspaceConf.hybridScanDeletedRatioThreshold(spark)

        // For append-only Hybrid Scan, deleted files are not allowed.
        lazy val isAppendOnlyCandidate = deletedCnt == 0 && commonCnt > 0 &&
          appendedBytesRatio < HyperspaceConf.hybridScanAppendedRatioThreshold(spark)

        val isCandidate = isAppendAndDeleteCandidate || isAppendOnlyCandidate
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

  override def reason: String = {
    "Underlying data is changed. Hybrid Scan Configs: \n" +
      s"${IndexConstants.INDEX_HYBRID_SCAN_ENABLED}: ${HyperspaceConf.hybridScanEnabled(spark)}, " +
      s"${IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD}: " +
      s"${HyperspaceConf.hybridScanAppendedRatioThreshold(spark)}, " +
      s"${IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD}: " +
      s"${HyperspaceConf.hybridScanDeletedRatioThreshold(spark)}"
  }
}
