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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags, LogicalPlanSignatureProvider}
import com.microsoft.hyperspace.index.IndexLogEntryTags.{HYBRIDSCAN_RELATED_CONFIGS, IS_HYBRIDSCAN_CANDIDATE}
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.{PlanToIndexesMap, PlanToSelectedIndexMap}
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.util.{HyperspaceConf, ResolverUtils}

trait IndexFilter extends ActiveSparkSession {

  /**
   * Append a given reason string to WHYNOT_REASON tag of the index if the condition is false and
   * WHYNOT_ENABLED tag is set to the index.
   *
   * @param condition Flag for reason string
   * @param reasonString Informational message in case condition is false.
   * @param keyPair A pair of plan and index to tag
   */
  protected def setReasonTag(condition: Boolean, reasonString: => String)(
      implicit keyPair: (LogicalPlan, IndexLogEntry)): Unit = {
    val (plan, index) = keyPair
    if (!condition && index.getTagValue(IndexLogEntryTags.WHYNOT_ENABLED).getOrElse(false)) {
      val prevReason =
        index.getTagValue(plan, IndexLogEntryTags.WHYNOT_REASON).getOrElse(Nil)
      index.setTagValue(plan, IndexLogEntryTags.WHYNOT_REASON, prevReason :+ reasonString)
    }
  }

  /**
   * Append the reason string to WHYNOT_REASON tag for the given index
   * if the result of the function is false and WHYNOT_ENABLED tag is set to the index.
   *
   * @param reasonString Informational message in case condition is false.
   * @param f Function for a condition
   * @param keyPair A pair of plan and index to tag
   * @return Result of the given function
   */
  protected def withReasonTag(reasonString: => String)(f: => Boolean)(
      implicit keyPair: (LogicalPlan, IndexLogEntry)): Boolean = {
    val ret = f
    setReasonTag(ret, reasonString)(keyPair)
    ret
  }

  /**
   * Append the reason string to WHYNOT_REASON tag for the given list of indexes
   * if the result of the function is false and WHYNOT_ENABLED tag is set to the index.
   *
   * @param reasonString Informational message in case condition is false.
   * @param f Function for a condition
   * @param keyPair A pair of plan and index list to tag
   * @return Result of the given function
   */
  protected def withReasonTagAll(reasonString: => String)(f: => Boolean)(
      implicit keyPair: (LogicalPlan, Seq[IndexLogEntry])): Boolean = {
    val ret = f
    keyPair._2.foreach { index =>
      setReasonTag(ret, reasonString)(keyPair._1, index)
    }
    ret
  }

  /**
   * Append the reason string to WHYNOT_REASON tag for the given list of indexes
   * if WHYNOT_ENABLED tag is set to the indexes.
   *
   * @param reasonString Informational message in case condition is false.
   * @param keyPair A pair of plan and index list to tag
   * @return Result of the given function
   */
  protected def setReasonTagAll(reasonString: => String)(
      implicit keyPair: (LogicalPlan, Seq[IndexLogEntry])): Unit = {
    keyPair._2.foreach { index =>
      setReasonTag(false, reasonString)(keyPair._1, index)
    }
  }
}

/**
 * IndexFilter used in CandidateIndexCollector.
 */
trait SourcePlanIndexFilter extends IndexFilter {

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
trait QueryPlanIndexFilter extends IndexFilter {

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
trait IndexRankFilter extends IndexFilter {

  /**
   * Rank best index for the given query plan.
   *
   * @param plan Query plan
   * @param applicableIndexes Map of source plan to applicable indexes
   * @return Map of source plan to selected index
   */
  def apply(plan: LogicalPlan, applicableIndexes: PlanToIndexesMap): PlanToSelectedIndexMap

  /**
   * Set WHYNOT_REASON tag for unselected indexes.
   *
   * @param plan Plan to tag
   * @param indexes Indexes to tag
   * @param selectedIndex Selected index
   */
  protected def setRankReasonTag(
      plan: LogicalPlan,
      indexes: Seq[IndexLogEntry],
      selectedIndex: IndexLogEntry): Unit = {
    indexes.foreach { index =>
      setReasonTag(
        selectedIndex.name.equals(index.name),
        s"Another candidate index is applied: ${selectedIndex.name}")(plan, index)
    }
  }
}

/**
 * Check if the given source plan contains all index columns.
 */
object ColumnSchemaFilter extends SourcePlanIndexFilter {
  override def apply(plan: LogicalPlan, indexes: Seq[IndexLogEntry]): Seq[IndexLogEntry] = {
    val relationColumnNames = plan.output.map(_.name)

    indexes.filter { index =>
      withReasonTag(
        s"Column Schema does not match. " +
          s"Relation columns: [${relationColumnNames.mkString(", ")}], " +
          s"Index columns: [${(index.indexedColumns ++ index.includedColumns).mkString(", ")}]") {
        ResolverUtils
          .resolve(spark, index.indexedColumns ++ index.includedColumns, relationColumnNames)
          .isDefined
      }(plan, index)
    }
  }
}

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
        withReasonTag(s"Index signature does not match. Try Hybrid Scan or refreshIndex.") {
          signatureValid(relation, index, signatureMap)
        }(plan, index)
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
    // Tag to original index log entry to check the reason string with the given log entry.
    implicit val reasonTagKeyPair = (relation.plan, index)

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

        lazy val hasLineageColumnCond =
          withReasonTag("Lineage column does not exist.")(entry.hasLineageColumn)
        lazy val hasCommonFilesCond = withReasonTag("No common files.")(commonCnt > 0)
        lazy val appendThresholdCond = withReasonTag(
          s"Appended bytes ratio ($appendedBytesRatio) is larger than " +
            s"threshold config ${HyperspaceConf.hybridScanAppendedRatioThreshold(spark)}") {
          appendedBytesRatio < HyperspaceConf.hybridScanAppendedRatioThreshold(spark)
        }
        lazy val deleteThresholdCond = withReasonTag(
          s"Deleted bytes ratio ($deletedBytesRatio) is larger than " +
            s"threshold config ${HyperspaceConf.hybridScanDeletedRatioThreshold(spark)}") {
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
