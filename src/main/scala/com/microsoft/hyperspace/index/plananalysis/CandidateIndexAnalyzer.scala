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

package com.microsoft.hyperspace.index.plananalysis

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.hyperspace.utils.DataFrameUtils

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags}
import com.microsoft.hyperspace.index.rules.{CandidateIndexCollector, ScoreBasedIndexPlanOptimizer}

object CandidateIndexAnalyzer extends Logging {
  def whyNotIndexString(
      spark: SparkSession,
      df: DataFrame,
      indexName: String,
      extended: Boolean): String = {
    val (planWithHyperspace, filterReasons, applicableIndexes) =
      collectAnalysisResult(spark, df, indexName)
    generateWhyNotString(
      spark,
      df.queryExecution.optimizedPlan,
      planWithHyperspace,
      filterReasons,
      applicableIndexes,
      extended)
  }

  def whyNotIndexesString(spark: SparkSession, df: DataFrame, extended: Boolean): String = {
    val (planWithHyperspace, filterReasons, applicableIndexes) =
      collectAnalysisResult(spark, df)
    generateWhyNotString(
      spark,
      df.queryExecution.optimizedPlan,
      planWithHyperspace,
      filterReasons,
      applicableIndexes,
      extended)
  }

  def applicableIndexInfoString(spark: SparkSession, df: DataFrame): String = {
    val (_, applicableIndexes) = collectApplicableIndexInfo(spark, df)
    generateApplicableIndexInfoString(spark, df.queryExecution.optimizedPlan, applicableIndexes)
  }

  private def cleanupAnalysisTags(indexes: Seq[IndexLogEntry]): Unit = {
    indexes.foreach { index =>
      index.unsetTagValueForAllPlan(IndexLogEntryTags.INDEX_PLAN_ANALYSIS_ENABLED)
      index.unsetTagValueForAllPlan(IndexLogEntryTags.FILTER_REASONS)
      index.unsetTagValueForAllPlan(IndexLogEntryTags.APPLICABLE_INDEX_RULES)
    }
  }

  private def prepareTagsForAnalysis(indexes: Seq[IndexLogEntry]): Unit = {
    indexes.foreach { index =>
      index.setTagValue(IndexLogEntryTags.INDEX_PLAN_ANALYSIS_ENABLED, true)

      // Clean up previous reason tags.
      index.unsetTagValueForAllPlan(IndexLogEntryTags.FILTER_REASONS)
      index.unsetTagValueForAllPlan(IndexLogEntryTags.APPLICABLE_INDEX_RULES)
    }
  }

  private def getSubPlanLoc(
      originalPlanStrings: Seq[String],
      plan: LogicalPlan,
      numberSpaces: Int): String = {
    val planStrLines = plan.toString.split('\n')

    val firstLine = planStrLines.head
    val firstLineStr = originalPlanStrings.find { s =>
      s.endsWith(firstLine) &&
      s.substring(numberSpaces, s.length - firstLine.length).distinct.forall(" +-:".contains(_))
    }
    val numberRegex = raw"([0-9]+)".r
    val startLineNumber = if (firstLineStr.isDefined) {
      numberRegex.findFirstIn(firstLineStr.get).get.toInt
    } else {
      -1
    }
    s"${plan.nodeName} @$startLineNumber"
  }

  private def generateApplicableIndexInfoString(
      spark: SparkSession,
      planWithoutHyperspace: LogicalPlan,
      applicableIndexes: Seq[(IndexLogEntry, Seq[(LogicalPlan, Seq[String])])]): String = {
    val stringBuilder = new StringBuilder
    val originalPlanString = planWithoutHyperspace.numberedTreeString.split('\n')

    // to Dataframe
    // sub plan line number, index name, rule name
    val numberSpaces = originalPlanString.length.toString.length + 1
    val res = applicableIndexes
      .flatMap {
        case (index: IndexLogEntry, applicableRules: Seq[(LogicalPlan, Seq[String])]) =>
          applicableRules.flatMap {
            case (plan, ruleNames) =>
              val subPlanLocStr = getSubPlanLoc(originalPlanString, plan, numberSpaces)
              ruleNames.map { ruleName =>
                (subPlanLocStr, index.name, index.derivedDataset.kindAbbr, ruleName)
              }
          }
      }
      .sortBy(r => (r._1, r._2, r._3, r._4))
      .distinct

    if (res.isEmpty) {
      return "No applicable indexes. Try hyperspace.whyNot()"
    }
    val newLine = System.lineSeparator()
    stringBuilder.append("Plan without Hyperspace:")
    stringBuilder.append(newLine)
    stringBuilder.append(newLine)
    stringBuilder.append(originalPlanString.mkString(newLine))
    stringBuilder.append(newLine)
    stringBuilder.append(newLine)


    import spark.implicits._
    val df = res.toDF("SubPlan", "IndexName", "IndexType", "RuleName")

    // '\n' is used in `showString` as a delimiter for newlines.
    df.showString(res.size, truncate = 0).split('\n').foreach { l =>
      stringBuilder.append(l)
      stringBuilder.append(newLine)
    }
    stringBuilder.toString
  }

  private def generateWhyNotString(
      spark: SparkSession,
      planWithoutHyperspace: LogicalPlan,
      planWithHyperspace: LogicalPlan,
      filterReasons: Seq[(IndexLogEntry, Seq[(LogicalPlan, Seq[FilterReason])])],
      applicableIndexes: Seq[(IndexLogEntry, Seq[(LogicalPlan, Seq[String])])],
      extended: Boolean = false): String = {
    val stringBuilder = new StringBuilder
    val originalPlanString = planWithoutHyperspace.numberedTreeString.split('\n')
    val newLine = System.lineSeparator()
    stringBuilder.append(newLine)
    stringBuilder.append("=============================================================")
    stringBuilder.append(newLine)
    stringBuilder.append("Plan with Hyperspace & Summary:")
    stringBuilder.append(newLine)
    stringBuilder.append("=============================================================")
    stringBuilder.append(newLine)
    stringBuilder.append(planWithHyperspace.toString)
    stringBuilder.append(newLine)

    def printIndexNames(indexNames: Seq[String]): Unit = {
      indexNames.foreach { idxName =>
        stringBuilder.append(s"- $idxName")
        stringBuilder.append(newLine)
      }
      if (indexNames.isEmpty) {
        stringBuilder.append("- No such index found.")
      }
      stringBuilder.append(newLine)
    }

    stringBuilder.append("Applied indexes:")
    stringBuilder.append(newLine)
    val indexNameRegex = "Hyperspace\\(Type: .+, Name: (.+), LogVersion: [0-9]+\\)".r
    val appliedIndexNames = indexNameRegex
      .findAllIn(planWithHyperspace.toString)
      .matchData
      .map { matchStr =>
        matchStr.group(1)
      }
      .toSeq
      .distinct
      .sorted
    printIndexNames(appliedIndexNames)

    stringBuilder.append("Applicable indexes, but not applied due to priority:")
    stringBuilder.append(newLine)
    val applicableButNotAppliedIndexNames =
      applicableIndexes.map(_._1.name).distinct.sorted.filterNot(appliedIndexNames.contains(_))
    printIndexNames(applicableButNotAppliedIndexNames)

    // Covert reasons to Dataframe rows.
    // (sub plan location string, index name, index type, reason code, arg strings, verbose string)
    val numberSpaces = originalPlanString.length.toString.length + 1
    val res = filterReasons
      .flatMap {
        case (index: IndexLogEntry, reasonsForIndex: Seq[(LogicalPlan, Seq[FilterReason])]) =>
          reasonsForIndex.flatMap {
            case (plan, reasons) =>
              val subPlanLocStr = getSubPlanLoc(originalPlanString, plan, numberSpaces)
              reasons.map { reason =>
                (
                  subPlanLocStr,
                  index.name,
                  index.derivedDataset.kindAbbr,
                  reason.codeStr,
                  reason.argStr,
                  reason.verboseStr)
              }
          }
      }
      .sortBy(r => (r._1, r._2, r._3, r._4, r._5))
      .distinct

    import spark.implicits._
    val df = res.toDF("SubPlan", "IndexName", "IndexType", "Reason", "Message", "VerboseMessage")

    val outputDf = if (extended) {
      df
    } else {
      df.drop("VerboseMessage")
        .filter(!$"Reason".like("COL_SCHEMA_MISMATCH"))
    }

    stringBuilder.append("Non-applicable indexes - index is outdated:")
    stringBuilder.append(newLine)
    val indexNamesForOutdated = res
      .filter(row => row._4.equals("SOURCE_DATA_CHANGE"))
      .map(_._2)
      .distinct
      .sorted
      .filterNot(appliedIndexNames.contains(_))
      .filterNot(applicableButNotAppliedIndexNames.contains(_))
    printIndexNames(indexNamesForOutdated)

    stringBuilder.append(newLine)
    stringBuilder.append("Non-applicable indexes - no applicable query plan:")
    stringBuilder.append(newLine)
    val indexNamesForNoApplicablePlan = res
      .filterNot(row =>
        row._4.equals("COL_SCHEMA_MISMATCH") || row._4.equals("SOURCE_DATA_CHANGE"))
      .map(_._2)
      .distinct
      .sorted
      .filterNot(appliedIndexNames.contains(_))
      .filterNot(applicableButNotAppliedIndexNames.contains(_))
    printIndexNames(indexNamesForNoApplicablePlan)
    stringBuilder.append(newLine)

    stringBuilder.append("For more information, please visit: ")
    stringBuilder.append("https://microsoft.github.io/hyperspace/docs/why-not-result-analysis")

    stringBuilder.append(newLine)
    stringBuilder.append(newLine)
    stringBuilder.append("=============================================================")
    stringBuilder.append(newLine)
    stringBuilder.append("Plan without Hyperspace & WhyNot reasons:")
    stringBuilder.append(newLine)
    stringBuilder.append("=============================================================")
    stringBuilder.append(newLine)
    stringBuilder.append(originalPlanString.mkString(newLine))
    stringBuilder.append(newLine)
    stringBuilder.append(newLine)

    // '\n' is used in `showString` as a delimiter for newlines.
    outputDf.showString(res.size, truncate = 0).split('\n').foreach { l =>
      stringBuilder.append(l)
      stringBuilder.append(newLine)
    }
    stringBuilder.toString
  }

  private def collectApplicableIndexInfo(
      spark: SparkSession,
      df: DataFrame): (LogicalPlan, Seq[(IndexLogEntry, Seq[(LogicalPlan, Seq[String])])]) = {
    val (transformedPlan, _, applicableIndexInfo) = collectAnalysisResult(spark, df)
    (transformedPlan, applicableIndexInfo)
  }

  private def collectAnalysisResult(spark: SparkSession, df: DataFrame): (
      LogicalPlan,
      Seq[(IndexLogEntry, Seq[(LogicalPlan, Seq[FilterReason])])],
      Seq[(IndexLogEntry, Seq[(LogicalPlan, Seq[String])])]) = {
    val plan = df.queryExecution.optimizedPlan

    val indexManager = Hyperspace.getContext(spark).indexCollectionManager
    val allActiveIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    if (allActiveIndexes.isEmpty) {
      throw HyperspaceException(s"No available ACTIVE indexes")
    }
    applyHyperspaceForAnalysis(plan, allActiveIndexes)
  }

  private def collectAnalysisResult(spark: SparkSession, df: DataFrame, indexName: String): (
      LogicalPlan,
      Seq[(IndexLogEntry, Seq[(LogicalPlan, Seq[FilterReason])])],
      Seq[(IndexLogEntry, Seq[(LogicalPlan, Seq[String])])]) = {
    val plan = df.queryExecution.optimizedPlan

    val indexManager = Hyperspace.getContext(spark).indexCollectionManager
    val allIndexes =
      indexManager.getIndexes().filter(!_.state.equals(Constants.States.DOESNOTEXIST))
    val targetIndex = allIndexes.find(index => index.name.equals(indexName))

    if (targetIndex.isEmpty) {
      throw HyperspaceException(s"Index with name $indexName could not be found.")
    } else if (!targetIndex.get.state.equals(Constants.States.ACTIVE)) {
      throw HyperspaceException(
        s"Index with name $indexName is not ACTIVE state: ${targetIndex.get.state}")
    }

    val allActiveIndexes = allIndexes.filter(_.state.equals(Constants.States.ACTIVE))
    applyHyperspaceForAnalysis(plan, allActiveIndexes, Some(indexName))
  }

  private def applyHyperspaceForAnalysis(
      plan: LogicalPlan,
      indexes: Seq[IndexLogEntry],
      indexName: Option[String] = None): (
      LogicalPlan,
      Seq[(IndexLogEntry, Seq[(LogicalPlan, Seq[FilterReason])])],
      Seq[(IndexLogEntry, Seq[(LogicalPlan, Seq[String])])]) = {
    try {
      prepareTagsForAnalysis(indexes)
      val candidateIndexes = CandidateIndexCollector.apply(plan, indexes)
      val transformedPlan = new ScoreBasedIndexPlanOptimizer().apply(plan, candidateIndexes)
      (
        transformedPlan,
        indexes
          .filter(i => indexName.isEmpty || indexName.get.equals(i.name))
          .map(i => (i, i.getTagValuesForAllPlan(IndexLogEntryTags.FILTER_REASONS))),
        indexes
          .map(i => (i, i.getTagValuesForAllPlan(IndexLogEntryTags.APPLICABLE_INDEX_RULES))))
    } finally {
      cleanupAnalysisTags(indexes)
    }
  }
}
