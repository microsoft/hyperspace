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

package com.microsoft.hyperspace.index.covering

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags}
import com.microsoft.hyperspace.index.plananalysis.FilterReasons
import com.microsoft.hyperspace.index.rules.{HyperspaceRule, IndexRankFilter, IndexTypeFilter, QueryPlanIndexFilter, RuleUtils}
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.{PlanToIndexesMap, PlanToSelectedIndexMap}
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.shim.JoinWithoutHint
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEventLogging, HyperspaceIndexUsageEvent}
import com.microsoft.hyperspace.util.HyperspaceConf
import com.microsoft.hyperspace.util.ResolverUtils.resolve

/**
 * JoinV2PlanNodeFilter filters indexes if
 *   1) the given plan is not eligible join plan.
 *   1-1) Join does not have condition.
 *   1-2) Both left and right are not linear plan.
 *   1-3) Join condition is not eligible - only Equi-joins and simple CNF form are supported.
 *   1-4) SortMergeJoin.
 *   2) the source plan of indexes is not part of the join (neither Left nor Right).
 */
object JoinV2PlanNodeFilter extends QueryPlanIndexFilter with JoinQueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    if (candidateIndexes.isEmpty) {
      return Map.empty
    }

    plan match {
      case JoinWithoutHint(l, r, _, Some(condition)) =>
        val left = RuleUtils.getRelation(spark, l)
        val right = RuleUtils.getRelation(spark, r)

        if (!(left.isDefined && right.isDefined && !RuleUtils.isIndexApplied(
            left.get) && !RuleUtils
            .isIndexApplied(right.get))) {
          return Map.empty
        }

        val leftAndRightIndexes =
          candidateIndexes.getOrElse(left.get.plan, Nil) ++ candidateIndexes
            .getOrElse(right.get.plan, Nil)

        val sortMergeJoinCond = withFilterReasonTag(
          plan,
          leftAndRightIndexes,
          FilterReasons.NotEligibleJoin("Not SortMergeJoin")) {
          isSortMergeJoin(spark, plan)
        }

        val joinConditionCond = withFilterReasonTag(
          plan,
          leftAndRightIndexes,
          FilterReasons.NotEligibleJoin("Non equi-join or has literal")) {
          isJoinConditionSupported(condition)
        }

        val leftPlanLinearCond = isPlanLinear(l)
        val rightPlanLinearCond = isPlanLinear(r)

        if (sortMergeJoinCond && joinConditionCond && (leftPlanLinearCond || rightPlanLinearCond)) {
          // check left or right
          if (leftPlanLinearCond) {
            JoinIndexV2Rule.leftRelation.set(left.get)
          }
          if (rightPlanLinearCond) {
            JoinIndexV2Rule.rightRelation.set(right.get)
          }

          JoinIndexV2Rule.joinCondition.set(condition)
          (candidateIndexes.get(left.get.plan).map(lIndexes => left.get.plan -> lIndexes) ++
            candidateIndexes
              .get(right.get.plan)
              .map(rIndexes => right.get.plan -> rIndexes)).toMap
        } else {
          Map.empty
        }
      case JoinWithoutHint(_, _, _, None) =>
        setFilterReasonTag(
          plan,
          candidateIndexes.values.flatten.toSeq,
          FilterReasons.NotEligibleJoin("No join condition"))
        Map.empty
      case _ =>
        Map.empty
    }
  }
}

/**
 * JoinV2AttributeFilter filters indexes out if
 *   1) each join condition column should com from relations directly
 *   2) attributes from left plan must exclusively have one-to-one mapping with attribute
 *       from attributes from right plan.
 */
object JoinV2AttributeFilter extends JoinQueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    if (candidateIndexes.isEmpty) {
      return Map.empty
    }

    if (withFilterReasonTag(
        plan,
        candidateIndexes.flatMap(_._2).toSeq,
        FilterReasons.NotEligibleJoin("incompatible left and right join columns")) {
        ensureAttributeRequirements(
          JoinIndexV2Rule.leftRelation.get,
          JoinIndexV2Rule.rightRelation.get,
          JoinIndexV2Rule.joinCondition.get)
      }) {
      candidateIndexes
    } else {
      Map.empty
    }
  }
}

/**
 * JoinV2ColumnFilter filters indexes out if
 *   1) an index does not contain all required columns
 *   2) all join column should be the indexed columns of an index
 */
object JoinV2ColumnFilter extends JoinQueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    if (candidateIndexes.isEmpty || candidateIndexes.size != 2) {
      return Map.empty
    }

    val leftRelation = JoinIndexV2Rule.leftRelation.get
    val rightRelation = JoinIndexV2Rule.rightRelation.get

    val lBaseAttrs = leftRelation.plan.output.map(_.name)
    val rBaseAttrs = rightRelation.plan.output.map(_.name)

    // Map of left resolved columns with their corresponding right resolved
    // columns from condition.
    val lRMap = getLRColumnMapping(lBaseAttrs, rBaseAttrs, JoinIndexV2Rule.joinCondition.get)
    JoinIndexV2Rule.leftToRightColumnMap.set(lRMap)
    val lRequiredIndexedCols = lRMap.keys.toSeq
    val rRequiredIndexedCols = lRMap.values.toSeq

    plan match {
      case JoinWithoutHint(l, r, _, _) =>
        // All required columns resolved with base relation.
        val lRequiredAllCols = resolve(spark, allRequiredCols(l), lBaseAttrs).get
        val rRequiredAllCols = resolve(spark, allRequiredCols(r), rBaseAttrs).get

        // Make sure required indexed columns are subset of all required columns.
        assert(
          resolve(spark, lRequiredIndexedCols, lRequiredAllCols).isDefined &&
            resolve(spark, rRequiredIndexedCols, rRequiredAllCols).isDefined)

        val lIndexes =
          getUsableIndexes(
            plan,
            candidateIndexes.getOrElse(leftRelation.plan, Nil),
            lRequiredIndexedCols,
            lRequiredAllCols,
            "left")
        val rIndexes =
          getUsableIndexes(
            plan,
            candidateIndexes.getOrElse(rightRelation.plan, Nil),
            rRequiredIndexedCols,
            rRequiredAllCols,
            "right")

        Map(leftRelation.plan -> lIndexes, rightRelation.plan -> rIndexes)
    }
  }
}

/**
 * JoinV2RankFilter selected the best applicable indexes.
 */
object JoinV2RankFilter extends IndexRankFilter {
  override def apply(plan: LogicalPlan, indexes: PlanToIndexesMap): PlanToSelectedIndexMap = {
    if (indexes.isEmpty) {
      return Map.empty
    }

    val rPlan = JoinIndexV2Rule.rightRelation.get.plan
    val lPlan = JoinIndexV2Rule.leftRelation.get.plan

    // Pick one index with the largest coverage.
    val lIndexes = indexes.get(lPlan)
    val rIndexes = indexes.get(rPlan)

    if (lIndexes.isEmpty) {
      val candidate = indexWithMostCommonBytes(rPlan, rIndexes.get)
      setFilterReasonTagForRank(plan, rIndexes.get, candidate)
      Map(rPlan -> indexWithMostCommonBytes(rPlan, rIndexes.get))
    } else if (rIndexes.isEmpty) {
      val candidate = indexWithMostCommonBytes(lPlan, lIndexes.get)
      setFilterReasonTagForRank(plan, lIndexes.get, candidate)
      Map(lPlan -> candidate)
    } else {
      // Apply one index which has a larger data to reduce the shuffle/sorting time.
      // If we apply for both left having a different indexed columns and bucket number,
      // Spark optimizer might remove exchange of small side, and add a shuffle for large dataset.
      val lSize = JoinIndexV2Rule.leftRelation.get.allFileSizeInBytes
      val rSize = JoinIndexV2Rule.rightRelation.get.allFileSizeInBytes
      if (lSize > rSize) {
        val candidate = indexWithMostCommonBytes(lPlan, lIndexes.get)
        setFilterReasonTagForRank(plan, rIndexes.get, candidate)
        Map(lPlan -> candidate)
      } else {
        val candidate = indexWithMostCommonBytes(rPlan, rIndexes.get)
        setFilterReasonTagForRank(plan, lIndexes.get, candidate)
        Map(rPlan -> candidate)
      }
    }
  }

  private def indexWithMostCommonBytes(
      plan: LogicalPlan,
      candidates: Seq[IndexLogEntry]): IndexLogEntry = {
    // On the other side, there's no applicable index.
    if (HyperspaceConf.hybridScanEnabled(spark)) {
      candidates.maxBy(
        _.getTagValue(plan, IndexLogEntryTags.COMMON_SOURCE_SIZE_IN_BYTES).getOrElse(0L))
    } else {
      candidates.maxBy(_.indexFilesSizeInBytes)
    }
  }
}

/**
 * Rule to optimize a join by applying index either of left or right plan.
 *
 * This rule improves a SortMergeJoin performance by replacing data files with index files.
 * The index files being bucketed and sorted, will eliminate a full shuffle of the data
 * during a sort-merge-join operation.
 *
 * For e.g.
 * SELECT T1.A, T1.B, T2.C, T2.D FROM T1, T2 WHERE T1.A = T2.C
 * The above query can be optimized to use indexes if one of the following indexes exists:
 * Index1: indexedColumns: T1.A, includedColumns: T1.B
 * Index2: indexedColumns: T2.C, includedColumns: T2.D
 *
 * These indexes are indexed by the join columns and can improve the query performance by
 * avoiding full shuffling of T1 or T2.
 */
object JoinIndexV2Rule extends HyperspaceRule with HyperspaceEventLogging {

  override val filtersOnQueryPlan: Seq[QueryPlanIndexFilter] =
    IndexTypeFilter[CoveringIndex]() ::
      JoinV2PlanNodeFilter ::
      JoinV2AttributeFilter ::
      JoinV2ColumnFilter ::
      Nil

  override val indexRanker: IndexRankFilter = JoinV2RankFilter

  // Execution context
  var leftRelation: ThreadLocal[FileBasedRelation] = new ThreadLocal[FileBasedRelation]
  var rightRelation: ThreadLocal[FileBasedRelation] = new ThreadLocal[FileBasedRelation]
  var joinCondition: ThreadLocal[Expression] = new ThreadLocal[Expression]
  var leftToRightColumnMap: ThreadLocal[Map[String, String]] =
    new ThreadLocal[Map[String, String]]

  override def applyIndex(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): LogicalPlan = {
    if (indexes.size != 1) {
      return plan
    }

    plan match {
      case join @ JoinWithoutHint(l, r, _, _) =>
        val updatedPlan = if (indexes.contains(leftRelation.get.plan)) {
          val lIndex = indexes(leftRelation.get.plan)
          join
            .copy(left = CoveringIndexRuleUtils.transformPlanToUseIndex(
              spark,
              lIndex,
              l,
              useBucketSpec = true,
              useBucketUnionForAppended = true))

        } else {
          val rIndex = indexes(rightRelation.get.plan)
          join
            .copy(right = CoveringIndexRuleUtils.transformPlanToUseIndex(
              spark,
              rIndex,
              r,
              useBucketSpec = true,
              useBucketUnionForAppended = true))
        }

        logEvent(
          HyperspaceIndexUsageEvent(
            AppInfo(sparkContext.sparkUser, sparkContext.applicationId, sparkContext.appName),
            Seq(indexes.values.toSeq.head),
            join.toString,
            updatedPlan.toString,
            "Join index v2 rule applied."))
        updatedPlan
    }
  }

  override def score(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): Int = {
    if (indexes.size != 1) {
      return 0
    }

    val targetRel = if (indexes.contains(leftRelation.get.plan)) {
      leftRelation.get
    } else {
      rightRelation.get
    }

    def getCommonBytes(index: IndexLogEntry, relation: FileBasedRelation): Long = {
      index
        .getTagValue(relation.plan, IndexLogEntryTags.COMMON_SOURCE_SIZE_IN_BYTES)
        .getOrElse {
          relation.allFileInfos.foldLeft(0L) { (res, f) =>
            if (index.sourceFileInfoSet.contains(f)) {
              res + f.size // count, total bytes
            } else {
              res
            }
          }
        }
    }

    val commonBytes = getCommonBytes(indexes.head._2, targetRel)

    // TODO Enhance scoring function.
    //  See https://github.com/microsoft/hyperspace/issues/444
    (60 * (commonBytes.toFloat / targetRel.allFileSizeInBytes)).round
  }
}
