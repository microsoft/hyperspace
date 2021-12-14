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
import com.microsoft.hyperspace.util.ResolverUtils.resolve

/**
 * JoinPlanNodeFilter filters indexes if
 *   1) the given plan is not eligible join plan.
 *   1-1) Join does not have condition.
 *   1-2) Left or Right child is not linear plan.
 *   1-3) Join condition is not eligible - only Equi-joins and simple CNF form are supported.
 *   1-4) SortMergeJoin.
 *   2) the source plan of indexes is not part of the join (neither Left nor Right).
 */
object JoinPlanNodeFilter extends JoinQueryPlanIndexFilter {
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

        val leftPlanLinearCond =
          withFilterReasonTag(
            plan,
            leftAndRightIndexes,
            FilterReasons.NotEligibleJoin("Non linear left child plan")) {
            isPlanLinear(l)
          }

        val rightPlanLinearCond =
          withFilterReasonTag(
            plan,
            leftAndRightIndexes,
            FilterReasons.NotEligibleJoin("Non linear right child plan")) {
            isPlanLinear(r)
          }

        if (sortMergeJoinCond && joinConditionCond && leftPlanLinearCond && rightPlanLinearCond) {
          // Set join query context.
          JoinIndexRule.leftRelation.set(left.get)
          JoinIndexRule.rightRelation.set(right.get)
          JoinIndexRule.joinCondition.set(condition)

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
 * JoinAttributeFilter filters indexes out if
 *   1) each join condition column should com from relations directly
 *   2) attributes from left plan must exclusively have one-to-one mapping with attribute
 *       from attributes from right plan.
 */
object JoinAttributeFilter extends JoinQueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    if (candidateIndexes.isEmpty || candidateIndexes.size != 2) {
      return Map.empty
    }

    if (withFilterReasonTag(
        plan,
        candidateIndexes.head._2 ++ candidateIndexes.last._2,
        FilterReasons.NotEligibleJoin("incompatible left and right join columns")) {
        ensureAttributeRequirements(
          JoinIndexRule.leftRelation.get,
          JoinIndexRule.rightRelation.get,
          JoinIndexRule.joinCondition.get)
      }) {
      candidateIndexes
    } else {
      Map.empty
    }
  }

}

/**
 * JoinColumnFilter filters indexes out if
 *   1) an index does not contain all required columns
 *   2) all join column should be the indexed columns of an index
 */
object JoinColumnFilter extends JoinQueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    if (candidateIndexes.isEmpty || candidateIndexes.size != 2) {
      return Map.empty
    }

    val leftRelation = JoinIndexRule.leftRelation.get
    val rightRelation = JoinIndexRule.rightRelation.get

    val lBaseAttrs = leftRelation.plan.output.map(_.name)
    val rBaseAttrs = rightRelation.plan.output.map(_.name)

    // Map of left resolved columns with their corresponding right resolved
    // columns from condition.
    val lRMap = getLRColumnMapping(lBaseAttrs, rBaseAttrs, JoinIndexRule.joinCondition.get)
    JoinIndexRule.leftToRightColumnMap.set(lRMap)
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

        if (withFilterReasonTag(
            plan,
            candidateIndexes.head._2 ++ candidateIndexes.last._2,
            FilterReasons.NoAvailJoinIndexPair("left"))(lIndexes.nonEmpty) &&
          withFilterReasonTag(
            plan,
            candidateIndexes.head._2 ++ candidateIndexes.last._2,
            FilterReasons.NoAvailJoinIndexPair("right"))(rIndexes.nonEmpty)) {
          Map(leftRelation.plan -> lIndexes, rightRelation.plan -> rIndexes)
        } else {
          Map.empty
        }
    }
  }
}

/**
 * JoinRankFilter selected the best applicable pair of indexes for Left and Right plan.
 */
object JoinRankFilter extends IndexRankFilter {
  override def apply(plan: LogicalPlan, indexes: PlanToIndexesMap): PlanToSelectedIndexMap = {
    if (indexes.isEmpty || indexes.size != 2) {
      return Map.empty
    }

    val leftRelation = JoinIndexRule.leftRelation.get
    val rightRelation = JoinIndexRule.rightRelation.get
    val lRMap = JoinIndexRule.leftToRightColumnMap.get
    val compatibleIndexPairs =
      getCompatibleIndexPairs(indexes(leftRelation.plan), indexes(rightRelation.plan), lRMap)

    compatibleIndexPairs
      .map { indexPairs =>
        val index = JoinIndexRanker
          .rank(spark, leftRelation.plan, rightRelation.plan, indexPairs)
          .head
        setFilterReasonTagForRank(plan, indexes(leftRelation.plan), index._1)
        setFilterReasonTagForRank(plan, indexes(rightRelation.plan), index._2)
        Map(leftRelation.plan -> index._1, rightRelation.plan -> index._2)
      }
      .getOrElse {
        setFilterReasonTag(
          plan,
          indexes.head._2 ++ indexes.last._2,
          FilterReasons.NoCompatibleJoinIndexPair())
        Map.empty
      }
  }

  /**
   * Get compatible index pairs from left and right
   * Compatible indexes have same order of index columns
   * For e.g.
   * SELECT P, Q FROM T1, T2 WHERE T1.A = T2.B and T1.C = T2.D
   * IDX1((A,C), (_)) is compatible with IDX2((B,D), (_))
   * IDX3((C,A), (_)) is compatible with IDX4((D,B), (_))
   *
   * On the other hand,
   * IDX1((A,C), (_)) is NOT compatible with IDX4((D,B), (_)) because order of columns does not
   * match with the join condition
   *
   * In this case, the method must return pairs: (IDX1, IDX2), (IDX3, IDX4) but must not return
   * (IDX1, IDX4)
   *
   * @param lIndexes All possibly usable indexes for left side of the join
   * @param rIndexes All possibly usable indexes for right side of the join
   * @param lRMap Map for corresponding columns from left and right subplan.
   * @return Compatible index pairs for left and right. First value of a pair represents left
   *         index. Second value represents right index
   */
  private def getCompatibleIndexPairs(
      lIndexes: Seq[IndexLogEntry],
      rIndexes: Seq[IndexLogEntry],
      lRMap: Map[String, String]): Option[Seq[(IndexLogEntry, IndexLogEntry)]] = {
    val compatibleIndexes = for {
      lIdx <- lIndexes
      rIdx <- rIndexes
      if isCompatible(lIdx, rIdx, lRMap)
    } yield (lIdx, rIdx)

    if (compatibleIndexes.nonEmpty) {
      Some(compatibleIndexes)
    } else {
      None
    }
  }

  /**
   * Two indexes are compatible with each other if the corresponding indexed columns of both
   * indexes are in the same order. Corresponding indexed columns are passed in a left to right
   * column mapping.
   *
   * E.g.
   * Let column mapping is (LA -> RA, LB -> RB), where LA, LB are columns of left subplan,
   * RA, RB are columns of right subplan.
   *
   * An index with indexed columns (LA, LB) is compatible with an index with indexed columns
   * (RA, RB), but not with indexed columns (RB, RA)
   *
   * Note: If number of buckets are same, it's great. If they are not, the indexes can still be
   * used by spark. Spark will pick one to repartition into number of buckets equal to the other.
   *
   * @param lIndex first index
   * @param rIndex second index
   * @param columnMapping Mapping of columns from left subplan to corresponding columns in right
   *                      subplan. Keys represent left subplan. Values represent right.
   * @return true if the two indexes can be used together to improve join query
   */
  private def isCompatible(
      lIndex: IndexLogEntry,
      rIndex: IndexLogEntry,
      columnMapping: Map[String, String]): Boolean = {
    require(columnMapping.keys.toSet.equals(lIndex.indexedColumns.toSet))
    require(columnMapping.values.toSet.equals(rIndex.indexedColumns.toSet))

    val requiredRightIndexedCols = lIndex.indexedColumns.map(columnMapping)
    rIndex.indexedColumns.equals(requiredRightIndexedCols)
  }
}

/**
 * Rule to optimize a join between two indexed dataframes.
 *
 * This rule improves a SortMergeJoin performance by replacing data files with index files.
 * The index files being bucketed and sorted, will eliminate a full shuffle of the data
 * during a sort-merge-join operation.
 *
 * For e.g.
 * SELECT T1.A, T1.B, T2.C, T2.D FROM T1, T2 WHERE T1.A = T2.C
 * The above query can be optimized to use indexes if indexes of the following configs exist:
 * Index1: indexedColumns: T1.A, includedColumns: T1.B
 * Index2: indexedColumns: T2.C, includedColumns: T2.D
 *
 * These indexes are indexed by the join columns and can improve the query performance by
 * avoiding full shuffling of T1 and T2.
 */
object JoinIndexRule extends HyperspaceRule with HyperspaceEventLogging {

  override val filtersOnQueryPlan: Seq[QueryPlanIndexFilter] =
    IndexTypeFilter[CoveringIndex]() ::
      JoinPlanNodeFilter ::
      JoinAttributeFilter ::
      JoinColumnFilter ::
      Nil

  override val indexRanker: IndexRankFilter = JoinRankFilter

  // Execution context
  var leftRelation: ThreadLocal[FileBasedRelation] = new ThreadLocal[FileBasedRelation]
  var rightRelation: ThreadLocal[FileBasedRelation] = new ThreadLocal[FileBasedRelation]
  var joinCondition: ThreadLocal[Expression] = new ThreadLocal[Expression]
  var leftToRightColumnMap: ThreadLocal[Map[String, String]] =
    new ThreadLocal[Map[String, String]]

  override def applyIndex(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): LogicalPlan = {
    if (indexes.size != 2) {
      return plan
    }
    plan match {
      case join @ JoinWithoutHint(l, r, _, _) =>
        val lIndex = indexes(leftRelation.get.plan)
        val rIndex = indexes(rightRelation.get.plan)

        val updatedPlan =
          join
            .copy(
              left = CoveringIndexRuleUtils.transformPlanToUseIndex(
                spark,
                lIndex,
                l,
                useBucketSpec = true,
                useBucketUnionForAppended = true),
              right = CoveringIndexRuleUtils.transformPlanToUseIndex(
                spark,
                rIndex,
                r,
                useBucketSpec = true,
                useBucketUnionForAppended = true))

        logEvent(
          HyperspaceIndexUsageEvent(
            AppInfo(sparkContext.sparkUser, sparkContext.applicationId, sparkContext.appName),
            Seq(lIndex, rIndex),
            join.toString,
            updatedPlan.toString,
            "Join index rule applied."))
        updatedPlan
    }
  }

  override def score(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): Int = {
    if (indexes.size != 2) {
      return 0
    }

    val lIndex = indexes(leftRelation.get.plan)
    val rIndex = indexes(rightRelation.get.plan)

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

    val leftCommonBytes = getCommonBytes(lIndex, leftRelation.get)
    val rightCommonBytes = getCommonBytes(rIndex, rightRelation.get)

    // TODO Enhance scoring function.
    //  See https://github.com/microsoft/hyperspace/issues/444

    (70 * (leftCommonBytes.toFloat / leftRelation.get.allFileSizeInBytes)).round +
      (70 * (rightCommonBytes.toFloat / rightRelation.get.allFileSizeInBytes)).round
  }
}
