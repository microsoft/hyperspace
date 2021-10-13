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

import scala.collection.mutable
import scala.util.Try

import org.apache.spark.sql.catalyst.analysis.CleanupAliases
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.joins.SortMergeJoinExec
import org.apache.spark.sql.hyperspace.shim.SparkPlannerShim

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags}
import com.microsoft.hyperspace.index.covering.JoinAttributeFilter.extractConditions
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
 *   2) the source plan of indexes is not part of the join (neither Left nor Right).
 */
object JoinPlanNodeFilter extends QueryPlanIndexFilter {
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

        val joinConditionCond = withFilterReasonTag(
          plan,
          leftAndRightIndexes,
          FilterReasons.NotEligibleJoin("Non equi-join or has literal")) {
          isJoinConditionSupported(condition)
        }

        val sortMergeJoinCond = withFilterReasonTag(
          plan,
          leftAndRightIndexes,
          FilterReasons.NotEligibleJoin("Not SortMergeJoin")) {
          isSortMergeJoin(plan)
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

  private def isSortMergeJoin(join: LogicalPlan): Boolean = {
    val execJoin = new SparkPlannerShim(spark).JoinSelection(join)
    execJoin.head.isInstanceOf[SortMergeJoinExec]
  }

  /**
   * Checks whether a logical plan is linear. Linear means starting at the top, each node in the
   * plan has at most one child.
   *
   * Note: More work needs to be done to support arbitrary logical plans. Until then, this check
   * is in place to avoid unexplored scenarios.
   *
   * The non-linearity can affect when the logical plan signature conflicts happen.
   * Assume plan: Join(A, Join(B, C)) ->
   * Here,
   * left = A
   * right = Join(B, C)
   *
   * If by any chance, signature(right) matches with some signature(some other table D), then
   * we might use indexes of table D as a replacement of right
   * This problem is possible because currently the signature function depends on the data files
   * only. It doesn't incorporate the logical plan structure.
   * Once the signature function evolves to take the plan structure into account, we can remove
   * the linearity check completely.
   *
   * @param plan logical plan
   * @return true if the plan is linear. False otherwise
   */
  private def isPlanLinear(plan: LogicalPlan): Boolean =
    plan.children.length <= 1 && plan.children.forall(isPlanLinear)

  /**
   * Check for supported Join Conditions. Equi-Joins in simple CNF form are supported.
   *
   * Predicates should be of the form (A = B and C = D and E = F and...). OR based conditions
   * are not supported. E.g. (A = B OR C = D) is not supported
   *
   * TODO: Investigate whether OR condition can use bucketing info for optimization
   *
   * @param condition Join condition
   * @return True if the condition is supported. False otherwise.
   */
  private def isJoinConditionSupported(condition: Expression): Boolean = {
    condition match {
      case EqualTo(_: AttributeReference, _: AttributeReference) => true
      case And(left, right) => isJoinConditionSupported(left) && isJoinConditionSupported(right)
      case _ => false
    }
  }
}

/**
 * JoinAttributeFilter filters indexes out if
 *   1) each join condition column should com from relations directly
 *   2) attributes from left plan must exclusively have one-to-one mapping with attribute
 *       from attributes from right plan.
 */
object JoinAttributeFilter extends QueryPlanIndexFilter {
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

  /**
   * Requirements to support join optimizations using join indexes are as follows:
   *
   * 1. All Join condition attributes, i.e. attributes referenced in join condition
   * must directly come from base relation attributes.
   * E.g. for condition (A = B) => Both A and B should come directly from the base tables.
   *
   * This will not be true if say, A is an alias of some other column C from the base table. We
   * don't optimize Join queries for such cases.
   * E.g. The following query is not supported:
   *    WITH newT1 AS (SELECT a AS aliasCol FROM T1)
   *    SELECT aliasCol, b
   *    FROM newT1, T2
   *    WHERE newT1.aliasCol = T2.b
   * Here, aliasCol is not directly from the base relation T1
   *
   * TODO: add alias resolver for supporting aliases in join condition. Until then,
   *   make sure this scenario isn't supported
   *
   * 2. For each equality condition in the join predicate, one attribute must belong to the left
   * subplan, and another from right subplan.
   * E.g. A = B => A should come from left and B should come from right or vice versa.
   *
   * 3. Exclusivity Check: Join Condition Attributes from left subplan must exclusively have
   * one-to-one mapping with join condition attributes from right subplan.
   * E.g. (A = B and C = D) is supported. A maps with B, C maps with D exclusively.
   * E.g. (A = B and A = D) is not supported. A maps with both B and D. There isn't a one-to-one
   * mapping.
   *
   * Background knowledge:
   * An alias in a query plan is represented as [[Alias]] at the time of
   * its creation. Unnecessary aliases get resolved and removed during query analysis phase by
   * [[CleanupAliases]] rule. Some nodes still remain with alias definitions. E.g. [[Project]].
   *
   * Secondly, the output of a logical plan is an [[AttributeSet]]. Alias objects get converted
   * to [[AttributeReference]]s at plan boundaries.
   *
   * From looking at the join condition, we can't know whether the attributes used in the
   * condition were original columns from the base table, or were alias-turned-attributes.
   *
   * Algorithm:
   * 1. Collect base table attributes from the supported relation nodes. These are the output set
   * from leaf nodes.
   * 2. Maintain a mapping of join condition attributes.
   * 3. For every equality condition (A == B), Ensure one-to-one mapping between A and B. A
   * should not be compared against any other attribute than B. B should not be compared with any
   * other attribute than A. Steps:
   *   a. A and B should belong to base tables from left and right subplans.
   *   b. If A belongs to left subplan, B should be from right, or vice-versa.
   *   c. If A and B are never seen in the past (check the mapping), add them to the map with
   *      (A -> B) and (B -> A) entries
   *   d. If A exists in the map, make sure A maps to B AND B maps to A back.
   *   e. If A doesn't exist in the map, B should not exist in the map
   *
   * Note: this check might affect performance of query optimizer for very large query plans,
   * because of multiple collectLeaves calls. We call this method as late as possible.
   *
   * @param l Left relation
   * @param r Right relation
   * @param condition Join condition
   * @return True if all attributes in join condition are from base relation nodes.
   */
  private def ensureAttributeRequirements(
      l: FileBasedRelation,
      r: FileBasedRelation,
      condition: Expression): Boolean = {
    // Output attributes from base relations. Join condition attributes must belong to these
    // attributes. We work on canonicalized forms to make sure we support case-sensitivity.
    val lBaseAttrs = l.plan.output.map(_.canonicalized)
    val rBaseAttrs = r.plan.output.map(_.canonicalized)

    def fromDifferentBaseRelations(c1: Expression, c2: Expression): Boolean = {
      (lBaseAttrs.contains(c1) && rBaseAttrs.contains(c2)) ||
      (lBaseAttrs.contains(c2) && rBaseAttrs.contains(c1))
    }

    // Map to maintain and check one-to-one relation between join condition attributes. For join
    // condition attributes, we store their corresponding base relation attributes in the map.
    // This ensures uniqueness of attributes in case of case-insensitive system. E.g. We want to
    // store just one copy of column 'A' when join condition contains column 'A' as well as 'a'.
    val attrMap = new mutable.HashMap[Expression, Expression]()

    extractConditions(condition).forall {
      case EqualTo(e1, e2) =>
        val (c1, c2) = (e1.canonicalized, e2.canonicalized)
        // Check 1: c1 and c2 should belong to l and r respectively, or r and l respectively.
        if (!fromDifferentBaseRelations(c1, c2)) {
          return false
        }
        // Check 2: c1 is compared only against c2 and vice versa.
        if (attrMap.contains(c1) && attrMap.contains(c2)) {
          attrMap(c1).equals(c2) && attrMap(c2).equals(c1)
        } else if (!attrMap.contains(c1) && !attrMap.contains(c2)) {
          attrMap.put(c1, c2)
          attrMap.put(c2, c1)
          true
        } else {
          false
        }
      case _ => throw new IllegalStateException("Unsupported condition found.")
    }
  }

  /**
   * Simple single equi-join conditions and composite AND based conditions will be optimized.
   * OR-based conditions will not be optimized.
   *
   * @param condition Join condition
   * @return Sequence of simple conditions from original condition
   */
  private[hyperspace] def extractConditions(condition: Expression): Seq[Expression] =
    condition match {
      case EqualTo(_: AttributeReference, _: AttributeReference) =>
        Seq(condition)
      case And(left, right) =>
        extractConditions(left) ++ extractConditions(right)
      case _ => throw new IllegalStateException("Unsupported condition found")
    }
}

/**
 * JoinColumnFilter filters indexes out if
 *   1) an index does not contain all required columns
 *   2) all join column should be the indexed columns of an index
 */
object JoinColumnFilter extends QueryPlanIndexFilter {
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

  /**
   * Returns a one-to-one column mapping from the predicates. E.g. for predicate.
   * T1.A = T2.B and T2.D = T1.C
   * it returns a mapping (A -> B), (C -> D), assuming T1 is left table and t2 is right
   *
   * This mapping is used to find compatible indexes of T1 and T2.
   *
   * @param leftBaseAttrs required indexed columns from left plan
   * @param rightBaseAttrs required indexed columns from right plan
   * @param condition join condition which will be used to find the left-right column mapping
   * @return Mapping of corresponding columns from left and right, depending on the join
   *         condition. The keys represent columns from left subplan. The values are columns from
   *         right subplan.
   */
  private[hyperspace] def getLRColumnMapping(
      leftBaseAttrs: Seq[String],
      rightBaseAttrs: Seq[String],
      condition: Expression): Map[String, String] = {
    extractConditions(condition).map {
      case EqualTo(attr1: AttributeReference, attr2: AttributeReference) =>
        Try {
          (
            resolve(spark, attr1.name, leftBaseAttrs).get,
            resolve(spark, attr2.name, rightBaseAttrs).get)
        }.getOrElse {
          Try {
            (
              resolve(spark, attr2.name, leftBaseAttrs).get,
              resolve(spark, attr1.name, rightBaseAttrs).get)
          }.getOrElse {
            throw new IllegalStateException("Unexpected exception while using join rule")
          }
        }
    }.toMap
  }

  /**
   * Get usable indexes which satisfy indexed and included column requirements.
   *
   * Pre-requisite: the indexed and included columns required must be already resolved with their
   * corresponding base relation columns at this point.
   *
   * @param plan Query plan
   * @param indexes All available indexes for the logical plan
   * @param requiredIndexCols required indexed columns resolved with their base relation column.
   * @param allRequiredCols required included columns resolved with their base relation column.
   * @return Indexes which satisfy the indexed and covering column requirements from the logical
   *         plan and join condition
   */
  private def getUsableIndexes(
      plan: LogicalPlan,
      indexes: Seq[IndexLogEntry],
      requiredIndexCols: Seq[String],
      allRequiredCols: Seq[String],
      leftOrRight: String): Seq[IndexLogEntry] = {
    indexes.filter { idx =>
      val allCols = idx.derivedDataset.referencedColumns
      // All required index columns should match one-to-one with all indexed columns and
      // vice-versa. All required columns must be present in the available index columns.
      withFilterReasonTag(
        plan,
        idx,
        FilterReasons.NotAllJoinColIndexed(
          leftOrRight,
          requiredIndexCols.mkString(","),
          idx.indexedColumns.mkString(","))) {
        requiredIndexCols.toSet.equals(idx.indexedColumns.toSet)
      } &&
      withFilterReasonTag(
        plan,
        idx,
        FilterReasons.MissingIndexedCol(
          leftOrRight,
          allRequiredCols.mkString(","),
          idx.indexedColumns.mkString(","))) {
        allRequiredCols.forall(allCols.contains)
      }
    }
  }

  /**
   * Returns list of column names which must be present in either the indexed or the included
   * columns list of a selected index. For this, collect all columns referenced in the plan
   * EXCEPT for the logical relation (i.e. scan node).
   *
   * E.g. Project(A, B) -> Filter (C = 10) -> Scan T(A,B,C,D,E)
   * In the above scenario, A, B, C must be part of either the indexed or the included columns
   * requirement. D,E are not part of the requirement.
   *
   * E.g Filter (C = 10) -> Scan T(A,B,C,D,E)
   * In this example, it turns out, all columns A,B,C,D,E are required columns. The Filter
   * node doesn't do column pruning. This query plan corresponds to a sql query:
   * SELECT * FROM T WHERE C = 10
   * We need to make sure all columns are part of the required columns
   *
   * Algorithm:
   * 1. allReferences: Collect all referenced columns from nodes as required columns. These
   * columns (i.e. `LogicalPlan.references`) represent the columns used in the plan. For e.g.
   * Filter(a = 10 && b > 20) contains column 'a' and 'b' as referenced columns.
   *
   * 2. topLevelOutputs: Collect the top level output columns from the plan. These are required
   * as these will be used later on in the original query. This is basically a shortened list
   * of columns which did not get masked in intermediate Project nodes.
   *  E.g.1 Filter (C = 10) -> Scan T(A,B,C,D,E)
   *  Here, top level output will contain all columns A,B,C,D,E
   *
   *  E.g.2 Project (A) -> Filter(c=10) -> Scan T(A,B,C,D,E)
   *  Here, top level output will contain only column A
   *
   * 3. return the distinct set of column names from 1 and 2 combined.
   *
   * @param plan logical plan
   * @return list of column names from this plan which must be part of either indexed or included
   *         columns in a chosen index
   */
  private def allRequiredCols(plan: LogicalPlan): Seq[String] = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    val cleaned = CleanupAliases(plan)
    val allReferences = cleaned.collect {
      case l: LeafNode if provider.isSupportedRelation(l) => Seq()
      case other => other.references
    }.flatten
    val topLevelOutputs = cleaned.outputSet.toSeq

    (allReferences ++ topLevelOutputs).distinct.collect {
      case attr: AttributeReference => attr.name
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
