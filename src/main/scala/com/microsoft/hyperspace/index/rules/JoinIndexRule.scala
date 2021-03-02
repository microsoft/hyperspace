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
import scala.util.Try

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.CleanupAliases
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, EqualTo, Expression, GetStructField, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.rankers.JoinIndexRanker
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEventLogging, HyperspaceIndexUsageEvent}
import com.microsoft.hyperspace.util.ResolverUtils._
import com.microsoft.hyperspace.util.SchemaUtils

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
object JoinIndexRule
    extends Rule[LogicalPlan]
    with Logging
    with HyperspaceEventLogging
    with ActiveSparkSession {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case join @ Join(l, r, _, Some(condition)) if isApplicable(l, r, condition) =>
      try {
        getBestIndexPair(l, r, condition)
          .map {
            case (lIndex, rIndex) =>
              val updatedPlan =
                join
                  .copy(
                    left = RuleUtils.transformPlanToUseIndex(
                      spark,
                      lIndex,
                      l,
                      useBucketSpec = true,
                      useBucketUnionForAppended = true),
                    right = RuleUtils.transformPlanToUseIndex(
                      spark,
                      rIndex,
                      r,
                      useBucketSpec = true,
                      useBucketUnionForAppended = true))

              logEvent(
                HyperspaceIndexUsageEvent(
                  AppInfo(
                    sparkContext.sparkUser,
                    sparkContext.applicationId,
                    sparkContext.appName),
                  Seq(lIndex, rIndex),
                  join.toString,
                  updatedPlan.toString,
                  "Join index rule applied."))

              updatedPlan
          }
          .getOrElse(join)
      } catch {
        case e: Exception =>
          logWarning("Non fatal exception in running join index rule: " + e.getMessage)
          join
      }
  }

  /**
   * Checks whether this join rule is applicable for the current node
   *
   * @param l left logical plan
   * @param r right logical plan
   * @param condition join condition
   * @return true if supported. False if not.
   */
  private def isApplicable(l: LogicalPlan, r: LogicalPlan, condition: Expression): Boolean = {
    // The given plan is eligible if it is supported and index has not been applied.
    def isEligible(optRel: Option[FileBasedRelation]): Boolean = {
      optRel.exists(!RuleUtils.isIndexApplied(_))
    }

    lazy val optLeftRel = RuleUtils.getRelation(spark, l)
    lazy val optRightRel = RuleUtils.getRelation(spark, r)

    val lProj = collectProjections(l)
    val rProj = collectProjections(r)

    isJoinConditionSupported(condition) &&
    isPlanLinear(l) && isPlanLinear(r) &&
    isEligible(optLeftRel) && isEligible(optRightRel) &&
    ensureAttributeRequirements(optLeftRel.get, optRightRel.get, lProj, rProj, condition)
  }

  /**
   * Check for supported Join Conditions. Equi-Joins in simple CNF form are supported.
   *
   * Predicates should be of the form (A = B and C = D and E = F and...). OR based conditions
   * are not supported. E.g. (A = B OR C = D) is not supported
   *
   * TODO: Investigate whether OR condition can use bucketing info for optimization
   *
   * @param condition the join condition
   * @return true if the condition is supported. False otherwise.
   */
  private def isJoinConditionSupported(condition: Expression): Boolean = {
    condition match {
      case EqualTo(_: AttributeReference, _: AttributeReference) => true
      case And(left, right) => isJoinConditionSupported(left) && isJoinConditionSupported(right)
      case _ => false
    }
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
   * @param l left relation
   * @param r right relation
   * @param lp left projections
   * @param rp right projections
   * @param condition join condition
   * @return true if all attributes in join condition are from base relation nodes. False
   *         otherwise
   */
  private def ensureAttributeRequirements(
      l: FileBasedRelation,
      r: FileBasedRelation,
      lp: Seq[NamedExpression],
      rp: Seq[NamedExpression],
      condition: Expression): Boolean = {

    // Output attributes from base relations. Join condition attributes must belong to these
    // attributes. We work on canonicalized forms to make sure we support case-sensitivity.
    val lBaseAttrs = l.plan.output.map(_.canonicalized)
    val rBaseAttrs = r.plan.output.map(_.canonicalized)

    def fromDifferentBaseRelations(
        c1: Expression,
        c2: Expression,
        p1: Seq[NamedExpression],
        p2: Seq[NamedExpression]): Boolean = {
      val cr1 = if (p1.nonEmpty) {
        Try {
          extractFieldFromProjection(c1, p1).get.references.head.canonicalized
        }.getOrElse(c1)
      } else {
        c1
      }
      val cr2 = if (p2.nonEmpty) {
        Try {
          extractFieldFromProjection(c2, p2).get.references.head.canonicalized
        }.getOrElse(c2)
      } else {
        c2
      }
      (lBaseAttrs.contains(cr1) && rBaseAttrs.contains(cr2)) ||
      (lBaseAttrs.contains(cr2) && rBaseAttrs.contains(cr1))
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
        if (!fromDifferentBaseRelations(c1, c2, lp, rp)) {
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
      case _ => throw new IllegalStateException("Unsupported condition found")
    }
  }

  /**
   * The method extracts all the projection fields.
   *
   * @param plan The plan from which to extract projections
   * @return A sequence of [[NamedExpression]]
   */
  private def collectProjections(plan: LogicalPlan): Seq[NamedExpression] = {
    plan.collect {
      case p: Project => p.projectList
    }.flatten
  }

  /**
   * The method tries to map any top level condition field to the fields present in relation.
   * It does this by going through projections.
   *
   * @param projections The available projection expressions
   * @return Some of the found expression when the condition field is found otherwise None
   */
  private def conditionFieldsToRelationFields(
      projections: Seq[NamedExpression]): Map[Expression, Expression] = {
    projections.collect {
      case a: Alias =>
        (a.toAttribute.canonicalized, a.child)
    }.toMap
  }

  /**
   * The method tries to return a field out of the fields present in relation.
   *
   * @param conditionField The field to map to
   * @param projections The available projection expressions
   * @return Some of the found expression when the condition field is found otherwise None
   */
  private def extractFieldFromProjection(
      conditionField: Expression,
      projections: Seq[NamedExpression]): Option[Expression] = {
    val fields = conditionFieldsToRelationFields(projections)
    Try(fields(conditionField.canonicalized)).toOption
  }

  /**
   * Get best ranked index pair from available indexes of both sides.
   *
   * @param left Left subplan.
   * @param right Right subplan.
   * @param joinCondition Join condition.
   * @return The best index pair, where the first element is for left subplan, second for right.
   */
  private def getBestIndexPair(
      left: LogicalPlan,
      right: LogicalPlan,
      joinCondition: Expression): Option[(IndexLogEntry, IndexLogEntry)] = {
    val indexManager = Hyperspace.getContext(spark).indexCollectionManager

    // TODO: the following check only considers indexes in ACTIVE state for usage. Update
    //  the code to support indexes in transitioning states as well.
    //  See https://github.com/microsoft/hyperspace/issues/65
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    // TODO: we can write an extractor that applies `isApplicable` so that we don't have to
    //   get relations twice. Note that `getRelation` should always succeed since this has
    //   been already checked in `isApplicable`.
    val leftRelation = RuleUtils.getRelation(spark, left).get
    val rightRelation = RuleUtils.getRelation(spark, right).get
    val lBaseAttrs = SchemaUtils.flatten(leftRelation.plan.output)
    val rBaseAttrs = SchemaUtils.flatten(rightRelation.plan.output)

    // Map of left resolved columns with their corresponding right resolved
    // columns from condition.
    val lProj = collectProjections(left)
    val rProj = collectProjections(right)
    val lRMap = getLRColumnMapping(lBaseAttrs, rBaseAttrs, lProj, rProj, joinCondition)
    val lRequiredIndexedCols = lRMap.keys.toSeq
    val rRequiredIndexedCols = lRMap.values.toSeq

    // All required columns resolved with base relation.
    val lRequiredAllCols = resolve(spark, allRequiredCols(left), lBaseAttrs).get
    val rRequiredAllCols = resolve(spark, allRequiredCols(right), rBaseAttrs).get

    // Make sure required indexed columns are subset of all required columns for a subplan
    require(resolve(spark, lRequiredIndexedCols, lRequiredAllCols).isDefined)
    require(resolve(spark, rRequiredIndexedCols, rRequiredAllCols).isDefined)

    val lUsable = getUsableIndexes(allIndexes, lRequiredIndexedCols, lRequiredAllCols)
    val rUsable = getUsableIndexes(allIndexes, rRequiredIndexedCols, rRequiredAllCols)

    val leftRel = RuleUtils.getRelation(spark, left).get
    val rightRel = RuleUtils.getRelation(spark, right).get

    // Get candidate via file-level metadata validation. This is performed after pruning
    // by column schema, as this might be expensive when there are numerous files in the
    // relation or many indexes to be checked.
    val lIndexes = RuleUtils.getCandidateIndexes(spark, lUsable, leftRel)
    val rIndexes = RuleUtils.getCandidateIndexes(spark, rUsable, rightRel)

    val compatibleIndexPairs = getCompatibleIndexPairs(lIndexes, rIndexes, lRMap)

    compatibleIndexPairs.map(
      indexPairs =>
        JoinIndexRanker
          .rank(spark, leftRel.plan, rightRel.plan, indexPairs)
          .head)
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
    val projectionFields = collectProjections(plan)

    val allReferences = plan.collect {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        Seq.empty[String]
      case other =>
        other match {
          case project: Project =>
            val fields = conditionFieldsToRelationFields(project.projectList).values
            fields.flatMap {
              case g: GetStructField =>
                Seq(ExtractFilterNode.getChildNameFromStruct(g))
              case otherFieldType =>
                ExtractFilterNode.extractNamesFromExpression(otherFieldType).toSeq
            }
          case filter: Filter =>
            var acc = Seq.empty[String]
            val fls = ExtractFilterNode
              .extractNamesFromExpression(filter.condition)
              .toSeq
              .distinct
              .sortBy(-_.length)
              .toList
            var h :: t = fls
            while (t.nonEmpty) {
              if (!t.exists(_.contains(h))) {
                acc = acc :+ h
              }
              h = t.head
              t = t.tail
            }
            acc
          case o =>
            o.references.map(_.name)
        }
    }.flatten

    val topLevelOutputs = if (projectionFields.nonEmpty) {
      plan.outputSet.map { i =>
        val attr = extractFieldFromProjection(i, projectionFields)
        val opt = attr.map { e =>
          ExtractFilterNode.getChildNameFromStruct(e.asInstanceOf[GetStructField])
        }
        opt.getOrElse(i.name)
      }
    } else {
      plan.outputSet.toSeq.map(_.name)
    }

    (allReferences ++ topLevelOutputs).distinct
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
  private def getLRColumnMapping(
      leftBaseAttrs: Seq[String],
      rightBaseAttrs: Seq[String],
      lp: Seq[NamedExpression],
      rp: Seq[NamedExpression],
      condition: Expression): Map[String, String] = {
    extractConditions(condition).map {
      case EqualTo(attr1: AttributeReference, attr2: AttributeReference) =>
        val attrLeftName = if (lp.nonEmpty) {
          Try {
            val attrLeft = extractFieldFromProjection(attr1, lp).get
            ExtractFilterNode.getChildNameFromStruct(attrLeft.asInstanceOf[GetStructField])
          }.getOrElse(attr1.name)
        } else {
          attr1.name
        }
        val attrRightName = if (rp.nonEmpty) {
          Try {
            val attrRight = extractFieldFromProjection(attr2, rp).get
            ExtractFilterNode.getChildNameFromStruct(attrRight.asInstanceOf[GetStructField])
          }.getOrElse(attr2.name)
        } else {
          attr2.name
        }

        Try {
          (
            resolve(spark, attrLeftName, leftBaseAttrs).get,
            resolve(spark, attrRightName, rightBaseAttrs).get)
        }.getOrElse {
          Try {
            (
              resolve(spark, attrRightName, leftBaseAttrs).get,
              resolve(spark, attrLeftName, rightBaseAttrs).get)
          }.getOrElse {
            throw new IllegalStateException("Unexpected exception while using join rule")
          }
        }
    }.toMap
  }

  /**
   * Simple single equi-join conditions and composite AND based conditions will be optimized.
   * OR-based conditions will not be optimized
   *
   * @param condition Join condition
   * @return Sequence of simple conditions from original condition
   */
  private def extractConditions(condition: Expression): Seq[Expression] = condition match {
    case EqualTo(_: AttributeReference, _: AttributeReference) =>
      Seq(condition)
    case And(left, right) =>
      extractConditions(left) ++ extractConditions(right)
    case _ => throw new IllegalStateException("Unsupported condition found")
  }

  /**
   * Get usable indexes which satisfy indexed and included column requirements.
   *
   * Pre-requisite: the indexed and included columns required must be already resolved with their
   * corresponding base relation columns at this point.
   *
   * @param indexes All available indexes for the logical plan
   * @param requiredIndexCols required indexed columns resolved with their base relation column.
   * @param allRequiredCols required included columns resolved with their base relation column.
   * @return Indexes which satisfy the indexed and covering column requirements from the logical
   *         plan and join condition
   */
  private def getUsableIndexes(
      indexes: Seq[IndexLogEntry],
      requiredIndexCols: Seq[String],
      allRequiredCols: Seq[String]): Seq[IndexLogEntry] = {
    indexes.filter { idx =>
      val allCols = idx.indexedColumns ++ idx.includedColumns

      // All required index columns should match one-to-one with all indexed columns and
      // vice-versa. All required columns must be present in the available index columns.
      SchemaUtils.escapeFieldNames(requiredIndexCols).toSet.equals(idx.indexedColumns.toSet) &&
      SchemaUtils.escapeFieldNames(allRequiredCols).forall(allCols.contains)
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
    val escapedMap = columnMapping.map {
      case (k, v) => SchemaUtils.escapeFieldName(k) -> SchemaUtils.escapeFieldName(v)
    }
    require(escapedMap.keys.toSet.equals(lIndex.indexedColumns.toSet))
    require(escapedMap.values.toSet.equals(rIndex.indexedColumns.toSet))

    val requiredRightIndexedCols = lIndex.indexedColumns.map(escapedMap)
    rIndex.indexedColumns.equals(requiredRightIndexedCols)
  }
}
