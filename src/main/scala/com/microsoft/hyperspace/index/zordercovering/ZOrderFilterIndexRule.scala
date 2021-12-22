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

package com.microsoft.hyperspace.index.zordercovering

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.IndexLogEntryTags
import com.microsoft.hyperspace.index.covering.{CoveringIndexRuleUtils, FilterPlanNodeFilter}
import com.microsoft.hyperspace.index.plananalysis.FilterReasons
import com.microsoft.hyperspace.index.rules.{HyperspaceRule, IndexRankFilter, IndexTypeFilter, QueryPlanIndexFilter, RuleUtils}
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.{PlanToIndexesMap, PlanToSelectedIndexMap}
import com.microsoft.hyperspace.util.{HyperspaceConf, ResolverUtils}

/**
 * ZOrderFilterColumnFilter filters indexes out if
 *   1) an index doesn't have all required output columns.
 *   2) the filter condition doesn't include any of indexed columns of the index.
 *
 * The only difference between FilterColumnFilter for CoveringIndex is allowing all indexed columns
 * in the filter condition, not just the first indexed column.
 */
object ZOrderFilterColumnFilter extends QueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    if (candidateIndexes.isEmpty || candidateIndexes.size != 1) {
      return Map.empty
    }

    val (projectColumnNames, filterColumnNames) = RuleUtils.getProjectAndFilterColumns(plan)

    // Filter candidate indexes if:
    //  1. Filter predicate's columns include any of indexed columns of the index.
    //  2. The index covers all columns from the filter predicate and output columns list.
    val (rel, indexes) = candidateIndexes.head
    val filteredIndexes =
      indexes.filter { index =>
        withFilterReasonTag(
          plan,
          index,
          FilterReasons.IneligibleFilterCondition("No indexed column in filter condition")) {
          index.derivedDataset.indexedColumns.exists(
            col =>
              ResolverUtils
                .resolve(spark, col, filterColumnNames)
                .isDefined)
        } &&
        withFilterReasonTag(
          plan,
          index,
          FilterReasons.MissingRequiredCol(
            (filterColumnNames ++ projectColumnNames).toSet.mkString(","),
            index.derivedDataset.referencedColumns.mkString(","))) {
          ResolverUtils
            .resolve(
              spark,
              filterColumnNames ++ projectColumnNames,
              index.derivedDataset.referencedColumns)
            .isDefined
        }
      }

    Map(rel -> filteredIndexes)
  }
}

/**
 * IndexRankFilter selects the best applicable index.
 */
object ZOrderFilterRankFilter extends IndexRankFilter {
  override def apply(
      plan: LogicalPlan,
      applicableIndexes: PlanToIndexesMap): PlanToSelectedIndexMap = {
    if (applicableIndexes.isEmpty || applicableIndexes.size != 1
      || applicableIndexes.head._2.isEmpty) {
      Map.empty
    } else {
      // TODO Enhance rank algorithm for z-order covering index. Currently, we pick an index
      //  with the least number of indexed column which might have a better min/max distribution
      //  for data skipping. However, apparently it is not the best.
      //  For example, a high cardinality indexed column could be better for data skipping.
      val selected = applicableIndexes.head._2.minBy(_.indexedColumns.length)
      setFilterReasonTagForRank(plan, applicableIndexes.head._2, selected)
      Map(applicableIndexes.head._1 -> selected)
    }
  }
}

/**
 * ZOrderFilterIndexRule looks for opportunities in a logical plan to replace
 * a relation with an available z-ordered index according to columns in filter predicate.
 */
object ZOrderFilterIndexRule extends HyperspaceRule {
  override val filtersOnQueryPlan: Seq[QueryPlanIndexFilter] =
    IndexTypeFilter[ZOrderCoveringIndex]() :: FilterPlanNodeFilter ::
      ZOrderFilterColumnFilter :: Nil

  override val indexRanker: IndexRankFilter = ZOrderFilterRankFilter

  override def applyIndex(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): LogicalPlan = {
    if (indexes.isEmpty || (indexes.size != 1)) {
      return plan
    }

    // As FilterIndexRule is not intended to support bucketed scan, we set
    // useBucketUnionForAppended as false. If it's true, Hybrid Scan can cause
    // unnecessary shuffle for appended data to apply BucketUnion for merging data.
    CoveringIndexRuleUtils.transformPlanToUseIndex(
      spark,
      indexes.head._2,
      plan,
      useBucketSpec = HyperspaceConf.useBucketSpecForFilterRule(spark),
      useBucketUnionForAppended = false)
  }

  override def score(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): Int = {
    if (indexes.isEmpty || (indexes.size != 1)) {
      return 0
    }

    val candidateIndex = indexes.head._2
    // Filter index rule
    val relation = RuleUtils.getRelation(spark, plan).get
    val commonBytes = candidateIndex
      .getTagValue(relation.plan, IndexLogEntryTags.COMMON_SOURCE_SIZE_IN_BYTES)
      .getOrElse {
        relation.allFileInfos.foldLeft(0L) { (res, f) =>
          if (candidateIndex.sourceFileInfoSet.contains(f)) {
            res + f.size // count, total bytes
          } else {
            res
          }
        }
      }

    // TODO: Enhance scoring function.
    //  See https://github.com/microsoft/hyperspace/issues/444
    // ZOrderCoveringIndex should be prior to CoveringIndex for a filter query.
    (60 * (commonBytes.toFloat / relation.allFileSizeInBytes)).round
  }
}
