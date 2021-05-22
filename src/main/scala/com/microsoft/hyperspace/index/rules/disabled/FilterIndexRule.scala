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

package com.microsoft.hyperspace.index.rules.disabled

import org.apache.spark.sql.catalyst.analysis.CleanupAliases
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}

import com.microsoft.hyperspace.index.IndexLogEntryTags
import com.microsoft.hyperspace.index.rankers.FilterIndexRanker
import com.microsoft.hyperspace.index.rules._
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.{PlanToIndexesMap, PlanToSelectedIndexMap}
import com.microsoft.hyperspace.util.{HyperspaceConf, ResolverUtils}

object FilterPlanNodeFilter extends QueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    if (candidateIndexes.isEmpty) {
      return Map.empty
    }

    // FilterPlanNodeFilter looks for below patterns, in ordered manner:
    //  Pattern-1: Scan -> Filter -> Project
    //  Pattern-2: Scan -> Filter
    // Pattern-2 covers the case where project node is eliminated or not present.
    // An example is when all columns are selected.
    plan match {
      case Project(_, Filter(_: Expression, ExtractRelation(relation)))
          if !RuleUtils.isIndexApplied(relation) =>
        candidateIndexes.filterKeys(relation.plan.equals(_))
      case Filter(_: Expression, ExtractRelation(relation))
          if !RuleUtils.isIndexApplied(relation) =>
        candidateIndexes.filterKeys(relation.plan.equals(_))
      case _ =>
        Map.empty
    }
  }
}

object FilterColumnFilter extends QueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    if (candidateIndexes.isEmpty || candidateIndexes.size != 1) {
      return Map.empty
    }

    val (filterColumnNames, projectColumnNames) = plan match {
      case project @ Project(_, _ @Filter(condition: Expression, ExtractRelation(relation)))
          if !RuleUtils.isIndexApplied(relation) =>
        val projectColumnNames = CleanupAliases(project)
          .asInstanceOf[Project]
          .projectList
          .map(_.references.map(_.asInstanceOf[AttributeReference].name))
          .flatMap(_.toSeq)
        val filterColumnNames = condition.references.map(_.name).toSeq
        (filterColumnNames, projectColumnNames)

      case Filter(condition: Expression, ExtractRelation(relation))
          if !RuleUtils.isIndexApplied(relation) =>
        val relationColumnNames = relation.plan.output.map(_.name)
        val filterColumnNames = condition.references.map(_.name).toSeq
        (filterColumnNames, relationColumnNames)
      case _ =>
        (Seq(), Seq())
    }

    // Filter candidate indexes if:
    //  1. Filter predicate's columns include the first 'indexed' column of the index.
    //  2. The index covers all columns from the filter predicate and output columns list.
    val (rel, indexes) = candidateIndexes.head
    val filteredIndexes =
      indexes.filter { index =>
        val ddColumns = index.derivedDataset.properties.columns
        withFilterReasonTag(
          plan,
          index,
          "The first indexed column should be in filter condition columns.") {
          ResolverUtils.resolve(spark, ddColumns.indexed.head, filterColumnNames).isDefined
        } &&
        withFilterReasonTag(
          plan,
          index,
          "Index does not contain required columns. Required columns: " +
            s"[${filterColumnNames ++ projectColumnNames}], indexed & included columns: " +
            s"[${ddColumns.indexed ++ ddColumns.included}]") {
          ResolverUtils
            .resolve(
              spark,
              filterColumnNames ++ projectColumnNames,
              ddColumns.indexed ++ ddColumns.included)
            .isDefined
        }
      }

    Map(rel -> filteredIndexes)
  }
}

object FilterRankFilter extends IndexRankFilter {
  override def apply(
      plan: LogicalPlan,
      applicableIndexes: PlanToIndexesMap): PlanToSelectedIndexMap = {
    if (applicableIndexes.isEmpty || applicableIndexes.size != 1
        || applicableIndexes.head._2.isEmpty) {
      Map.empty
    } else {
      val selected = FilterIndexRanker.rank(spark, plan, applicableIndexes.head._2).get
      setFilterReasonTagForRank(plan, applicableIndexes.head._2, selected)
      Map(applicableIndexes.head._1 -> selected)
    }
  }
}

/**
 * FilterIndexRule looks for opportunities in a logical plan to replace
 * a relation with an available hash partitioned index according to columns in
 * filter predicate.
 */
object FilterIndexRule extends HyperspaceRule {
  override val filtersOnQueryPlan: Seq[QueryPlanIndexFilter] =
    FilterPlanNodeFilter :: FilterColumnFilter :: Nil

  override val indexRanker: IndexRankFilter = FilterRankFilter

  override def applyIndex(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): LogicalPlan = {
    if (indexes.isEmpty || (indexes.size != 1)) {
      return plan
    }

    // As FilterIndexRule is not intended to support bucketed scan, we set
    // useBucketUnionForAppended as false. If it's true, Hybrid Scan can cause
    // unnecessary shuffle for appended data to apply BucketUnion for merging data.
    RuleUtils.transformPlanToUseIndex(
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
    (50 * (commonBytes.toFloat / relation.allFileSizeInBytes)).round
  }
}
