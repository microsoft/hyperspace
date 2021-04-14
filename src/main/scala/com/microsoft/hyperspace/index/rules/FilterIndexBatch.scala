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

import org.apache.spark.sql.catalyst.analysis.CleanupAliases
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}

import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags}
import com.microsoft.hyperspace.index.rankers.FilterIndexRanker
import com.microsoft.hyperspace.util.{HyperspaceConf, ResolverUtils}

object FilterNodeCheck extends HyperspacePlanCheck {
  override def apply(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): Map[LogicalPlan, Seq[IndexLogEntry]] = {
    if (indexes.isEmpty) {
      return Map.empty
    }
    plan match {
      case Project(_, Filter(_: Expression, ExtractRelation(relation)))
          if !RuleUtils.isIndexApplied(relation) =>
        indexes.filterKeys(relation.plan.equals(_))
      case Filter(_: Expression, ExtractRelation(relation))
          if !RuleUtils.isIndexApplied(relation) =>
        indexes.filterKeys(relation.plan.equals(_))
      case _ =>
        Map.empty
    }
  }

  override def reason: String = {
    "FilterIndex: no applicable filter node or another index is applied."
  }
}

object FilterColumnCheck extends HyperspacePlanCheck {
  override def apply(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): Map[LogicalPlan, Seq[IndexLogEntry]] = {
    if (indexes.isEmpty) {
      return Map.empty
    }
    val (filterColumnNames, projectColumnNames) = plan match {
      case project @ Project(
            _,
            filter @ Filter(condition: Expression, ExtractRelation(relation))) =>
        val projectColumnNames = CleanupAliases(project)
          .asInstanceOf[Project]
          .projectList
          .map(_.references.map(_.asInstanceOf[AttributeReference].name))
          .flatMap(_.toSeq)
        val filterColumnNames = condition.references.map(_.name).toSeq
        (filterColumnNames, projectColumnNames)

      case filter @ Filter(condition: Expression, ExtractRelation(relation))
          if !RuleUtils.isIndexApplied(relation) =>
        val relationColumnNames = relation.plan.output.map(_.name)
        val filterColumnNames = condition.references.map(_.name).toSeq
        (filterColumnNames, relationColumnNames)

      case _ =>
        (Seq(), Seq())
    }

    val candidateIndexes =
      indexes.head._2.filter { index =>
        val ddColumns = index.derivedDataset.properties.columns
        ResolverUtils.resolve(spark, ddColumns.indexed.head, filterColumnNames).isDefined &&
        ResolverUtils
          .resolve(
            spark,
            filterColumnNames ++ projectColumnNames,
            ddColumns.indexed ++ ddColumns.included)
          .isDefined
      }
    Map(indexes.head._1 -> candidateIndexes)
  }

  override def reason: String = {
    "FilterIndex: filter column does not match."
  }
}

object FilterRankCheck extends HyperspacePlanCheck {
  override def apply(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): Map[LogicalPlan, Seq[IndexLogEntry]] = {
    if (indexes.isEmpty || indexes.size != 1 || indexes.head._2.size == 0) {
      Map.empty
    } else {
      Map(indexes.head._1 -> Seq(FilterIndexRanker.rank(spark, plan, indexes.head._2).get))
    }
  }

  override def reason: String = "FilterIndex: another index has a higher priority."
}

object FilterIndexBatch extends HyperspaceBatch {

  val checkBatch = FilterNodeCheck :: FilterColumnCheck :: FilterRankCheck :: Nil

  override def applyIndex(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): LogicalPlan = {
    if (indexes.isEmpty || (indexes.size != 1 && indexes.head._2.size != 1)) {
      return plan
    }
    RuleUtils.transformPlanToUseIndex(
      spark,
      indexes.head._2.head,
      plan,
      useBucketSpec = HyperspaceConf.useBucketSpecForFilterRule(spark),
      useBucketUnionForAppended = false)
  }

  override def score(plan: LogicalPlan, indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): Int = {
    if (indexes.isEmpty || (indexes.size != 1 && indexes.head._2.size != 1)) {
      return 0
    }

    val candidateIndex = indexes.head._2.head
    // Filter index rule
    val relation = RuleUtils.getRelation(spark, plan).get
    val commonBytes = candidateIndex
      .getTagValue(relation.plan, IndexLogEntryTags.COMMON_SOURCE_SIZE_IN_BYTES)
      .getOrElse {
        relation.allFileInfos.foldLeft(0L) { (res, f) =>
          if (candidateIndex.sourceFileInfoSet.contains(f)) {
            (res + f.size) // count, total bytes
          } else {
            res
          }
        }
      }

    // TODO enhance scoring
    (50 * (commonBytes * 1.0f / relation.allFileSizeInBytes)).round
  }
}
