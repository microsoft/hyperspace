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

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags}
import com.microsoft.hyperspace.util.{HyperspaceConf, ResolverUtils}

object FilterNodeCheck extends HyperspacePlanCheck {
  override def apply(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): Map[LogicalPlan, Seq[IndexLogEntry]] = {
    if (indexes.isEmpty) {
      return Map.empty
    }
    val res = plan match {
      case project @ Project(_, filter @ Filter(condition: Expression, ExtractRelation(relation)))
          if !RuleUtils.isIndexApplied(relation) =>
        indexes.filterKeys(relation.plan.equals(_))
      case filter @ Filter(condition: Expression, ExtractRelation(relation))
          if !RuleUtils.isIndexApplied(relation) =>
        indexes.filterKeys(relation.plan.equals(_))
      case _ =>
        Map[LogicalPlan, Seq[IndexLogEntry]]()
    }
    res
  }

  override def reason: String = {
    "No available filter node."
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
        (projectColumnNames, filterColumnNames)

      case filter @ Filter(condition: Expression, ExtractRelation(relation))
          if !RuleUtils.isIndexApplied(relation) =>
        val relationColumnNames = relation.plan.output.map(_.name)
        val filterColumnNames = condition.references.map(_.name).toSeq
        (filterColumnNames, relationColumnNames)

      case _ =>
        (Seq(), Seq())
    }
    val candidateIndexes = indexes.head._2.filter { index =>
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
    "Filter column does not match."
  }
}

object FilterIndexBatch extends HyperspaceBatch {

  val checkBatch = FilterNodeCheck :: FilterColumnCheck :: Nil

  override def applyIndex(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): LogicalPlan = {
    if (!(indexes.size == 1 && indexes.head._2.size == 1)) {
      return plan
    }
    // assert (indexes.size == 1 && indexes.head._2.size == 1)
    // Pick the first candidate index.
    val candidateIndex = indexes.head._2.head
    RuleUtils.transformPlanToUseIndex(
      spark,
      candidateIndex,
      plan,
      useBucketSpec = HyperspaceConf.useBucketSpecForFilterRule(spark),
      useBucketUnionForAppended = false)
  }

  override def score(plan: LogicalPlan, indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): Int = {
    if (indexes.isEmpty || !(indexes.size == 1 && indexes.head._2.size == 1)) {
      return 0
    }
    val candidateIndex = indexes.head._2.head
    // Filter index rule
    val provider = Hyperspace.getContext(spark).sourceProviderManager
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
    (50 * (commonBytes * 0.1f / relation.allFileSizeInBytes)).round
  }
}
