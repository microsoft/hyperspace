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

package com.microsoft.hyperspace.index.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.CleanupAliases
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.IndexConstants
import com.microsoft.hyperspace.index.sources.FileBasedRelation

object RuleUtils {

  /**
   * Check if an index was applied the given relation or not.
   * This can be determined by an identifier in [[FileBasedRelation]]'s options.
   *
   * @param relation FileBasedRelation to check if an index is already applied.
   * @return true if an index is applied to the given relation. Otherwise false.
   */
  def isIndexApplied(relation: FileBasedRelation): Boolean = {
    relation.options.exists(_.equals(IndexConstants.INDEX_RELATION_IDENTIFIER))
  }

  /**
   * Extract the relation node if the given logical plan is linear.
   *
   * @param plan Logical plan to extract a relation node from.
   * @return If the plan is linear and the relation node is supported, the [[FileBasedRelation]]
   *         object that wraps the relation node. Otherwise None.
   */
  def getRelation(spark: SparkSession, plan: LogicalPlan): Option[FileBasedRelation] = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    val leaves = plan.collectLeaves()
    if (leaves.size == 1 && provider.isSupportedRelation(leaves.head)) {
      Some(provider.getRelation(leaves.head))
    } else {
      None
    }
  }

  /**
   * Extract project and filter columns when the given plan is Project-Filter-Relation
   * or Filter-Relation. Otherwise, return empty lists.
   *
   * @param plan Logical plan to extract project and filter columns.
   * @return A pair of project column names and filter column names
   */
  def getProjectAndFilterColumns(plan: LogicalPlan): (Seq[String], Seq[String]) = {
    plan match {
      case project @ Project(_, _ @Filter(condition: Expression, ExtractRelation(relation)))
          if !isIndexApplied(relation) =>
        val projectColumnNames = CleanupAliases(project)
          .asInstanceOf[Project]
          .projectList
          .map(_.references.map(_.asInstanceOf[AttributeReference].name))
          .flatMap(_.toSeq)
        val filterColumnNames = condition.references.map(_.name).toSeq
        (projectColumnNames, filterColumnNames)

      case Filter(condition: Expression, ExtractRelation(relation))
          if !isIndexApplied(relation) =>
        val relationColumnNames = relation.plan.output.map(_.name)
        val filterColumnNames = condition.references.map(_.name).toSeq
        (relationColumnNames, filterColumnNames)
      case _ =>
        (Seq(), Seq())
    }
  }
}
