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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.analysis.CleanupAliases
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEventLogging, HyperspaceIndexUsageEvent}
import com.microsoft.hyperspace.util.ResolverUtils

/**
 * FilterIndex rule looks for opportunities in a logical plan to replace
 * a relation with an available covering index according to columns in
 * filter predicate.
 */
object FilterIndexRule
    extends Rule[LogicalPlan]
    with Logging
    with HyperspaceEventLogging
    with ActiveSparkSession {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    // FilterIndex rule looks for below patterns, in ordered manner, to trigger a transformation:
    //  Pattern-1: Scan -> Filter -> Project
    //  Pattern-2: Scan -> Filter
    // Pattern-2 covers the case where project node is eliminated or not present. An example is
    // when all columns are selected.
    // Currently, this rule replaces a relation with an index when:
    //  1. The index covers all columns from the filter predicate and output columns list, and
    //  2. Filter predicate's columns include the first 'indexed' column of the index.
    plan transformDown {
      case ExtractFilterNode(
          originalPlan,
          filter,
          outputColumns,
          filterColumns,
          logicalRelation,
          fsRelation) =>
        try {
          val candidateIndexes =
            findCoveringIndexes(filter, outputColumns, filterColumns, fsRelation)
          rank(candidateIndexes) match {
            case Some(index) =>
              val replacedPlan = RuleUtils.getReplacementPlanForFilter(spark, index, originalPlan)
              logEvent(
                HyperspaceIndexUsageEvent(
                  AppInfo(
                    sparkContext.sparkUser,
                    sparkContext.applicationId,
                    sparkContext.appName),
                  Seq(index),
                  filter.toString,
                  replacedPlan.toString,
                  "Filter index rule applied."))
              replacedPlan
            case None => originalPlan
          }
        } catch {
          case e: Exception =>
            logWarning("Non fatal exception in running filter index rule: " + e.getMessage)
            originalPlan
        }
    }
  }

  /**
   * For a given relation, find all available indexes on it which fully cover given output and
   * filter columns.
   *
   * @param filter Filter node in the subplan that is being optimized.
   * @param outputColumns List of output columns in subplan.
   * @param filterColumns List of columns in filter predicate.
   * @param fsRelation Input relation in the subplan.
   * @return List of available candidate indexes on fsRelation for the given columns.
   */
  private def findCoveringIndexes(
      filter: Filter,
      outputColumns: Seq[String],
      filterColumns: Seq[String],
      fsRelation: HadoopFsRelation): Seq[IndexLogEntry] = {
    RuleUtils.getLogicalRelation(filter) match {
      case Some(r) =>
        val indexManager = Hyperspace
          .getContext(spark)
          .indexCollectionManager
        val candidateIndexes = RuleUtils.getCandidateIndexes(spark, indexManager, r)

        candidateIndexes.filter { index =>
          indexCoversPlan(
            outputColumns,
            filterColumns,
            index.indexedColumns,
            index.includedColumns,
            fsRelation.fileFormat)
        }

      case None => Nil // There is zero or more than one LogicalRelation nodes in Filter's subplan
    }
  }

  /**
   * For a given index and filter and output columns, check if index covers them
   * according to the FilterIndex rule requirement.
   *
   * @param outputColumns List of output columns in subplan.
   * @param filterColumns List of columns in filter predicate.
   * @param indexedColumns List of indexed columns (e.g. from an index being checked)
   * @param includedColumns List of included columns (e.g. from an index being checked)
   * @param fileFormat FileFormat for input relation in original logical plan.
   * @return 'true' if
   *         1. Index fully covers output and filter columns, and
   *         2. Filter predicate contains first column in index's 'indexed' columns.
   */
  private def indexCoversPlan(
      outputColumns: Seq[String],
      filterColumns: Seq[String],
      indexedColumns: Seq[String],
      includedColumns: Seq[String],
      fileFormat: FileFormat): Boolean = {
    val allColumnsInPlan = outputColumns ++ filterColumns
    val allColumnsInIndex = indexedColumns ++ includedColumns

    // TODO: Normalize predicates into CNF and incorporate more conditions.
    ResolverUtils.resolve(spark, indexedColumns.head, filterColumns).isDefined &&
    ResolverUtils.resolve(spark, allColumnsInPlan, allColumnsInIndex).isDefined
  }

  /**
   * @param candidates List of all indexes that fully cover logical plan.
   * @return top-most index which is expected to maximize performance gain
   *         according to ranking algorithm.
   */
  private def rank(candidates: Seq[IndexLogEntry]): Option[IndexLogEntry] = {
    // TODO: Add ranking algorithm to sort candidates.
    candidates match {
      case Nil => None
      case _ => Some(candidates.head)
    }
  }
}

object ExtractFilterNode {
  type returnType = (
      LogicalPlan, // original plan
      Filter,
      Seq[String], // output columns
      Seq[String], // filter columns
      LogicalRelation,
      HadoopFsRelation)

  def unapply(plan: LogicalPlan): Option[returnType] = plan match {
    case project @ Project(
          _,
          filter @ Filter(
            condition: Expression,
            logicalRelation @ LogicalRelation(
              fsRelation @ HadoopFsRelation(_, _, _, _, _, _),
              _,
              _,
              _))) =>
      val projectColumnNames = CleanupAliases(project)
        .asInstanceOf[Project]
        .projectList
        .map(_.references.map(_.asInstanceOf[AttributeReference].name))
        .flatMap(_.toSeq)
      val filterColumnNames = condition.references.map(_.name).toSeq

      Some(project, filter, projectColumnNames, filterColumnNames, logicalRelation, fsRelation)

    case filter @ Filter(
          condition: Expression,
          logicalRelation @ LogicalRelation(
            fsRelation @ HadoopFsRelation(_, _, _, _, _, _),
            _,
            _,
            _)) =>
      val relationColumnsName = logicalRelation.output.map(_.name)
      val filterColumnNames = condition.references.map(_.name).toSeq

      Some(filter, filter, relationColumnsName, filterColumnNames, logicalRelation, fsRelation)

    case _ => None // plan does not match with any of filter index rule patterns
  }
}
