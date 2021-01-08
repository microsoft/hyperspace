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
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.index.rankers.FilterIndexRanker
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEventLogging, HyperspaceIndexUsageEvent}
import com.microsoft.hyperspace.util.{HyperspaceConf, LogicalPlanUtils, ResolverUtils}

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
      case ExtractFilterNode(originalPlan, filter, outputColumns, filterColumns, _, _) =>
        try {
          val candidateIndexes =
            findCoveringIndexes(filter, outputColumns, filterColumns)
          FilterIndexRanker.rank(spark, filter, candidateIndexes) match {
            case Some(index) =>
              // As FilterIndexRule is not intended to support bucketed scan, we set
              // useBucketUnionForAppended as false. If it's true, Hybrid Scan can cause
              // unnecessary shuffle for appended data to apply BucketUnion for merging data.
              val transformedPlan =
                RuleUtils.transformPlanToUseIndex(
                  spark,
                  index,
                  originalPlan,
                  useBucketSpec = HyperspaceConf.useBucketSpecForFilterRule(spark),
                  useBucketUnionForAppended = false)
              logEvent(
                HyperspaceIndexUsageEvent(
                  AppInfo(
                    sparkContext.sparkUser,
                    sparkContext.applicationId,
                    sparkContext.appName),
                  Seq(index),
                  filter.toString,
                  transformedPlan.toString,
                  "Filter index rule applied."))
              transformedPlan
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
   * @return List of available candidate indexes on fsRelation for the given columns.
   */
  private def findCoveringIndexes(
      filter: Filter,
      outputColumns: Seq[String],
      filterColumns: Seq[String]): Seq[IndexLogEntry] = {
    RuleUtils.getLogicalRelation(filter) match {
      case Some(r) =>
        val indexManager = Hyperspace
          .getContext(spark)
          .indexCollectionManager

        // TODO: the following check only considers indexes in ACTIVE state for usage. Update
        //  the code to support indexes in transitioning states as well.
        //  See https://github.com/microsoft/hyperspace/issues/65
        val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

        val candidateIndexes = allIndexes.filter { index =>
          indexCoversPlan(
            outputColumns,
            filterColumns,
            index.indexedColumns,
            index.includedColumns)
        }

        // Get candidate via file-level metadata validation. This is performed after pruning
        // by column schema, as this might be expensive when there are numerous files in the
        // relation or many indexes to be checked.
        RuleUtils.getCandidateIndexes(spark, candidateIndexes, r)

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
      includedColumns: Seq[String]): Boolean = {
    val allColumnsInPlan = outputColumns ++ filterColumns
    val allColumnsInIndex = indexedColumns ++ includedColumns

    // TODO: Normalize predicates into CNF and incorporate more conditions.
    ResolverUtils.resolve(spark, indexedColumns.head, filterColumns).isDefined &&
    ResolverUtils.resolve(spark, allColumnsInPlan, allColumnsInIndex).isDefined
  }
}

object ExtractFilterNode {
  type returnType = (
      LogicalPlan, // original plan
      Filter,
      Seq[String], // output columns
      Seq[String], // filter columns
      LogicalPlan, // relation node
      Option[HadoopFsRelation])

  def unapply(plan: LogicalPlan): Option[returnType] = plan match {
    case project @ Project(
          _,
          filter @ Filter(
            condition: Expression,
            logicalRelation @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)))
        if !RuleUtils.isIndexApplied(logicalRelation) =>
      val projectColumnNames = CleanupAliases(project)
        .asInstanceOf[Project]
        .projectList
        .map(_.references.map(_.asInstanceOf[AttributeReference].name))
        .flatMap(_.toSeq)
      val filterColumnNames = condition.references.map(_.name).toSeq
      Some(project, filter, projectColumnNames, filterColumnNames, logicalRelation,

        Some(fsRelation))

    case project @ Project(
          _,
          filter @ Filter(
          condition: Expression,
          dataSourceV2Relation @ DataSourceV2Relation(_, _, _, _, _)))
        if !RuleUtils.isIndexApplied(dataSourceV2Relation) =>
      val projectColumnNames = CleanupAliases(project)
          .asInstanceOf[Project]
          .projectList
          .map(_.references.map(_.asInstanceOf[AttributeReference].name))
          .flatMap(_.toSeq)
      val filterColumnNames = condition.references.map(_.name).toSeq

      Some(project, filter, projectColumnNames, filterColumnNames, dataSourceV2Relation,
        None)

    case filter @ Filter(
          condition: Expression,
          logicalRelation @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))
        if !RuleUtils.isIndexApplied(logicalRelation) =>
      val relationColumnsName = logicalRelation.output.map(_.name)
      val filterColumnNames = condition.references.map(_.name).toSeq

      Some(filter, filter, relationColumnsName, filterColumnNames, logicalRelation,
        Some(fsRelation))

    case _ => None // plan does not match with any of filter index rule patterns
  }
}

object ExtractIndexSupportedLogicalPlan {
  def unapply(plan: LogicalPlan): Option[LogicalPlan] =
    Some(plan).filter(LogicalPlanUtils.isLogicalPlanSupported)
}
