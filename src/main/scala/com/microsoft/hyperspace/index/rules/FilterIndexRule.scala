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

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.CleanupAliases
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{IndexLogEntry, LogicalPlanSignatureProvider}

/**
 * FilterIndex rule looks for opportunities in a logical plan to replace
 * a relation with an available covering index according to columns in
 * filter predicate.
 */
object FilterIndexRule extends Rule[LogicalPlan] with Logging {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    // FilterIndex rule looks for (Scan -> Filter -> Project) pattern to trigger
    // a transformation. Currently, this rule replaces a relation with an index when:
    //  1. The index covers all columns from the filter predicate and project, and
    //  2. Filter predicate's columns include the first 'indexed' column of the index.
    plan transform {
      case project @ Project(
            _,
            Filter(
              condition: Expression,
              logicalRelation @ LogicalRelation(
                fsRelation @ HadoopFsRelation(location, _, _, _, _, _),
                _,
                _,
                _))) =>
        try {
          // "CleanupAliases" cleans up Alias expression inside a logical plan
          // such that its children would not have any Alias expressions.
          // Calling "references" on the expression in projectList ensures
          // we will get the correct (original) column names.
          val projectColumnNames = CleanupAliases(project)
            .asInstanceOf[Project]
            .projectList
            .map(_.references.map(_.asInstanceOf[AttributeReference].name))
            .flatMap(_.toSeq)
          val filterColumnNames = condition.references.map(_.name).toSeq

          replaceWithIndexIfPlanCovered(
            project,
            projectColumnNames,
            filterColumnNames,
            logicalRelation,
            fsRelation,
            location)
        } catch {
          case e: Exception =>
            logWarning("Non fatal exception in running filter index rule: " + e.getMessage)
            project
        }
    }
  }

  /**
   * For a given relation, check its available indexes and replace it with the top-ranked index
   * (according to cost model).
   *
   * @param project  top-most node in the logical plan that is being optimized.
   * @param projectColumns List of project columns.
   * @param filterColumns  List of columns in filter predicate.
   * @param logicalRelation  child logical relation in the subplan.
   * @param fsRelation Input relation in the subplan.
   * @param location FileIndex associated with the locations of all files comprising fsRelation.
   * @return transformed logical plan in which original fsRelation is replaced by
   *         the top-ranked index.
   */
  private def replaceWithIndexIfPlanCovered(
      project: Project,
      projectColumns: Seq[String],
      filterColumns: Seq[String],
      logicalRelation: LogicalRelation,
      fsRelation: HadoopFsRelation,
      location: FileIndex): Project = {
    require(project.child.isInstanceOf[Filter])

    val candidateIndexes =
      findCoveringIndexes(project, projectColumns, filterColumns, fsRelation)
    rank(candidateIndexes) match {
      case Some(index) =>
        val spark = fsRelation.sparkSession
        val newLocation =
          new InMemoryFileIndex(spark, Seq(new Path(index.content.root)), Map(), None)

        val newRelation = HadoopFsRelation(
          newLocation,
          new StructType(),
          index.schema,
          None, // Do not set BucketSpec to avoid limiting Spark's degree of parallelism
          new ParquetFileFormat,
          Map())(spark)

        val filter = project.child.asInstanceOf[Filter]
        val newOutput =
          logicalRelation.output.filter(attr => index.schema.fieldNames.contains(attr.name))

        project.copy(
          child =
            filter.copy(child = logicalRelation.copy(relation = newRelation, output = newOutput)))

      case None => project // No candidate index found
    }
  }

  /**
   * For a given relation, find all available indexes on it which fully cover given project and
   * filter columns.
   *
   * TODO: This method is duplicated in FilterIndexRule and JoinIndexRule. Deduplicate.
   *
   * @param project top-most node in the logical plan that is being optimized.
   * @param projectColumns List of project columns.
   * @param filterColumns List of columns in filter predicate.
   * @param fsRelation Input relation in the subplan.
   * @return List of available candidate indexes on fsRelation for the given columns.
   */
  private def findCoveringIndexes(
      project: Project,
      projectColumns: Seq[String],
      filterColumns: Seq[String],
      fsRelation: HadoopFsRelation): Seq[IndexLogEntry] = {

    // map of signature provider to signature for this subplan
    val signatureMap: mutable.Map[String, String] = mutable.Map()

    def signatureValid(entry: IndexLogEntry): Boolean = {
      val sourcePlanSignatures = entry.source.plan.properties.fingerprint.properties.signatures
      assert(sourcePlanSignatures.length == 1)
      val sourcePlanSignature = sourcePlanSignatures.head

      if (!signatureMap.contains(sourcePlanSignature.provider)) {
        val signature = LogicalPlanSignatureProvider
          .create(sourcePlanSignature.provider)
          .signature(project)
        signatureMap.put(sourcePlanSignature.provider, signature)
      }

      signatureMap(sourcePlanSignature.provider).equals(sourcePlanSignature.value)
    }

    val allIndexes = Hyperspace
      .getContext(SparkSession.getActiveSession.get)
      .indexCollectionManager
      .getIndexes(Seq(Constants.States.ACTIVE))

    // TODO: the following check only considers indexes in ACTIVE state for usage. Update
    //  the code to support indexes in transitioning states as well.
    allIndexes.filter { index =>
      index.created &&
      signatureValid(index) &&
      indexCoversPlan(
        projectColumns,
        filterColumns,
        index.indexedColumns,
        index.includedColumns,
        fsRelation.fileFormat)
    }

  }

  /**
   * For a given index and filter and project columns, check if index covers them
   * according to the FilterIndex rule requirement.
   *
   * @param projectColumns List of project columns.
   * @param filterColumns List of columns in filter predicate.
   * @param indexedColumns List of indexed columns (e.g. from an index being checked)
   * @param includedColumns List of included columns (e.g. from an index being checked)
   * @param fileFormat FileFormat for input relation in original logical plan.
   * @return 'true' if
   *         1. Index fully covers project and filter columns, and
   *         2. Filter predicate contains first column in index's 'indexed' columns.
   */
  private def indexCoversPlan(
      projectColumns: Seq[String],
      filterColumns: Seq[String],
      indexedColumns: Seq[String],
      includedColumns: Seq[String],
      fileFormat: FileFormat): Boolean = {
    val allColumnsInPlan = projectColumns ++ filterColumns
    val allColumnsInIndex = indexedColumns ++ includedColumns

    // TODO: Normalize predicates into CNF and incorporate more conditions.
    filterColumns.contains(indexedColumns.head) &&
    allColumnsInPlan.forall(allColumnsInIndex.contains)
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
