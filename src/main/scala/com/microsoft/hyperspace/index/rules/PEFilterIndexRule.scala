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

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, BinaryComparison, EqualTo, Expression, UnaryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{IndexConstants, IndexLogEntry}
import com.microsoft.hyperspace.index.rankers.FilterIndexRanker
import com.microsoft.hyperspace.telemetry.HyperspaceEventLogging
import com.microsoft.hyperspace.util.ResolverUtils

/**
 * FilterIndex rule looks for opportunities in a logical plan to replace
 * a relation with an available covering index according to columns in
 * filter predicate.
 */
object PEFilterIndexRule
    extends Rule[LogicalPlan]
    with Logging
    with HyperspaceEventLogging
    with ActiveSparkSession {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {
      case ExtractFilterNode(originalPlan, filter, _, filterColumns) =>
        try {
          val candidateIndexes = findCoveringIndexes(filter, filterColumns)
          FilterIndexRanker.rank(spark, filter, candidateIndexes) match {
            case Some(index) => transformPlan(filter, index, originalPlan)
            case None => originalPlan
          }
        } catch {
          case e: Exception =>
            logWarning("Non fatal exception in running filter index rule: " + e.getMessage)
            originalPlan
        }
    }
  }

  def transformPlan(
      filter: Filter,
      index: IndexLogEntry,
      originalPlan: LogicalPlan): LogicalPlan = {
    // Filter useable files from original data source.
    val fileList = getFilteredFiles(filter, index)

    // Make InMemoryFileIndex from file list.
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    val returnVal = originalPlan transformDown {
      case l @ LogicalRelation(
            fs @ HadoopFsRelation(oldFileIndex: InMemoryFileIndex, _, _, _, _, _),
            _,
            _,
            _) =>
        val relation = provider.getRelation(l)
        val options = relation.partitionBasePath
          .map { basePath =>
            // Set "basePath" so that partitioned columns are also included in the output schema.
            Map("basePath" -> basePath)
          }
          .getOrElse(Map())
        val schema = StructType((l.schema ++ oldFileIndex.partitionSchema).distinct)
        val newFileIndex = new InMemoryFileIndex(spark, fileList, options, Some(schema))
        val fsOptions = fs.options ++ Map(IndexConstants.INDEX_RELATION_IDENTIFIER)

        l.copy(fs.copy(location = newFileIndex, options = fsOptions)(spark))
    }

    returnVal
  }

  def getFilteredFiles(filter: Filter, index: IndexLogEntry): Seq[Path] = {
    val condition = getIndexCompatibleCondition(filter.condition, index)

    val filteredDf =
      spark.read
        .parquet(index.content.files.map(_.toString): _*)
        .where(condition.sql)
        .select(IndexConstants.DATA_FILE_NAME_ID)
    val fileIds = filteredDf.rdd.map(r => r(0)).collect.toSet

    index.fileIdTracker.getFileToIdMap
      .filterKeys(k => fileIds.contains(index.fileIdTracker.getFileToIdMap(k)))
      .keys
      .map(f => new Path(f._1))
      .toSeq
  }

  private def extractConditions(condition: Expression): Seq[Expression] = condition match {
    case e: BinaryComparison => Seq(e)
    case e: UnaryExpression => Seq(e)
    case And(left, right) => extractConditions(left) ++ extractConditions(right)
    case _ => throw new IllegalStateException("Unsupported condition found")
  }

  /**
   * Choose only those conditions which work with the index. For filter queries, these conditions
   * should reference to only the head column of indexed columns.
   *
   * @param condition Complete filter condition expression.
   * @param index IndexLogEntry for which we are extracting the compatible sub-conditions.
   * @return Subset of sub-conditions which work with this index.
   */
  private def getIndexCompatibleCondition(
      condition: Expression,
      index: IndexLogEntry): Expression = {
    extractConditions(condition)
      .filter(_.references.forall(_.name.equals(index.indexedColumns.head)))
      .reduce(And)
  }

  private def findCoveringIndexes(
      filter: Filter,
      filterColumns: Seq[String]): Seq[IndexLogEntry] = {
    RuleUtils.getRelation(spark, filter) match {
      case Some(r) =>
        val indexManager = Hyperspace
          .getContext(spark)
          .indexCollectionManager

        val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

        val candidateIndexes = allIndexes.filter { index =>
          indexCoversPlan(filterColumns, index.indexedColumns)
        }

        RuleUtils.getCandidateIndexes(spark, candidateIndexes, r)

      case None => Nil
    }
  }

  private def indexCoversPlan(
      filterColumns: Seq[String],
      indexedColumns: Seq[String]): Boolean = {
    ResolverUtils.resolve(spark, indexedColumns.head, filterColumns).isDefined
  }
}
