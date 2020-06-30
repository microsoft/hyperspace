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

package com.microsoft.hyperspace.actions

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.index.serde.LogicalPlanSerDeUtils
import com.microsoft.hyperspace.util.IndexNameUtils

/**
 * CreateActionBase provides functionality to write dataframe as covering index.
 */
private[actions] abstract class CreateActionBase(dataManager: IndexDataManager) {
  protected lazy val indexDataPath: Path = {
    dataManager
      .getLatestVersionId()
      .map(id => dataManager.getPath(id + 1))
      .getOrElse(dataManager.getPath(0))
  }

  protected def getIndexLogEntry(
      spark: SparkSession,
      df: DataFrame,
      indexConfig: IndexConfig,
      path: Path,
      sourceFiles: Seq[String]): IndexLogEntry = {
    val numBuckets = spark.sessionState.conf
      .getConfString(
        IndexConstants.INDEX_NUM_BUCKETS,
        IndexConstants.INDEX_NUM_BUCKETS_DEFAULT.toString)
      .toInt

    val signatureProvider = LogicalPlanSignatureProvider.create()

    val schema = {
      val allColumns = indexConfig.indexedColumns ++ indexConfig.includedColumns
      df.select(allColumns.head, allColumns.tail: _*).schema
    }

    // Here we use the unanalyzed logical plan returned by the parser, before analysis and
    // optimization. Analyzed and optimized plans have additional ser/de issues that are not
    // fully covered by the current ser/de framework.
    val serializedPlan =
      LogicalPlanSerDeUtils.serialize(df.queryExecution.logical, spark)

    val sourcePlanProperties = SparkPlan.Properties(
      serializedPlan,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(
          Seq(
            Signature(
              signatureProvider.name,
              signatureProvider.signature(df.queryExecution.optimizedPlan))))))

    // Note: Source files are fingerprinted as part of the serialized logical plan currently.
    val sourceDataProperties =
      Hdfs.Properties(Content("", Seq(Content.Directory("", sourceFiles, NoOpFingerprint()))))

    IndexLogEntry(
      indexConfig.indexName,
      CoveringIndex(
        CoveringIndex.Properties(
          CoveringIndex.Properties
            .Columns(indexConfig.indexedColumns, indexConfig.includedColumns),
          IndexLogEntry.schemaString(schema),
          numBuckets)),
      Content(path.toString, Seq()),
      Source(SparkPlan(sourcePlanProperties), Seq(Hdfs(sourceDataProperties))),
      Map())
  }

  protected def sourceFiles(df: DataFrame): Seq[String] =
    df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        location.allFiles.map(_.getPath.toString)
    }.flatten

  protected def write(spark: SparkSession, df: DataFrame, indexConfig: IndexConfig): Unit = {
    val numBuckets = spark.sessionState.conf
      .getConfString(
        IndexConstants.INDEX_NUM_BUCKETS,
        IndexConstants.INDEX_NUM_BUCKETS_DEFAULT.toString)
      .toInt

    val dfColumnNames = df.schema.fieldNames
    val resolvedIndexedColumns = indexConfig.indexedColumns.map(resolve(spark, _, dfColumnNames))
    val resolvedIncludedColumns =
      indexConfig.includedColumns.map(resolve(spark, _, dfColumnNames))
    val selectedColumns = resolvedIndexedColumns ++ resolvedIncludedColumns
    val indexDataFrame = df.select(selectedColumns.head, selectedColumns.tail: _*)

    // run job
    val repartitionedIndexDataFrame =
      indexDataFrame.repartition(numBuckets, resolvedIndexedColumns.map(df(_)): _*)

    // Save the index with the number of buckets specified.
    repartitionedIndexDataFrame.write
      .saveWithBuckets(
        repartitionedIndexDataFrame,
        indexDataPath.toString,
        numBuckets,
        resolvedIndexedColumns)
  }

  private def resolve(spark: SparkSession, dst: String, srcs: Array[String]): String = {
    srcs
      .find(src => IndexNameUtils.resolve(spark, dst, src))
      .getOrElse {
        throw HyperspaceException(
          s"Column $dst could not be resolved from available columns $srcs")
      }
  }
}
