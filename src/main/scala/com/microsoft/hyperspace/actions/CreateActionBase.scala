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
import org.apache.spark.sql.sources.DataSourceRegister

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer

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
      path: Path): IndexLogEntry = {
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

    signatureProvider.signature(df.queryExecution.optimizedPlan) match {
      case Some(s) =>
        val relations = sourceRelations(df)
        // Currently we only support to create an index on a LogicalRelation
        assert(relations.size == 1)

        val sourcePlanProperties = SparkPlan.Properties(
          relations,
          null,
          null,
          LogicalPlanFingerprint(
            LogicalPlanFingerprint.Properties(Seq(Signature(signatureProvider.name, s)))))

        IndexLogEntry(
          indexConfig.indexName,
          CoveringIndex(
            CoveringIndex.Properties(
              CoveringIndex.Properties
                .Columns(indexConfig.indexedColumns, indexConfig.includedColumns),
              IndexLogEntry.schemaString(schema),
              numBuckets)),
          Content(path.toString, Seq()),
          Source(SparkPlan(sourcePlanProperties)),
          Map())

      case None => throw HyperspaceException("Invalid plan for creating an index.")
    }
  }

  protected def sourceRelations(df: DataFrame): Seq[Relation] =
    df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, dataSchema, _, fileFormat, _),
          _,
          _,
          _) =>
        val files = location.allFiles.map(_.getPath.toString)
        // Note that source files are currently fingerprinted when the optimized plan is
        // fingerprinted by LogicalPlanFingerprint.
        val sourceDataProperties =
          Hdfs.Properties(Content("", Seq(Content.Directory("", files, NoOpFingerprint()))))
        val fileFormatName = fileFormat match {
          case d: DataSourceRegister => d.shortName
          case other => throw HyperspaceException(s"Unsupported file format: $other")
        }
        Relation(
          location.rootPaths.map(_.toString),
          Hdfs(sourceDataProperties),
          dataSchema.json,
          fileFormatName)
    }

  protected def write(spark: SparkSession, df: DataFrame, indexConfig: IndexConfig): Unit = {
    val numBuckets = spark.sessionState.conf
      .getConfString(
        IndexConstants.INDEX_NUM_BUCKETS,
        IndexConstants.INDEX_NUM_BUCKETS_DEFAULT.toString)
      .toInt
    val selectedColumns = indexConfig.indexedColumns ++ indexConfig.includedColumns
    val indexDataFrame = df.select(selectedColumns.head, selectedColumns.tail: _*)
    val indexColNames = indexConfig.indexedColumns

    // run job
    val repartitionedIndexDataFrame =
      indexDataFrame.repartition(numBuckets, indexColNames.map(df(_)): _*)

    // Save the index with the number of buckets specified.
    repartitionedIndexDataFrame.write
      .saveWithBuckets(
        repartitionedIndexDataFrame,
        indexDataPath.toString,
        numBuckets,
        indexColNames)
  }
}
