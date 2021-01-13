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
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.util.{HyperspaceConf, LogicalPlanUtils, PathUtils, ResolverUtils}

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

  protected val fileIdTracker = new FileIdTracker

  protected def numBucketsForIndex(spark: SparkSession): Int = {
    HyperspaceConf.numBucketsForIndex(spark)
  }

  protected def hasLineage(spark: SparkSession): Boolean = {
    HyperspaceConf.indexLineageEnabled(spark)
  }

  protected def getIndexLogEntry(
      spark: SparkSession,
      df: DataFrame,
      indexConfig: IndexConfig,
      path: Path): IndexLogEntry = {
    val absolutePath = PathUtils.makeAbsolute(path, spark.sessionState.newHadoopConf())
    val numBuckets = numBucketsForIndex(spark)

    val signatureProvider = LogicalPlanSignatureProvider.create()

    // Resolve the passed column names with existing column names from the dataframe.
    val (indexDataFrame, resolvedIndexedColumns, resolvedIncludedColumns) =
      prepareIndexDataFrame(spark, df, indexConfig)

    signatureProvider.signature(df.queryExecution.optimizedPlan) match {
      case Some(s) =>
        val relations = sourceRelations(spark, df)
        assert(relations.size == 1)

        val sourcePlanProperties = SparkPlan.Properties(
          relations,
          null,
          null,
          LogicalPlanFingerprint(
            LogicalPlanFingerprint.Properties(Seq(Signature(signatureProvider.name, s)))))

        val coveringIndexProperties =
          (hasLineageProperty(spark) ++ hasParquetAsSourceFormatProperty(spark, df)).toMap

        IndexLogEntry(
          indexConfig.indexName,
          CoveringIndex(
            CoveringIndex.Properties(
              CoveringIndex.Properties
                .Columns(resolvedIndexedColumns, resolvedIncludedColumns),
              IndexLogEntry.schemaString(indexDataFrame.schema),
              numBuckets,
              coveringIndexProperties)),
          Content.fromDirectory(absolutePath, fileIdTracker),
          Source(SparkPlan(sourcePlanProperties)),
          Map())

      case None => throw HyperspaceException("Invalid plan for creating an index.")
    }
  }

  private def hasParquetAsSourceFormatProperty(
      spark: SparkSession,
      df: DataFrame): Option[(String, String)] = {
    if (Hyperspace.getContext(spark).sourceProviderManager
        .hasParquetAsSourceFormat(df.queryExecution.optimizedPlan)) {
      Some(IndexConstants.HAS_PARQUET_AS_SOURCE_FORMAT_PROPERTY -> "true")
    } else {
      None
    }
  }

  private def hasLineageProperty(spark: SparkSession): Option[(String, String)] = {
    if (hasLineage(spark)) {
      Some(IndexConstants.LINEAGE_PROPERTY -> "true")
    } else {
      None
    }
  }

  protected def sourceRelations(spark: SparkSession, df: DataFrame): Seq[Relation] =
    df.queryExecution.optimizedPlan.collect {
      case p: LogicalPlan if LogicalPlanUtils.hasSupportedLogicalRelation(p) =>
        Hyperspace.getContext(spark).sourceProviderManager.createRelation(p, fileIdTracker)
    }

  protected def write(spark: SparkSession, df: DataFrame, indexConfig: IndexConfig): Unit = {
    val numBuckets = numBucketsForIndex(spark)

    val (indexDataFrame, resolvedIndexedColumns, _) =
      prepareIndexDataFrame(spark, df, indexConfig)

    // run job
    val repartitionedIndexDataFrame =
      indexDataFrame.repartition(numBuckets, resolvedIndexedColumns.map(df(_)): _*)

    // Save the index with the number of buckets specified.
    repartitionedIndexDataFrame.write
      .saveWithBuckets(
        repartitionedIndexDataFrame,
        indexDataPath.toString,
        numBuckets,
        resolvedIndexedColumns,
        SaveMode.Overwrite)
  }

  private def resolveConfig(
      df: DataFrame,
      indexConfig: IndexConfig): (Seq[String], Seq[String]) = {
    val spark = df.sparkSession
    val dfColumnNames = df.schema.fieldNames
    val indexedColumns = indexConfig.indexedColumns
    val includedColumns = indexConfig.includedColumns
    val resolvedIndexedColumns = ResolverUtils.resolve(spark, indexedColumns, dfColumnNames)
    val resolvedIncludedColumns = ResolverUtils.resolve(spark, includedColumns, dfColumnNames)

    (resolvedIndexedColumns, resolvedIncludedColumns) match {
      case (Some(indexed), Some(included)) => (indexed, included)
      case _ =>
        val unresolvedColumns = (indexedColumns ++ includedColumns)
          .map(c => (c, ResolverUtils.resolve(spark, c, dfColumnNames)))
          .collect { case c if c._2.isEmpty => c._1 }
        throw HyperspaceException(
          s"Columns '${unresolvedColumns.mkString(",")}' could not be resolved " +
            s"from available source columns '${dfColumnNames.mkString(",")}'")
    }
  }

  private def prepareIndexDataFrame(
      spark: SparkSession,
      df: DataFrame,
      indexConfig: IndexConfig): (DataFrame, Seq[String], Seq[String]) = {
    val (resolvedIndexedColumns, resolvedIncludedColumns) = resolveConfig(df, indexConfig)
    val columnsFromIndexConfig = resolvedIndexedColumns ++ resolvedIncludedColumns

    val indexDF = if (hasLineage(spark)) {
      // Lineage is captured using two sets of columns:
      // 1. DATA_FILE_ID_COLUMN column contains source data file id for each index record.
      // 2. If source data is partitioned, all partitioning key(s) are added to index schema
      //    as columns if they are not already part of the schema.
      val missingPartitionColumns = getPartitionColumns(df).filter(
        ResolverUtils.resolve(spark, _, columnsFromIndexConfig).isEmpty)
      val allIndexColumns = columnsFromIndexConfig ++ missingPartitionColumns

      // File id value in DATA_FILE_ID_COLUMN column (lineage column) is stored as a
      // Long data type value. Each source data file has a unique file id, assigned by
      // Hyperspace. We populate lineage column by joining these file ids with index records.
      // The normalized path of source data file for each record is the join key.
      // We normalize paths by removing extra preceding `/` characters in them,
      // similar to the way they are stored in Content in an IndexLogEntry instance.
      // Path normalization example:
      // - Original raw path (output of input_file_name() udf, before normalization):
      //    + file:///C:/hyperspace/src/test/part-00003.snappy.parquet
      // - Normalized path (used in join):
      //    + file:/C:/hyperspace/src/test/part-00003.snappy.parquet
      import spark.implicits._
      val dataPathColumn = "_data_path"
      val lineagePairs = Hyperspace.getContext(spark).sourceProviderManager
          .lineagePairs(df.queryExecution.optimizedPlan, fileIdTracker)
      val lineageDF = lineagePairs.toDF(dataPathColumn, IndexConstants.DATA_FILE_NAME_ID)

      df.withColumn(dataPathColumn, input_file_name())
        .join(lineageDF.hint("broadcast"), dataPathColumn)
        .select(
          allIndexColumns.head,
          allIndexColumns.tail :+ IndexConstants.DATA_FILE_NAME_ID: _*)
    } else {
      df.select(columnsFromIndexConfig.head, columnsFromIndexConfig.tail: _*)
    }

    (indexDF, resolvedIndexedColumns, resolvedIncludedColumns)
  }

  private def getPartitionColumns(df: DataFrame): Seq[String] = {
    // Extract partition keys, if original data is partitioned.
    val partitionSchemas = df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(HadoopFsRelation(_, pSchema, _, _, _, _), _, _, _) => pSchema
      case DataSourceV2Relation(_, _, _, _, uSchema) => uSchema.getOrElse(StructType(Nil))
    }

    // Currently we only support creating an index on a single LogicalRelation.
    assert(partitionSchemas.length == 1)
    partitionSchemas.head.map(_.name)
  }
}
