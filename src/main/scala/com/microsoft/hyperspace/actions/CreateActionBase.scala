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
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.functions.{col, input_file_name}

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.util.{HyperspaceConf, PathUtils, ResolverUtils}
import com.microsoft.hyperspace.util.ResolverUtils.ResolvedColumn

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

  protected def prevIndexProperties: Map[String, String] = {
    // Return empty map for index creation - no previous properties.
    Map()
  }

  protected def getIndexLogEntry(
      spark: SparkSession,
      df: DataFrame,
      indexConfig: IndexConfig,
      path: Path,
      versionId: Int): IndexLogEntry = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val absolutePath = PathUtils.makeAbsolute(path, hadoopConf)
    val numBuckets = numBucketsForIndex(spark)

    val signatureProvider = LogicalPlanSignatureProvider.create()

    // Resolve the passed column names with existing column names from the dataframe.
    val (indexDataFrame, resolvedIndexedColumns, resolvedIncludedColumns) =
      prepareIndexDataFrame(spark, df, indexConfig)

    signatureProvider.signature(df.queryExecution.optimizedPlan) match {
      case Some(s) =>
        val relation = getRelation(spark, df)
        val sourcePlanProperties = SparkPlan.Properties(
          Seq(relation.createRelationMetadata(fileIdTracker)),
          null,
          null,
          LogicalPlanFingerprint(
            LogicalPlanFingerprint.Properties(Seq(Signature(signatureProvider.name, s)))))

        val coveringIndexProperties =
          Hyperspace
            .getContext(spark)
            .sourceProviderManager
            .enrichIndexProperties(
              sourcePlanProperties.relations.head,
              prevIndexProperties + (IndexConstants.INDEX_LOG_VERSION -> versionId.toString)
                ++ hasLineageProperty(spark) ++ hasParquetAsSourceFormatProperty(relation))

        IndexLogEntry(
          indexConfig.indexName,
          CoveringIndex(
            CoveringIndex.Properties(
              CoveringIndex.Properties
                .Columns(
                  resolvedIndexedColumns.map(_.normalizedName),
                  resolvedIncludedColumns.map(_.normalizedName)),
              IndexLogEntry.schemaString(indexDataFrame.schema),
              numBuckets,
              coveringIndexProperties)),
          Content.fromDirectory(absolutePath, fileIdTracker, hadoopConf),
          Source(SparkPlan(sourcePlanProperties)),
          Map())

      case None => throw HyperspaceException("Invalid plan for creating an index.")
    }
  }

  protected def write(spark: SparkSession, df: DataFrame, indexConfig: IndexConfig): Unit = {
    val numBuckets = numBucketsForIndex(spark)

    val (indexDataFrame, resolvedIndexedColumns, _) =
      prepareIndexDataFrame(spark, df, indexConfig)

    // Run job
    val repartitionedIndexDataFrame = {
      // We are repartitioning with normalized columns (e.g., flattened nested column).
      indexDataFrame.repartition(numBuckets, resolvedIndexedColumns.map(_.toNormalizedColumn): _*)
    }

    // Save the index with the number of buckets specified.
    repartitionedIndexDataFrame.write
      .saveWithBuckets(
        repartitionedIndexDataFrame,
        indexDataPath.toString,
        numBuckets,
        resolvedIndexedColumns.map(_.normalizedName),
        SaveMode.Overwrite)
  }

  protected def getRelation(spark: SparkSession, df: DataFrame): FileBasedRelation = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    val relations = df.queryExecution.optimizedPlan.collect {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        provider.getRelation(l)
    }
    // Currently we only support creating an index on a single relation.
    assert(relations.length == 1)
    relations.head
  }

  private def hasParquetAsSourceFormatProperty(
      relation: FileBasedRelation): Option[(String, String)] = {
    if (relation.hasParquetAsSourceFormat) {
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

  private def resolveConfig(
      df: DataFrame,
      indexConfig: IndexConfig): (Seq[ResolvedColumn], Seq[ResolvedColumn]) = {
    val spark = df.sparkSession
    val plan = df.queryExecution.analyzed
    val indexedColumns = indexConfig.indexedColumns
    val includedColumns = indexConfig.includedColumns
    val resolvedIndexedColumns = ResolverUtils.resolve(spark, indexedColumns, plan)
    val resolvedIncludedColumns = ResolverUtils.resolve(spark, includedColumns, plan)

    (resolvedIndexedColumns, resolvedIncludedColumns) match {
      case (Some(indexed), Some(included)) => (indexed, included)
      case _ =>
        val unresolvedColumns = (indexedColumns ++ includedColumns)
          .map(c => (c, ResolverUtils.resolve(spark, Seq(c), plan).map(_.map(_.name))))
          .collect { case (c, r) if r.isEmpty => c }
        throw HyperspaceException(
          s"Columns '${unresolvedColumns.mkString(",")}' could not be resolved " +
            s"from available source columns:\n${df.schema.treeString}")
    }
  }

  private def prepareIndexDataFrame(
      spark: SparkSession,
      df: DataFrame,
      indexConfig: IndexConfig): (DataFrame, Seq[ResolvedColumn], Seq[ResolvedColumn]) = {
    val (resolvedIndexedColumns, resolvedIncludedColumns) = resolveConfig(df, indexConfig)
    val projectColumns = (resolvedIndexedColumns ++ resolvedIncludedColumns).map(_.toColumn)

    val indexDF = if (hasLineage(spark)) {
      val relation = getRelation(spark, df)

      // Lineage is captured using two sets of columns:
      // 1. DATA_FILE_ID_COLUMN column contains source data file id for each index record.
      // 2. If source data is partitioned, all partitioning key(s) are added to index schema
      //    as columns if they are not already part of the schema.
      val partitionColumnNames = relation.partitionSchema.map(_.name)
      val resolvedColumnNames = (resolvedIndexedColumns ++ resolvedIncludedColumns).map(_.name)
      val missingPartitionColumns =
        partitionColumnNames
          .filter(ResolverUtils.resolve(spark, _, resolvedColumnNames).isEmpty)
          .map(col(_))

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
      val lineagePairs = relation.lineagePairs(fileIdTracker)
      val lineageDF = lineagePairs.toDF(dataPathColumn, IndexConstants.DATA_FILE_NAME_ID)

      df.withColumn(dataPathColumn, input_file_name())
        .join(lineageDF.hint("broadcast"), dataPathColumn)
        .select(
          projectColumns ++ missingPartitionColumns :+ col(IndexConstants.DATA_FILE_NAME_ID): _*)
    } else {
      df.select(projectColumns: _*)
    }

    (indexDF, resolvedIndexedColumns, resolvedIncludedColumns)
  }
}
