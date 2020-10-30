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

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{input_file_name, udf}
import org.apache.spark.sql.sources.DataSourceRegister

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.util.{HyperspaceConf, PathUtils, ResolverUtils}

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

  protected def numBucketsForIndex(spark: SparkSession): Int = {
    HyperspaceConf.numBucketsForIndex(spark)
  }

  protected def indexLineageEnabled(spark: SparkSession): Boolean = {
    HyperspaceConf.indexLineageEnabled(spark)
  }

  protected def getIndexLogEntry(
      spark: SparkSession,
      df: DataFrame,
      indexConfig: IndexConfig,
      path: Path,
      fileIdsMap: Map[String, Long],
      lastFileId: Long): IndexLogEntry = {
    val absolutePath = PathUtils.makeAbsolute(path)
    val numBuckets = numBucketsForIndex(spark)

    val signatureProvider = LogicalPlanSignatureProvider.create()

    // Resolve the passed column names with existing column names from the dataframe.
    val (indexDataFrame, resolvedIndexedColumns, resolvedIncludedColumns) =
      prepareIndexDataFrame(spark, df, indexConfig, fileIdsMap)

    signatureProvider.signature(df.queryExecution.optimizedPlan) match {
      case Some(s) =>
        val relations = sourceRelations(df)
        // Currently we only support to create an index on a LogicalRelation.
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
                .Columns(resolvedIndexedColumns, resolvedIncludedColumns),
              IndexLogEntry.schemaString(indexDataFrame.schema),
              numBuckets)),
          Content.fromDirectory(absolutePath),
          Source(SparkPlan(sourcePlanProperties)),
          Map())
          .withFileIdsMap(lastFileId, fileIdsMap)

      case None => throw HyperspaceException("Invalid plan for creating an index.")
    }
  }

  protected def sourceRelations(df: DataFrame): Seq[Relation] =
    df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(
          HadoopFsRelation(
            location: PartitioningAwareFileIndex,
            _,
            dataSchema,
            _,
            fileFormat,
            options),
          _,
          _,
          _) =>
        val files = location.allFiles
        // Note that source files are currently fingerprinted when the optimized plan is
        // fingerprinted by LogicalPlanFingerprint.
        val sourceDataProperties = Hdfs.Properties(Content.fromLeafFiles(files).get)
        val fileFormatName = fileFormat match {
          case d: DataSourceRegister => d.shortName
          case other => throw HyperspaceException(s"Unsupported file format: $other")
        }
        // "path" key in options can incur multiple data read unexpectedly.
        val opts = options - "path"
        Relation(
          location.rootPaths.map(_.toString),
          Hdfs(sourceDataProperties),
          dataSchema.json,
          fileFormatName,
          opts)
    }

  protected def write(
      spark: SparkSession,
      df: DataFrame,
      indexConfig: IndexConfig,
      fileIdsMap: Map[String, Long]): Unit = {
    val numBuckets = numBucketsForIndex(spark)

    val (indexDataFrame, resolvedIndexedColumns, _) =
      prepareIndexDataFrame(spark, df, indexConfig, fileIdsMap)

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
      indexConfig: IndexConfig,
      fileIdsMap: Map[String, Long]): (DataFrame, Seq[String], Seq[String]) = {
    val (resolvedIndexedColumns, resolvedIncludedColumns) = resolveConfig(df, indexConfig)
    val columnsFromIndexConfig = resolvedIndexedColumns ++ resolvedIncludedColumns
    val addLineage = indexLineageEnabled(spark)

    val indexDF = if (addLineage) {
      // Lineage is captured using two sets of columns:
      // 1. DATA_FILE_ID_COLUMN column contains source data file id for each index record.
      // 2. If source data is partitioned, all partitioning key(s) are added to index schema
      //    as columns if they are not already part of the schema.
      val missingPartitionColumns = getPartitionColumns(df).filter(
        ResolverUtils.resolve(spark, _, columnsFromIndexConfig).isEmpty)
      val allIndexColumns = columnsFromIndexConfig ++ missingPartitionColumns

      // File id value in DATA_FILE_ID_COLUMN column is stored as Long.
      // We lookup the normalized path of the source data file for each record in
      // fileIdsMap (already generated by Hyperspace when listing source data files
      // for the index) and store the assigned id to the source data file as the lineage
      // for the records coming from that source data file.
      // - Original raw path (output of input_file_name() udf, before normalization):
      //    + file:///C:/hyperspace/src/test/part-00003.snappy.parquet
      // - Normalized path to be looked up in fileIdsMap:
      //    + file:/C:/hyperspace/src/test/part-00003.snappy.parquet
      val getFileId: UserDefinedFunction = udf((filePath: String) => {
        fileIdsMap
          .get(filePath.replace("file:///", "file:/")) match {
          case Some(id) => id
          case _ =>
            throw HyperspaceException(
              s"Encountered source data file with unknown file id. (File: $filePath).")
        }
      })

      df.select(allIndexColumns.head, allIndexColumns.tail: _*)
        .withColumn(IndexConstants.DATA_FILE_NAME_ID, getFileId(input_file_name()))
    } else {
      df.select(columnsFromIndexConfig.head, columnsFromIndexConfig.tail: _*)
    }

    (indexDF, resolvedIndexedColumns, resolvedIncludedColumns)
  }

  private def getPartitionColumns(df: DataFrame): Seq[String] = {
    // Extract partition keys, if original data is partitioned.
    val partitionSchemas = df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(HadoopFsRelation(_, pSchema, _, _, _, _), _, _, _) => pSchema
    }

    // Currently we only support creating an index on a single LogicalRelation.
    assert(partitionSchemas.length == 1)
    partitionSchemas.head.map(_.name)
  }

  /**
   * Generate a mapping for source data files by assigning a unique file id
   * to each source data file. Once assigned, this file id does not change
   * for a given file and is used to refer to that file.
   *
   * @param df DataFrame to index.
   * @return mapping of source data file paths to their file ids, and the
   *         last assigned file id.
   */
  protected def getFileIdsMap(df: DataFrame): (Map[String, Long], Long) = {
    var lastId = -1L
    val fileIdsMap = mutable.Map[String, Long]()
    df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        location.allFiles.foreach { f =>
          val filePath = f.getPath.toUri.toString.replace("file:///", "file:/")
          lastId += 1
          fileIdsMap.put(filePath, lastId)
        }
    }
    (fileIdsMap.toMap, lastId)
  }
}
