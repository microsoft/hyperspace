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

package com.microsoft.hyperspace.index

import com.fasterxml.jackson.annotation.{JsonGetter, JsonRawValue}
import com.fasterxml.jackson.databind.annotation.JsonDeserialize
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.hyperspace.utils.StructTypeUtils
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.shim.StructTypeConverter
import com.microsoft.hyperspace.util.ResolverUtils
import com.microsoft.hyperspace.util.ResolverUtils.ResolvedColumn

case class CoveringIndex(
    override val indexedColumns: Seq[String],
    includedColumns: Seq[String],
    // See schemaJson for more information about the annotation.
    @JsonDeserialize(converter = classOf[StructTypeConverter.T]) schema: StructType,
    numBuckets: Int,
    override val properties: Map[String, String])
    extends Index {

  override def kind: String = CoveringIndex.kind

  override def kindAbbr: String = CoveringIndex.kindAbbr

  override def referencedColumns: Seq[String] = indexedColumns ++ includedColumns

  override def withNewProperties(newProperties: Map[String, String]): CoveringIndex = {
    copy(properties = newProperties)
  }

  override def statistics(extended: Boolean = false): Map[String, String] = {
    simpleStatistics ++ (if (extended) extendedStatistics else Map.empty)
  }

  override def canHandleDeletedFiles: Boolean = hasLineageColumn

  override def write(ctx: IndexerContext, indexData: DataFrame): Unit = {
    // Run job
    val repartitionedIndexData = {
      // We are repartitioning with normalized columns (e.g., flattened nested column).
      indexData.repartition(numBuckets, indexedColumns.map(name => col(s"`$name`")): _*)
    }

    // Save the index with the number of buckets specified.
    repartitionedIndexData.write
      .saveWithBuckets(
        repartitionedIndexData,
        ctx.indexDataPath.toString,
        numBuckets,
        indexedColumns,
        SaveMode.Overwrite)
  }

  override def optimize(ctx: IndexerContext, indexDataFilesToOptimize: Seq[FileInfo]): Unit = {
    // Rewrite index using the eligible files to optimize.
    val indexData = ctx.spark.read.parquet(indexDataFilesToOptimize.map(_.name): _*)
    val repartitionedIndexData =
      indexData.repartition(numBuckets, indexedColumns.map(indexData(_)): _*)
    repartitionedIndexData.write.saveWithBuckets(
      repartitionedIndexData,
      ctx.indexDataPath.toString,
      numBuckets,
      indexedColumns,
      SaveMode.Overwrite)
  }

  override def refreshIncremental(
      ctx: IndexerContext,
      appendedSourceData: Option[DataFrame],
      deletedSourceDataFiles: Seq[FileInfo],
      indexContent: Content): CoveringIndex = {
    val updatedIndex = if (appendedSourceData.nonEmpty) {
      val (indexData, resolvedIndexedColumns, resolvedIncludedColumns) =
        CoveringIndex.createIndexData(
          ctx,
          appendedSourceData.get,
          indexedColumns.map(ResolvedColumn(_).name),
          includedColumns.map(ResolvedColumn(_).name),
          hasLineageColumn)
      write(ctx, indexData)
      copy(
        indexedColumns = resolvedIndexedColumns.map(_.normalizedName),
        includedColumns = resolvedIncludedColumns.map(_.normalizedName),
        schema = schema.merge(indexData.schema))
    } else {
      this
    }
    if (deletedSourceDataFiles.nonEmpty) {
      // For an index with lineage, find all the source data files which have been deleted,
      // and use index records' lineage to mark and remove index entries which belong to
      // deleted source data files as those entries are no longer valid.
      val refreshDF = ctx.spark.read
        .parquet(indexContent.files.map(_.toString): _*)
        .filter(!col(IndexConstants.DATA_FILE_NAME_ID).isin(deletedSourceDataFiles.map(_.id): _*))

      // Write refreshed data using Append mode if there are index data files from appended files.
      val writeMode = if (appendedSourceData.nonEmpty) {
        SaveMode.Append
      } else {
        SaveMode.Overwrite
      }
      refreshDF.write.saveWithBuckets(
        refreshDF,
        ctx.indexDataPath.toString,
        numBuckets,
        indexedColumns,
        writeMode)
    }
    updatedIndex
  }

  override def refreshFull(
      ctx: IndexerContext,
      sourceData: DataFrame): (CoveringIndex, DataFrame) = {
    val (indexData, resolvedIndexedColumns, resolvedIncludedColumns) =
      CoveringIndex.createIndexData(
        ctx,
        sourceData,
        // As indexed & included columns in previousLogEntry are resolved & prefixed names,
        // need to remove the prefix to resolve with the dataframe for refresh.
        indexedColumns.map(ResolvedColumn(_).name),
        includedColumns.map(ResolvedColumn(_).name),
        hasLineageColumn)
    (
      copy(
        indexedColumns = resolvedIndexedColumns.map(_.normalizedName),
        includedColumns = resolvedIncludedColumns.map(_.normalizedName),
        schema = indexData.schema),
      indexData)
  }

  override def equals(o: Any): Boolean = o match {
    case that: CoveringIndex => comparedData == that.comparedData
    case _ => false
  }

  override def hashCode: Int = {
    comparedData.hashCode
  }

  private def comparedData: (Seq[String], Seq[String], Int) = {
    (indexedColumns, includedColumns, numBuckets)
  }

  def bucketSpec: BucketSpec =
    BucketSpec(
      numBuckets = numBuckets,
      bucketColumnNames = indexedColumns,
      sortColumnNames = indexedColumns)

  def hasLineageColumn: Boolean = IndexUtils.hasLineageColumn(properties)

  private def simpleStatistics: Map[String, String] = {
    Map(
      "includedColumns" -> includedColumns.mkString(", "),
      "numBuckets" -> numBuckets.toString,
      "schema" -> schema.json)
  }

  private def extendedStatistics: Map[String, String] = {
    Map("hasLineage" -> hasLineageColumn.toString)
  }

  // Shim for Spark 2.4 - StructType doesn't work well with Jackson by default.
  // These annotations are used to make json de/serialization work in Spark 2.4.
  @JsonRawValue
  @JsonGetter("schema")
  private def schemaJson: String = schema.json
}

object CoveringIndex {
  final val kind = "CoveringIndex"
  final val kindAbbr = "CI"

  /**
   * Creates index data from the given source data.
   *
   * Roughly speaking, index data for [[CoveringIndex]] is just a vertical
   * slice of the source data, including only the indexed and included columns,
   * bucketed and sorted by the indexed columns for efficient access.
   *
   * If hasLineageColumn is true, then the index data will contain a
   * lineage column representing the source file each row came from, as well
   * as the indexed and included columns.
   *
   * This method returns resolved and normalized indexed/included columns
   * in addition to the index data. Resolved means that exact column names are
   * retrieved from the schema. For example, when Spark is configured as
   * case-insensitive and a column name "Foo.BaR" is given, and the schema is
   * "root |-- foo |-- bar: integer", a resolved column name would be "foo.bar".
   * This step is necessary to make the index work regardless of the current
   * case-sensitivity setting. Normalization is to supporte nested columns.
   * Nested columns in the source data is stored as a normal column in the
   * index data.
   *
   * @param ctx Helper object for indexing operations
   * @param sourceData Source data to index
   * @param indexedColumns Column names to be indexed
   * @param includedColumns Column names to be included
   * @param hasLineageColumn Whether or not to include the lineage column in
   *                         the index data
   * @return Tuple of (index data, resolved indexed columns, resolved included
   *         columns)
   */
  def createIndexData(
      ctx: IndexerContext,
      sourceData: DataFrame,
      indexedColumns: Seq[String],
      includedColumns: Seq[String],
      hasLineageColumn: Boolean): (DataFrame, Seq[ResolvedColumn], Seq[ResolvedColumn]) = {
    val spark = ctx.spark
    val (resolvedIndexedColumns, resolvedIncludedColumns) =
      resolveConfig(sourceData, indexedColumns, includedColumns)
    val projectColumns = (resolvedIndexedColumns ++ resolvedIncludedColumns).map(_.toColumn)

    val indexData =
      if (hasLineageColumn) {
        val relation = RelationUtils.getRelation(spark, sourceData.queryExecution.optimizedPlan)

        // Lineage is captured using two sets of columns:
        // 1. DATA_FILE_ID_COLUMN column contains source data file id for each index record.
        // 2. If source data is partitioned, all partitioning key(s) are added to index schema
        //    as columns if they are not already part of the schema.
        val partitionColumnNames = relation.partitionSchema.map(_.name)
        val resolvedColumnNames = (resolvedIndexedColumns ++ resolvedIncludedColumns).map(_.name)
        val missingPartitionColumns =
          partitionColumnNames
            .filter(ResolverUtils.resolve(spark, _, resolvedColumnNames).isEmpty)
            .map(col)

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
        val lineagePairs = relation.lineagePairs(ctx.fileIdTracker)
        val lineageDF = lineagePairs.toDF(dataPathColumn, IndexConstants.DATA_FILE_NAME_ID)

        sourceData
          .withColumn(dataPathColumn, input_file_name())
          .join(lineageDF.hint("broadcast"), dataPathColumn)
          .select(projectColumns ++ missingPartitionColumns :+ col(
            IndexConstants.DATA_FILE_NAME_ID): _*)
      } else {
        sourceData.select(projectColumns: _*)
      }

    (indexData, resolvedIndexedColumns, resolvedIncludedColumns)
  }

  private def resolveConfig(
      df: DataFrame,
      indexedColumns: Seq[String],
      includedColumns: Seq[String]): (Seq[ResolvedColumn], Seq[ResolvedColumn]) = {
    val spark = df.sparkSession
    val plan = df.queryExecution.analyzed
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
}
