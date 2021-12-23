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

package com.microsoft.hyperspace.index.covering

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.functions.{col, input_file_name}
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.util.ResolverUtils
import com.microsoft.hyperspace.util.ResolverUtils.ResolvedColumn

/**
 * CoveringIndex data is stored as bucketed & sorted by indexedColumns so that it can substitute
 * for a shuffle node in a join query plan or a data scan node in a filter query plan.
 */
case class CoveringIndex(
    override val indexedColumns: Seq[String],
    override val includedColumns: Seq[String],
    override val schema: StructType,
    numBuckets: Int,
    override val properties: Map[String, String])
    extends CoveringIndexTrait {

  override def kind: String = CoveringIndex.kind

  override def kindAbbr: String = CoveringIndex.kindAbbr

  override def withNewProperties(newProperties: Map[String, String]): CoveringIndex = {
    copy(properties = newProperties)
  }

  override protected def copyIndex(
      indexedCols: Seq[String],
      includedCols: Seq[String],
      schema: StructType): CoveringIndex = {
    copy(indexedColumns = indexedCols, includedColumns = includedCols, schema = schema)
  }

  override protected def write(ctx: IndexerContext, indexData: DataFrame, mode: SaveMode): Unit = {
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
        mode)
  }

  override def equals(o: Any): Boolean =
    o match {
      case that: CoveringIndex => comparedData == that.comparedData
      case _ => false
    }

  override def hashCode: Int = {
    comparedData.hashCode
  }

  private def comparedData: (Seq[String], Seq[String], Int) = {
    (indexedColumns, includedColumns, numBuckets)
  }

  override def bucketSpec: Option[BucketSpec] =
    Some(
      BucketSpec(
        numBuckets = numBuckets,
        bucketColumnNames = indexedColumns,
        sortColumnNames = indexedColumns))

  override protected def simpleStatistics: Map[String, String] = {
    Map(
      "includedColumns" -> includedColumns.mkString(", "),
      "numBuckets" -> numBuckets.toString,
      "schema" -> schema.json)
  }

  override protected def extendedStatistics: Map[String, String] = {
    Map("hasLineage" -> hasLineageColumn.toString)
  }
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
   * case-sensitivity setting. Normalization is to support nested columns.
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
    val resolvedIndexedColumns = IndexUtils.resolveColumns(sourceData, indexedColumns)
    val resolvedIncludedColumns = IndexUtils.resolveColumns(sourceData, includedColumns)
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
        val lineagePairs = ctx.fileIdTracker.getIdToFileMapping(relation.pathNormalizer)
        val lineageDF = lineagePairs.toDF(IndexConstants.DATA_FILE_NAME_ID, dataPathColumn)

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
}
