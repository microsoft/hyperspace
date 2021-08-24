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

package com.microsoft.hyperspace.index.dataskipping

import org.apache.spark.sql.{Column, DataFrame, SaveMode}
import org.apache.spark.sql.functions.{input_file_name, min, spark_partition_id}
import org.apache.spark.sql.hyperspace.utils.StructTypeUtils
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.dataskipping.sketch.Sketch
import com.microsoft.hyperspace.index.dataskipping.util.{DataFrameUtils, ExpressionUtils}
import com.microsoft.hyperspace.util.HyperspaceConf

/**
 * DataSkippingIndex is an index that can accelerate queries by filtering out
 * files in relations using sketches.
 *
 * @param sketches List of sketches for this index
 * @param schema Index data schema
 * @param properties Properties for this index; see [[Index.properties]] for details.
 */
case class DataSkippingIndex(
    sketches: Seq[Sketch],
    schema: StructType,
    override val properties: Map[String, String] = Map.empty)
    extends Index {
  assert(sketches.nonEmpty, "At least one sketch is required.")

  override def kind: String = DataSkippingIndex.kind

  override def kindAbbr: String = DataSkippingIndex.kindAbbr

  override def indexedColumns: Seq[String] = sketches.flatMap(_.indexedColumns).distinct

  override def referencedColumns: Seq[String] = sketches.flatMap(_.referencedColumns).distinct

  override def withNewProperties(newProperties: Map[String, String]): DataSkippingIndex = {
    copy(properties = newProperties)
  }

  override def statistics(extended: Boolean = false): Map[String, String] = {
    Map("sketches" -> sketches.mkString(", "))
  }

  override def canHandleDeletedFiles: Boolean = true

  override def write(ctx: IndexerContext, indexData: DataFrame): Unit = {
    writeImpl(ctx, indexData, SaveMode.Overwrite)
  }

  override def optimize(ctx: IndexerContext, indexDataFilesToOptimize: Seq[FileInfo]): Unit = {
    val indexData =
      ctx.spark.read.schema(schema).parquet(indexDataFilesToOptimize.map(_.name): _*)
    writeImpl(ctx, indexData, SaveMode.Overwrite)
  }

  override def refreshIncremental(
      ctx: IndexerContext,
      appendedSourceData: Option[DataFrame],
      deletedSourceDataFiles: Seq[FileInfo],
      indexContent: Content): (Index, Index.UpdateMode) = {
    if (appendedSourceData.nonEmpty) {
      writeImpl(
        ctx,
        DataSkippingIndex.createIndexData(ctx, sketches, appendedSourceData.get),
        SaveMode.Overwrite)
    }
    if (deletedSourceDataFiles.nonEmpty) {
      val spark = ctx.spark
      import spark.implicits._
      val deletedFileIds = deletedSourceDataFiles.map(_.id).toDF(IndexConstants.DATA_FILE_NAME_ID)
      val updatedIndexData = spark.read
        .parquet(indexContent.files.map(_.toString): _*)
        .join(deletedFileIds, Seq(IndexConstants.DATA_FILE_NAME_ID), "left_anti")
      val writeMode = if (appendedSourceData.nonEmpty) {
        SaveMode.Append
      } else {
        SaveMode.Overwrite
      }
      writeImpl(ctx, updatedIndexData, writeMode)
    }
    val updateMode = if (deletedSourceDataFiles.isEmpty) {
      Index.UpdateMode.Merge
    } else {
      Index.UpdateMode.Overwrite
    }
    (this, updateMode)
  }

  override def refreshFull(
      ctx: IndexerContext,
      sourceData: DataFrame): (DataSkippingIndex, DataFrame) = {
    val resolvedSketches = ExpressionUtils.resolve(ctx.spark, sketches, sourceData)
    val indexData = DataSkippingIndex.createIndexData(ctx, resolvedSketches, sourceData)
    val updatedIndex = copy(sketches = resolvedSketches, schema = indexData.schema)
    (updatedIndex, indexData)
  }

  override def equals(that: Any): Boolean =
    that match {
      case DataSkippingIndex(thatSketches, thatSchema, _) =>
        sketches.toSet == thatSketches.toSet && schema == thatSchema
      case _ => false
    }

  override def hashCode: Int = sketches.map(_.hashCode).sum

  private def writeImpl(ctx: IndexerContext, indexData: DataFrame, writeMode: SaveMode): Unit = {
    // require instead of assert, as the condition can potentially be broken by
    // code which is external to dataskipping.
    require(
      indexData.schema.sameType(schema),
      "Schema of the index data doesn't match the index schema: " +
        s"index data schema = ${indexData.schema.toDDL}, index schema = ${schema.toDDL}")
    indexData.cache()
    indexData.count() // force cache
    val indexDataSize = DataFrameUtils.getSizeInBytes(indexData)
    val targetIndexDataFileSize = HyperspaceConf.DataSkipping.targetIndexDataFileSize(ctx.spark)
    val numFiles = indexDataSize / targetIndexDataFileSize
    if (!numFiles.isValidInt) {
      throw HyperspaceException(
        "Could not create index data files due to too many files: " +
          s"indexDataSize=$indexDataSize, targetIndexDataFileSize=$targetIndexDataFileSize")
    }
    val repartitionedIndexData = indexData.repartition(math.max(1, numFiles.toInt))
    repartitionedIndexData.write.mode(writeMode).parquet(ctx.indexDataPath.toString)
    indexData.unpersist()
  }
}

object DataSkippingIndex {
  // $COVERAGE-OFF$ https://github.com/scoverage/scalac-scoverage-plugin/issues/125
  final val kind = "DataSkippingIndex"
  final val kindAbbr = "DS"
  // $COVERAGE-ON$

  /**
   * Creates index data for the given source data.
   */
  def createIndexData(
      ctx: IndexerContext,
      sketches: Seq[Sketch],
      sourceData: DataFrame): DataFrame = {
    val fileNameCol = "input_file_name"
    val aggregateFunctions = getNamedAggregateFunctions(sketches)
    val indexDataWithFileName = sourceData
      .groupBy(input_file_name().as(fileNameCol))
      .agg(aggregateFunctions.head, aggregateFunctions.tail: _*)

    // Construct a dataframe to convert file names to file ids.
    val spark = ctx.spark
    val relation = RelationUtils.getRelation(spark, sourceData.queryExecution.optimizedPlan)
    import spark.implicits._
    val fileIdDf = ctx.fileIdTracker
      .getIdToFileMapping(relation.pathNormalizer)
      .toDF(IndexConstants.DATA_FILE_NAME_ID, fileNameCol)

    indexDataWithFileName
      .join(
        fileIdDf.hint("broadcast"),
        IndexUtils.decodeInputFileName(indexDataWithFileName(fileNameCol)) ===
          fileIdDf(fileNameCol))
      .select(
        IndexConstants.DATA_FILE_NAME_ID,
        indexDataWithFileName.columns.filterNot(_ == fileNameCol).map(c => s"`$c`"): _*)
  }

  def getNamedAggregateFunctions(sketches: Seq[Sketch]): Seq[Column] = {
    sketches.flatMap { s =>
      val aggrs = s.aggregateFunctions
      assert(aggrs.nonEmpty)
      aggrs.zipWithIndex.map {
        case (aggr, idx) =>
          new Column(aggr).as(getNormalizeColumnName(s"${s}_$idx"))
      }
    }
  }

  /**
   * Returns a normalized column name valid for a Parquet format.
   */
  private def getNormalizeColumnName(name: String): String = {
    name.replaceAll("[ ,;{}()\n\t=]", "_")
  }
}
