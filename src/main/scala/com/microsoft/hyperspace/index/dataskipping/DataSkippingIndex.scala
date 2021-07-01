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

package com.microsoft.hyperspace.index.types.dataskipping

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.functions.{col, input_file_name}

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.types.dataskipping.sketch.Sketch

/**
 * DataSkippingIndex is an index that can accelerate queries by filtering out
 * files in relations using sketches.
 *
 * @param sketches List of sketches for this index
 * @param properties Properties for this index; see [[Index.properties]] for details.
 */
case class DataSkippingIndex(
    sketches: Seq[Sketch],
    override val properties: Map[String, String] = Map.empty)
    extends Index {
  assert(sketches.nonEmpty, "At least one sketch is required.")

  /**
   * Sketch offsets are used to map each sketch to its corresponding columns
   * in the dataframe.
   */
  lazy val sketchOffsets: Seq[Int] = sketches.map(_.numValues).scanLeft(0)(_ + _)

  override def kind: String = DataSkippingIndex.kind

  override def kindAbbr: String = DataSkippingIndex.kindAbbr

  override def indexedColumns: Seq[String] = sketches.flatMap(_.indexedColumns)

  override def referencedColumns: Seq[String] = sketches.flatMap(_.referencedColumns)

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
    val indexData = ctx.spark.read.parquet(indexDataFilesToOptimize.map(_.name): _*)
    write(ctx, indexData)
  }

  override def refreshIncremental(
      ctx: IndexerContext,
      appendedSourceData: Option[DataFrame],
      deletedSourceDataFiles: Seq[FileInfo],
      indexContent: Content): (Index, Index.UpdateMode) = {
    if (appendedSourceData.nonEmpty) {
      write(ctx, index(ctx, appendedSourceData.get))
    }
    if (deletedSourceDataFiles.nonEmpty) {
      val spark = ctx.spark
      import spark.implicits._
      val deletedFileIds = deletedSourceDataFiles.map(_.id).toDF(IndexConstants.DATA_FILE_NAME_ID)
      val updatedIndexData = ctx.spark.read
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
    val updatedIndex = copy(sketches = DataSkippingIndex.resolveSketch(sketches, sourceData))
    (updatedIndex, updatedIndex.index(ctx, sourceData))
  }

  /**
   * Creates index data for the given source data.
   */
  def index(ctx: IndexerContext, sourceData: DataFrame): DataFrame = {
    def getColumnName(c: Column): String =
      c.expr match {
        case expr: NamedExpression => expr.name
        case _ => c.toString
      }
    val aggregateFunctions = sketches.flatMap { s =>
      val aggrs = s.aggregateFunctions
      assert(aggrs.nonEmpty && aggrs.length == s.numValues)
      aggrs.map(aggr => aggr.as(DataSkippingIndex.normalizeColumnName(getColumnName(aggr))))
    }
    val fileNameCol = "input_file_name"
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

    // Drop the file name column and reorder the columns
    // so that the file id column comes first.
    indexDataWithFileName
      .join(fileIdDf.hint("broadcast"), fileNameCol)
      .select(IndexConstants.DATA_FILE_NAME_ID, indexDataWithFileName.columns: _*)
      .drop(fileNameCol)
  }

  private def writeImpl(ctx: IndexerContext, indexData: DataFrame, writeMode: SaveMode): Unit = {
    indexData.write.mode(writeMode).parquet(ctx.indexDataPath.toString)
  }

  override def equals(that: Any): Boolean =
    that match {
      case DataSkippingIndex(thatSketches, _) => sketches.toSet == thatSketches.toSet
      case _ => false
    }

  override def hashCode: Int = sketches.hashCode
}

object DataSkippingIndex {
  final val kind = "DataSkippingIndex"
  final val kindAbbr = "DS"

  /**
   * Returns a copy of the given sketches with the indexed columns replaced by
   * resolved column names.
   */
  def resolveSketch(sketches: Seq[Sketch], sourceData: DataFrame): Seq[Sketch] = {
    sketches.map { s =>
      val oldColumns = s.referencedColumns
      val newColumns = IndexUtils.resolveColumns(sourceData, oldColumns).map(_.name)
      assert(oldColumns.length == newColumns.length)
      s.withNewColumns(oldColumns.zip(newColumns).toMap)
    }
  }

  /**
   * Returns a normalized column name valid for a Parquet format.
   */
  def normalizeColumnName(name: String): String = {
    name.replaceAll("[ ,;{}()\n\t=]", "_")
  }
}
