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
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hyperspace.utils.StructTypeUtils
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.util.ResolverUtils.ResolvedColumn

/**
 * CoveringIndexTrait is a common interface for covering index type which contains a vertical
 * slice of source data including indexedColumns and includedColumns.
 */
trait CoveringIndexTrait extends Index {
  def schema: StructType
  def includedColumns: Seq[String]
  def bucketSpec: Option[BucketSpec]
  protected def write(ctx: IndexerContext, indexData: DataFrame, mode: SaveMode)
  protected def copyIndex(
      indexedCols: Seq[String],
      includedCols: Seq[String],
      schema: StructType): CoveringIndexTrait
  protected def simpleStatistics: Map[String, String]
  protected def extendedStatistics: Map[String, String]

  // override functions for Index
  override def write(ctx: IndexerContext, indexData: DataFrame): Unit = {
    write(ctx, indexData, SaveMode.Overwrite)
  }

  override def referencedColumns: Seq[String] = indexedColumns ++ includedColumns

  override def statistics(extended: Boolean = false): Map[String, String] = {
    simpleStatistics ++ (if (extended) extendedStatistics else Map.empty)
  }

  override def canHandleDeletedFiles: Boolean = hasLineageColumn

  override def refreshIncremental(
      ctx: IndexerContext,
      appendedSourceData: Option[DataFrame],
      deletedSourceDataFiles: Seq[FileInfo],
      indexContent: Content): (CoveringIndexTrait, Index.UpdateMode) = {
    val updatedIndex = if (appendedSourceData.nonEmpty) {
      val (indexData, resolvedIndexedColumns, resolvedIncludedColumns) =
        CoveringIndex.createIndexData(
          ctx,
          appendedSourceData.get,
          indexedColumns.map(ResolvedColumn(_).name),
          includedColumns.map(ResolvedColumn(_).name),
          hasLineageColumn)
      write(ctx, indexData)
      copyIndex(
        indexedCols = resolvedIndexedColumns.map(_.normalizedName),
        includedCols = resolvedIncludedColumns.map(_.normalizedName),
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

      write(ctx, refreshDF, writeMode)
    }

    // If there is no deleted files, there are index data files only for appended data in this
    // version and we need to add the index data files of previous index version.
    // Otherwise, as previous index data is rewritten in this version while excluding
    // indexed rows from deleted files, all necessary index data files exist in this version.
    val updatedMode = if (deletedSourceDataFiles.isEmpty) {
      Index.UpdateMode.Merge
    } else {
      Index.UpdateMode.Overwrite
    }
    (updatedIndex, updatedMode)
  }

  override def refreshFull(
      ctx: IndexerContext,
      sourceData: DataFrame): (CoveringIndexTrait, DataFrame) = {
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
      copyIndex(
        indexedCols = resolvedIndexedColumns.map(_.normalizedName),
        includedCols = resolvedIncludedColumns.map(_.normalizedName),
        schema = indexData.schema),
      indexData)
  }

  def hasLineageColumn: Boolean = IndexUtils.hasLineageColumn(properties)

  override def optimize(ctx: IndexerContext, indexDataFilesToOptimize: Seq[FileInfo]): Unit = {
    // Rewrite index using the eligible files to optimize.
    val indexData = ctx.spark.read.parquet(indexDataFilesToOptimize.map(_.name): _*)
    write(ctx, indexData)
  }
}
