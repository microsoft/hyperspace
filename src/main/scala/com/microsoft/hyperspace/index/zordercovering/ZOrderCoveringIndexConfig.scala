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

package com.microsoft.hyperspace.index.zordercovering

import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.covering.{CoveringIndex, CoveringIndexConfigTrait}
import com.microsoft.hyperspace.util.HyperspaceConf

/**
 * ZOrderCoveringIndexConfig specifies the configuration of a covering index which is z-ordered.
 *
 * Use this class to create a Z-ordered covering index with [[Hyperspace.createIndex()]].
 *
 * @param indexName Index name.
 * @param indexedColumns Columns to be used for calculating z-address.
 * @param includedColumns Columns to be included in the index.
 */
case class ZOrderCoveringIndexConfig(
    override val indexName: String,
    override val indexedColumns: Seq[String],
    override val includedColumns: Seq[String] = Seq())
    extends CoveringIndexConfigTrait {
  override def createIndex(
      ctx: IndexerContext,
      sourceData: DataFrame,
      properties: Map[String, String]): (ZOrderCoveringIndex, DataFrame) = {
    val (indexData, resolvedIndexedColumns, resolvedIncludedColumns) =
      CoveringIndex.createIndexData(
        ctx,
        sourceData,
        indexedColumns,
        includedColumns,
        IndexUtils.hasLineageColumn(properties))

    val index = ZOrderCoveringIndex(
      resolvedIndexedColumns.map(_.normalizedName),
      resolvedIncludedColumns.map(_.normalizedName),
      indexData.schema,
      HyperspaceConf.ZOrderCovering.targetSourceBytesPerPartition(ctx.spark),
      properties)
    (index, indexData)
  }
}
