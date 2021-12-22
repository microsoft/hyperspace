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

import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.covering.CoveringIndexTrait
import com.microsoft.hyperspace.util.HyperspaceConf

/**
 * ZOrderCoveringIndex data is stored as sorted by z-address based on the values of indexedColumns
 * so that it can substitute a data scan node in a filter query plan.
 */
case class ZOrderCoveringIndex(
    override val indexedColumns: Seq[String],
    override val includedColumns: Seq[String],
    override val schema: StructType,
    targetBytesPerPartition: Long,
    override val properties: Map[String, String])
    extends CoveringIndexTrait {

  override def bucketSpec: Option[BucketSpec] = None

  override def kind: String = ZOrderCoveringIndex.kind

  override def kindAbbr: String = ZOrderCoveringIndex.kindAbbr

  override def withNewProperties(newProperties: Map[String, String]): ZOrderCoveringIndex = {
    copy(properties = newProperties)
  }

  private def collectStats(df: DataFrame, zOrderByCols: Seq[String]): Map[String, Any] = {
    val isQuantileEnabled =
      HyperspaceConf.ZOrderCovering.quantileBasedZAddressEnabled(df.sparkSession)
    val (percentileBasedCols, minMaxBasedCols) = if (!isQuantileEnabled) {
      (Nil, zOrderByCols)
    } else {
      zOrderByCols.partition(name => ZOrderField.percentileApplicableType(df(name).expr.dataType))
    }

    val getMinMax: Seq[String] => Map[String, Any] = cols => {
      if (cols.nonEmpty) {
        val minMaxAgg = cols.flatMap { name =>
          min(df(name)).as(s"min($name)") :: max(df(name)).as(s"max($name)") :: Nil
        }
        val aggDf = df.agg(minMaxAgg.head, minMaxAgg.tail: _*)
        aggDf.head.getValuesMap(aggDf.schema.fieldNames)
      } else {
        Map.empty
      }
    }

    val getQuantiles: Seq[String] => Map[String, Any] = cols => {
      val relativeError =
        HyperspaceConf.ZOrderCovering.quantileBasedZAddressRelativeError(df.sparkSession)

      // Sample list always contains min/max elements, so we could get min/max in double
      // with 0.0 and 1.0 probabilities and < 1.0 relativeError.
      val res = df.stat
        .approxQuantile(cols.toArray, Array(0.0, 0.25, 0.50, 0.75, 1.0), relativeError)
        .toSeq
      res.zipWithIndex.flatMap {
        case (arr, idx) =>
          val name = cols(idx)
          val keys =
            Seq(s"min($name)", s"ap_25($name)", s"ap_50($name)", s"ap_75($name)", s"max($name)")
          keys.zip(arr).toMap
      }.toMap
    }

    // Using par to submit both jobs concurrently.
    val resMaps = Seq((percentileBasedCols, getQuantiles), (minMaxBasedCols, getMinMax)).par.map {
      case (cols, f) =>
        f(cols)
    }.toIndexedSeq
    resMaps.head ++ resMaps.last
  }

  override protected def write(
      ctx: IndexerContext,
      indexData: DataFrame,
      mode: SaveMode): Unit = {
    val relation = RelationUtils.getRelation(ctx.spark, indexData.queryExecution.optimizedPlan)
    val numPartitions = (relation.allFileSizeInBytes / targetBytesPerPartition).toInt.max(2)

    if (indexedColumns.size == 1) {
      val repartitionedIndexDataFrame = {
        indexData
          .repartitionByRange(numPartitions, col(indexedColumns.head))
          .sortWithinPartitions(indexedColumns.head)
      }
      repartitionedIndexDataFrame.write
        .format("parquet")
        .mode(mode)
        .save(ctx.indexDataPath.toString)
    } else {
      // Get min/max of indexed columns and total number of record.

      val isQuantileEnabled =
        HyperspaceConf.ZOrderCovering.quantileBasedZAddressEnabled(indexData.sparkSession)
      val stats = collectStats(indexData, indexedColumns)
      val zOrderFields = indexedColumns.map { name =>
        val min = stats(s"min($name)")
        val max = stats(s"max($name)")
        val percentile =
          if (stats.contains(s"ap_25($name)")) {
            Seq(stats(s"ap_25($name)"), stats(s"ap_50($name)"), stats(s"ap_75($name)"))
          } else {
            Nil
          }
        ZOrderField.build(
          name,
          indexData(name).expr.dataType,
          min,
          max,
          percentile,
          isQuantileEnabled)
      }

      val zOrderUdf = ZOrderUDF(zOrderFields)
      val repartitionedIndexDataFrame = {
        indexData
          .withColumn(
            "_zaddr",
            zOrderUdf.zAddressUdf(struct(indexedColumns.map(indexData(_)): _*)))
          .repartitionByRange(numPartitions, col("_zaddr"))
          .sortWithinPartitions("_zaddr")
          .drop("_zaddr")
      }

      repartitionedIndexDataFrame.write
        .format("parquet")
        .mode(mode)
        .save(ctx.indexDataPath.toString)
    }
  }

  override protected def copyIndex(
      indexedCols: Seq[String],
      includedCols: Seq[String],
      schema: StructType): ZOrderCoveringIndex = {
    copy(indexedColumns = indexedCols, includedColumns = includedCols, schema = schema)
  }

  override def equals(o: Any): Boolean =
    o match {
      case that: ZOrderCoveringIndex => comparedData == that.comparedData
      case _ => false
    }

  override def hashCode: Int = {
    comparedData.hashCode
  }

  private def comparedData: (Seq[String], Seq[String]) = {
    (indexedColumns, includedColumns)
  }

  override protected def simpleStatistics: Map[String, String] = {
    Map("includedColumns" -> includedColumns.mkString(", "), "schema" -> schema.json)
  }

  override protected def extendedStatistics: Map[String, String] = {
    Map("hasLineage" -> hasLineageColumn.toString)
  }
}

object ZOrderCoveringIndex {
  final val kind = "ZOrderCoveringIndex"
  final val kindAbbr = "ZCI"
}
