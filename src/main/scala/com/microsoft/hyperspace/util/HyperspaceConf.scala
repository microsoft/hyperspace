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

package com.microsoft.hyperspace.util

import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.IndexConstants

/**
 * Helper class to extract Hyperspace-related configs from SparkSession.
 */
object HyperspaceConf {

  /**
   * Returns the config value whether hyperspace is enabled or not.
   */
  def hyperspaceApplyEnabled(spark: SparkSession): Boolean = {
    spark.conf
      .get(
        IndexConstants.HYPERSPACE_APPLY_ENABLED,
        IndexConstants.HYPERSPACE_APPLY_ENABLED_DEFAULT)
      .toBoolean
  }

  def setHyperspaceApplyEnabled(spark: SparkSession, apply: Boolean): Unit = {
    spark.conf.set(IndexConstants.HYPERSPACE_APPLY_ENABLED, apply.toString)
  }

  def hybridScanEnabled(spark: SparkSession): Boolean = {
    spark.conf
      .get(
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED,
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED_DEFAULT)
      .toBoolean
  }

  def hybridScanDeleteEnabled(spark: SparkSession): Boolean = {
    hybridScanDeletedRatioThreshold(spark) > 0.0
  }

  def optimizeFileSizeThreshold(spark: SparkSession): Long = {
    spark.conf
      .get(
        IndexConstants.OPTIMIZE_FILE_SIZE_THRESHOLD,
        IndexConstants.OPTIMIZE_FILE_SIZE_THRESHOLD_DEFAULT.toString)
      .toLong
  }

  def hybridScanDeletedRatioThreshold(spark: SparkSession): Double = {
    spark.conf
      .get(
        IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD,
        IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD_DEFAULT)
      .toDouble
  }

  def hybridScanAppendedRatioThreshold(spark: SparkSession): Double = {
    spark.conf
      .get(
        IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD,
        IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD_DEFAULT)
      .toDouble
  }

  def useBucketSpecForFilterRule(spark: SparkSession): Boolean = {
    spark.conf
      .get(
        IndexConstants.INDEX_FILTER_RULE_USE_BUCKET_SPEC,
        IndexConstants.INDEX_FILTER_RULE_USE_BUCKET_SPEC_DEFAULT)
      .toBoolean
  }

  def numBucketsForIndex(spark: SparkSession): Int = {
    getConfStringWithMultipleKeys(
      spark,
      Seq(IndexConstants.INDEX_NUM_BUCKETS, IndexConstants.INDEX_NUM_BUCKETS_LEGACY),
      IndexConstants.INDEX_NUM_BUCKETS_DEFAULT.toString).toInt
  }

  def indexLineageEnabled(spark: SparkSession): Boolean = {
    spark.sessionState.conf
      .getConfString(
        IndexConstants.INDEX_LINEAGE_ENABLED,
        IndexConstants.INDEX_LINEAGE_ENABLED_DEFAULT)
      .toBoolean
  }

  def fileBasedSourceBuilders(spark: SparkSession): String = {
    spark.sessionState.conf
      .getConfString(
        "spark.hyperspace.index.sources.fileBasedBuilders",
        "com.microsoft.hyperspace.index.sources.default.DefaultFileBasedSourceBuilder")
  }

  def supportedFileFormatsForDefaultFileBasedSource(spark: SparkSession): String = {
    spark.sessionState.conf
      .getConfString(
        "spark.hyperspace.index.sources.defaultFileBasedSource.supportedFileFormats",
        "avro,csv,json,orc,parquet,text")
  }

  def nestedColumnEnabled(spark: SparkSession): Boolean = {
    spark.conf
      .get(
        IndexConstants.DEV_NESTED_COLUMN_ENABLED,
        IndexConstants.DEV_NESTED_COLUMN_ENABLED_DEFAULT)
      .toBoolean
  }

  object ZOrderCovering {
    def targetSourceBytesPerPartition(spark: SparkSession): Long = {
      spark.conf
        .get(
          IndexConstants.INDEX_ZORDER_TARGET_SOURCE_BYTES_PER_PARTITION,
          IndexConstants.INDEX_ZORDER_TARGET_SOURCE_BYTES_PER_PARTITION_DEFAULT)
        .toLong
    }

    def quantileBasedZAddressEnabled(spark: SparkSession): Boolean = {
      spark.conf
        .get(
          IndexConstants.INDEX_ZORDER_QUANTILE_ENABLED,
          IndexConstants.INDEX_ZORDER_QUANTILE_ENABLED_DEFAULT)
        .toBoolean
    }

    def quantileBasedZAddressRelativeError(spark: SparkSession): Double = {
      spark.conf
        .get(
          IndexConstants.INDEX_ZORDER_QUANTILE_RELATIVE_ERROR,
          IndexConstants.INDEX_ZORDER_QUANTILE_RELATIVE_ERROR_DEFAULT)
        .toDouble
    }
  }

  object DataSkipping {
    def targetIndexDataFileSize(spark: SparkSession): Long = {
      // TODO: Consider using a systematic way to validate the config value
      // like Spark's ConfigBuilder
      val value = spark.conf
        .get(
          IndexConstants.DATASKIPPING_TARGET_INDEX_DATA_FILE_SIZE,
          IndexConstants.DATASKIPPING_TARGET_INDEX_DATA_FILE_SIZE_DEFAULT)
      val longValue =
        try {
          value.toLong
        } catch {
          case e: NumberFormatException =>
            throw HyperspaceException(
              s"${IndexConstants.DATASKIPPING_TARGET_INDEX_DATA_FILE_SIZE} " +
                s"should be long, but was $value")
        }
      if (longValue <= 0) {
        throw HyperspaceException(
          s"${IndexConstants.DATASKIPPING_TARGET_INDEX_DATA_FILE_SIZE} " +
            s"should be a positive number.")
      }
      longValue
    }

    def maxIndexDataFileCount(spark: SparkSession): Int = {
      // TODO: Consider using a systematic way to validate the config value
      // like Spark's ConfigBuilder
      val value = spark.conf
        .get(
          IndexConstants.DATASKIPPING_MAX_INDEX_DATA_FILE_COUNT,
          IndexConstants.DATASKIPPING_MAX_INDEX_DATA_FILE_COUNT_DEFAULT)
      val intValue =
        try {
          value.toInt
        } catch {
          case e: NumberFormatException =>
            throw HyperspaceException(
              s"${IndexConstants.DATASKIPPING_MAX_INDEX_DATA_FILE_COUNT} " +
                s"should be int, but was $value")
        }
      if (intValue <= 0) {
        throw HyperspaceException(
          s"${IndexConstants.DATASKIPPING_MAX_INDEX_DATA_FILE_COUNT} " +
            s"should be a positive number.")
      }
      intValue
    }

    def autoPartitionSketch(spark: SparkSession): Boolean = {
      // TODO: Consider using a systematic way to validate the config value
      // like Spark's ConfigBuilder
      val value = spark.conf
        .get(
          IndexConstants.DATASKIPPING_AUTO_PARTITION_SKETCH,
          IndexConstants.DATASKIPPING_AUTO_PARTITION_SKETCH_DEFAULT)
      val booleanValue =
        try {
          value.toBoolean
        } catch {
          case e: IllegalArgumentException =>
            throw HyperspaceException(
              s"${IndexConstants.DATASKIPPING_AUTO_PARTITION_SKETCH} " +
                s"should be boolean, but was $value")
        }
      booleanValue
    }
  }

  /**
   * Returns the config value whose key matches the first key given multiple keys. If no keys are
   * matched, the given default value is returned.
   *
   * @param spark Spark session.
   * @param keys Config keys to look up.
   * @param defaultValue Default value to fall back if no config keys are matched.
   * @return Config value found. 'default' if no config value is found for the given keys.
   */
  private def getConfStringWithMultipleKeys(
      spark: SparkSession,
      keys: Seq[String],
      defaultValue: String): String = {
    keys
      .find(spark.sessionState.conf.contains)
      .map(spark.sessionState.conf.getConfString)
      .getOrElse(defaultValue)
  }
}
