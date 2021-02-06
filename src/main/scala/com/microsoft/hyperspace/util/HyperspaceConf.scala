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

import com.microsoft.hyperspace.index.IndexConstants

/**
 * Helper class to extract Hyperspace-related configs from SparkSession.
 */
object HyperspaceConf {
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
