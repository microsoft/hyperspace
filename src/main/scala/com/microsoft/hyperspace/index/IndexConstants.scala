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

package com.microsoft.hyperspace.index

import org.apache.spark.sql.internal.SQLConf

object IndexConstants {
  // If it is set as false, Hyperspace will not be applied.
  val HYPERSPACE_APPLY_ENABLED = "spark.hyperspace.apply.enabled"
  val HYPERSPACE_APPLY_ENABLED_DEFAULT = "true"

  val INDEXES_DIR = "indexes"

  // Config used for setting the system path, which is considered as a "root" path for Hyperspace;
  // e.g, indexes are created under the system path.
  val INDEX_SYSTEM_PATH = "spark.hyperspace.system.path"

  // Config used to set the number of buckets for the index.
  val INDEX_NUM_BUCKETS_LEGACY = "spark.hyperspace.index.num.buckets"
  val INDEX_NUM_BUCKETS = "spark.hyperspace.index.numBuckets"
  // Default number of buckets is set the default value of "spark.sql.shuffle.partitions".
  val INDEX_NUM_BUCKETS_DEFAULT: Int = SQLConf.SHUFFLE_PARTITIONS.defaultValue.get

  // This config enables Hybrid scan on mutable dataset at query time.
  val INDEX_HYBRID_SCAN_ENABLED = "spark.hyperspace.index.hybridscan.enabled"
  val INDEX_HYBRID_SCAN_ENABLED_DEFAULT = "false"

  // If the ratio of deleted files to all source files of a candidate index is greater than this
  // threshold, the index won't be applied even with Hybrid Scan.
  val INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD =
    "spark.hyperspace.index.hybridscan.maxDeletedRatio"
  val INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD_DEFAULT = "0.2"

  // If the ratio of newly appended files to all source files in the given relation is greater than
  // this threshold, the index won't be applied even with Hybrid Scan.
  val INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD =
    "spark.hyperspace.index.hybridscan.maxAppendedRatio"
  val INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD_DEFAULT = "0.3"

  // Config used to set bucketSpec for Filter rule. If bucketSpec is used, Spark can prune
  // not applicable buckets, so it can read less files in case of a highly selective query.
  val INDEX_FILTER_RULE_USE_BUCKET_SPEC = "spark.hyperspace.index.filterRule.useBucketSpec"
  val INDEX_FILTER_RULE_USE_BUCKET_SPEC_DEFAULT = "false"

  // Config to determine max file size for ZOrderCoveringIndex.
  // It's an approximate value as it's based on the summation of size of source files
  // which can vary on file format and compression rates.
  val INDEX_ZORDER_TARGET_SOURCE_BYTES_PER_PARTITION =
    "spark.hyperspace.index.zorder.targetSourceBytesPerPartition"
  val INDEX_ZORDER_TARGET_SOURCE_BYTES_PER_PARTITION_DEFAULT = "1073741824" // 1G

  // If enabled, Z-address will be calculated using percentile number for numeric column types
  // instead of actual column value. It can mitigate skewed data issue. It is disabled by default
  // as collecting quantiles takes longer than getting only min/max values.
  val INDEX_ZORDER_QUANTILE_ENABLED = "spark.hyperspace.index.zorder.quantile.enabled"
  val INDEX_ZORDER_QUANTILE_ENABLED_DEFAULT = "false"

  // relativeError value when collecting approximate quantiles for numeric columns.
  val INDEX_ZORDER_QUANTILE_RELATIVE_ERROR = "spark.hyperspace.index.zorder.quantile.relativeError"
  val INDEX_ZORDER_QUANTILE_RELATIVE_ERROR_DEFAULT = "0.01"

  // TODO: Remove dev config when nested column is fully supported.
  val DEV_NESTED_COLUMN_ENABLED = "spark.hyperspace.dev.index.nestedColumn.enabled"
  val DEV_NESTED_COLUMN_ENABLED_DEFAULT = "false"

  // Identifier injected to HadoopFsRelation as an option if an index is applied.
  // Currently, the identifier is added to options field of HadoopFsRelation.
  // In Spark 3.0, we could utilize TreeNodeTag to mark the identifier for each plan.
  // See https://github.com/microsoft/hyperspace/issues/185
  val INDEX_RELATION_IDENTIFIER: (String, String) = "indexRelation" -> "true"

  val INDEX_CACHE_EXPIRY_DURATION_SECONDS =
    "spark.hyperspace.index.cache.expiryDurationInSeconds"
  val INDEX_CACHE_EXPIRY_DURATION_SECONDS_DEFAULT = "300"

  // Operation Log constants
  val HYPERSPACE_LOG = "_hyperspace_log"
  val INDEX_VERSION_DIRECTORY_PREFIX = "v__"

  // Constants for display mode of explain API.
  val DISPLAY_MODE = "spark.hyperspace.explain.displayMode"
  val HIGHLIGHT_BEGIN_TAG = "spark.hyperspace.explain.displayMode.highlight.beginTag"
  val HIGHLIGHT_END_TAG = "spark.hyperspace.explain.displayMode.highlight.endTag"
  object DisplayMode {
    val CONSOLE = "console"
    val PLAIN_TEXT = "plaintext"
    val HTML = "html"
  }

  private[hyperspace] val DATA_FILE_NAME_ID = "_data_file_id"
  val INDEX_LINEAGE_ENABLED = "spark.hyperspace.index.lineage.enabled"
  val INDEX_LINEAGE_ENABLED_DEFAULT = "false"

  val REFRESH_MODE_INCREMENTAL = "incremental"
  val REFRESH_MODE_FULL = "full"
  val REFRESH_MODE_QUICK = "quick"

  /**
   * Optimize threshold. It is a threshold of size of index files in bytes. Files with size
   * below this threshold are eligible for index optimization.
   */
  val OPTIMIZE_FILE_SIZE_THRESHOLD = "spark.hyperspace.index.optimize.fileSizeThreshold"
  val OPTIMIZE_FILE_SIZE_THRESHOLD_DEFAULT = 256 * 1024 * 1024L // 256MB
  val OPTIMIZE_MODE_QUICK = "quick"
  val OPTIMIZE_MODE_FULL = "full"
  val OPTIMIZE_MODES = Seq(OPTIMIZE_MODE_QUICK, OPTIMIZE_MODE_FULL)

  // Default id used for a file which does not have an id or its id is not known.
  private[hyperspace] val UNKNOWN_FILE_ID: Long = -1L

  // JSON property names used in index metadata.
  // Indicate whether lineage is enabled for an index.
  private[hyperspace] val LINEAGE_PROPERTY = "lineage"
  // Indicate whether the source file format is parquet.
  private[hyperspace] val HAS_PARQUET_AS_SOURCE_FORMAT_PROPERTY = "hasParquetAsSourceFormat"
  // Indicate Hyperspace version.
  private[hyperspace] val HYPERSPACE_VERSION_PROPERTY: String = "hyperspaceVersion"
  // Indicate index log version.
  private[hyperspace] val INDEX_LOG_VERSION = "indexLogVersion"

  // Hyperspace allows users to use globbing patterns to create indexes on. E.g. if user wants to
  // create an index on "/temp/*/*", they can do so by setting this key to "/temp/*/*". If not set,
  // Hyperspace assumes the actual folder names were picked for indexing instead of the wildcards.
  // To provide multiple paths in the globbing pattern, separate them with commas, e.g.
  // "/temp/1/*, /temp/2/*"
  val GLOBBING_PATTERN_KEY = "spark.hyperspace.source.globbingPattern"

  /**
   * Target size for an index data file for data skipping indexes.
   *
   * Data skipping index application starts with filtering the index data with
   * a translated predicate. Usually it's I/O bound and it's important to have
   * index data distributed uniformly over files in terms of data size.
   */
  val DATASKIPPING_TARGET_INDEX_DATA_FILE_SIZE =
    "spark.hyperspace.index.dataskipping.targetIndexDataFileSize"
  val DATASKIPPING_TARGET_INDEX_DATA_FILE_SIZE_DEFAULT = "268435456" // 256 MiB

  /**
   * Maximum number of index data files.
   *
   * The number of index data files determined by targetIndexFileSize is
   * capped by this value.
   */
  val DATASKIPPING_MAX_INDEX_DATA_FILE_COUNT =
    "spark.hyperspace.index.dataskipping.maxIndexDataFileCount"
  val DATASKIPPING_MAX_INDEX_DATA_FILE_COUNT_DEFAULT = "10000"

  /**
   * If set to true, partition sketches for partition columns are included when
   * creating data skipping indexes. This does not affect existing indexes.
   */
  val DATASKIPPING_AUTO_PARTITION_SKETCH =
    "spark.hyperspace.index.dataskipping.autoPartitionSketch"
  val DATASKIPPING_AUTO_PARTITION_SKETCH_DEFAULT = "true"
}
