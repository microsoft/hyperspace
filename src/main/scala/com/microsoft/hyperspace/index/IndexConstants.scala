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
  val INDEXES_DIR = "indexes"

  // Constants related to Spark configuration.
  val INDEX_SYSTEM_PATH = "spark.hyperspace.system.path"
  val INDEX_CREATION_PATH = "spark.hyperspace.index.creation.path"
  val INDEX_SEARCH_PATHS = "spark.hyperspace.index.search.paths"
  val INDEX_NUM_BUCKETS = "spark.hyperspace.index.num.buckets"

  // This config enables Hybrid scan on mutable dataset at query time.
  // Currently, this config allows to perform Hybrid scan on append-only dataset.
  // For delete dataset, "spark.hyperspace.index.hybridscan.delete.enabled" is
  // also needed to be set.
  val INDEX_HYBRID_SCAN_ENABLED = "spark.hyperspace.index.hybridscan.enabled"
  val INDEX_HYBRID_SCAN_ENABLED_DEFAULT = "false"

  // This is a temporary config to support Hybrid scan on both append & delete dataset.
  // The config does not work without the Hybrid scan config
  // "spark.hyperspace.index.hybridscan.enabled"
  // and will be removed after performance validation and optimization.
  // See https://github.com/microsoft/hyperspace/issues/184
  val INDEX_HYBRID_SCAN_DELETE_ENABLED = "spark.hyperspace.index.hybridscan.delete.enabled"
  val INDEX_HYBRID_SCAN_DELETE_ENABLED_DEFAULT = "false"

  // Identifier injected to HadoopFsRelation as an option if an index is applied.
  // Currently, the identifier is added to options field of HadoopFsRelation.
  // In Spark 3.0, we could utilize TreeNodeTag to mark the identifier for each plan.
  // See https://github.com/microsoft/hyperspace/issues/185
  val INDEX_RELATION_IDENTIFIER: (String, String) = ("indexRelation" -> "true")

  // Default number of buckets is set the default value of "spark.sql.shuffle.partitions".
  val INDEX_NUM_BUCKETS_DEFAULT: Int = SQLConf.SHUFFLE_PARTITIONS.defaultValue.get

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

  private[hyperspace] val DATA_FILE_NAME_COLUMN = "_data_file_name"
  val INDEX_LINEAGE_ENABLED = "spark.hyperspace.index.lineage.enabled"
  val INDEX_LINEAGE_ENABLED_DEFAULT = "false"

  val REFRESH_DELETE_ENABLED = "spark.hyperspace.index.refresh.delete.enabled"
  val REFRESH_DELETE_ENABLED_DEFAULT = "false"

  /**
   * This flag enables refreshing index if additional data files are appended to the source. When
   * set to false, the refresh call will not run RefreshAppendAction. It will instead go for full
   * refresh.
   * This flag is temporary, and will be removed when both Append and Delete actions are merged
   * for refreshing indexes.
   */
  val REFRESH_APPEND_ENABLED = "spark.hyperspace.index.refresh.append.enabled"
  val REFRESH_APPEND_ENABLED_DEFAULT = "false"

  val REFRESH_MODE_INCREMENTAL = "incremental"
  val REFRESH_MODE_FULL = "full"
}
