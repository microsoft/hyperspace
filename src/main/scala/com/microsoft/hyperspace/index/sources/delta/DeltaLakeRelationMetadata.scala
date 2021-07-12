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

package com.microsoft.hyperspace.index.sources.delta

import com.microsoft.hyperspace.index.{IndexConstants, Relation}
import com.microsoft.hyperspace.index.sources.FileBasedRelationMetadata

/**
 * Implementation for file-based relation metadata used by [[DeltaLakeFileBasedSource]]
 */
class DeltaLakeRelationMetadata(metadata: Relation) extends FileBasedRelationMetadata {

  override def refresh(): Relation = {
    metadata.copy(options = metadata.options - "versionAsOf" - "timestampAsOf")
  }

  override def internalFileFormatName(): String = {
    "parquet"
  }

  /**
   * Returns enriched index properties.
   *
   * Delta Lake source provider adds:
   * 1) DELTA_VERSION_HISTORY_PROPERTY logs the history of INDEX_VERSION:DELTA_TABLE_VERSION
   *    values for each index creation & refresh.
   *
   * @param properties Index properties to enrich.
   * @return Updated index properties for index creation or refresh.
   */
  override def enrichIndexProperties(properties: Map[String, String]): Map[String, String] = {
    val indexVersion = properties(IndexConstants.INDEX_LOG_VERSION)
    val deltaVerHistory = metadata.options.get("versionAsOf").map { deltaVersion =>
      val newVersionMapping = s"$indexVersion:$deltaVersion"
      DeltaLakeConstants.DELTA_VERSION_HISTORY_PROPERTY ->
        properties
          .get(DeltaLakeConstants.DELTA_VERSION_HISTORY_PROPERTY)
          .map { prop =>
            s"$prop,$newVersionMapping"
          }
          .getOrElse(newVersionMapping)
    }
    properties ++ deltaVerHistory
  }

  /**
   * Remove DELTA_VERSION_HISTORY_PROPERTY from properties.
   *
   * @param properties Index properties to reset.
   * @return Updated index properties for vacuum outdated data.
   */
  def resetDeltaVersionHistory(properties: Map[String, String]): Map[String, String] = {
    properties - DeltaLakeConstants.DELTA_VERSION_HISTORY_PROPERTY
  }

  override def canSupportUserSpecifiedSchema: Boolean = false
}
