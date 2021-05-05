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

package com.microsoft.hyperspace.index.sources.iceberg

import com.microsoft.hyperspace.index.Relation
import com.microsoft.hyperspace.index.sources.FileBasedRelationMetadata

/**
 * Implementation for file-based relation metadata used by [[IcebergFileBasedSource]]
 */
class IcebergRelationMetadata(metadata: Relation) extends FileBasedRelationMetadata {

  override def refresh(): Relation = {
    metadata.copy(options = metadata.options - "snapshot-id" - "as-of-timestamp")
  }

  override def internalFileFormatName(): String = {
    "parquet"
  }

  override def enrichIndexProperties(properties: Map[String, String]): Map[String, String] = {
    properties
  }

  override def canSupportUserSpecifiedSchema: Boolean = false
}
