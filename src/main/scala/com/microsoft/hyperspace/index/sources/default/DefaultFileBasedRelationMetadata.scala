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

package com.microsoft.hyperspace.index.sources.default

import com.microsoft.hyperspace.index.Relation
import com.microsoft.hyperspace.index.sources.FileBasedRelationMetadata

/**
 * Default file-based relation metadata implementation for file-based Spark built-in sources
 */
class DefaultFileBasedRelationMetadata(metadata: Relation) extends FileBasedRelationMetadata {

  override def refresh(): Relation = {
    // No change is needed because rootPaths will be pointing to the latest source files.
    metadata
  }

  override def internalFileFormatName(): String = {
    metadata.fileFormat
  }

  override def enrichIndexProperties(properties: Map[String, String]): Map[String, String] = {
    properties
  }

  override def canSupportUserSpecifiedSchema: Boolean = true
}
