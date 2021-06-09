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

package com.microsoft.hyperspace.index

import org.apache.spark.sql.DataFrame

/**
 * IndexConfig specifies the configuration of an index.
 *
 * Index implementations should extend this trait and make the class visible
 * to users. Users can use the class when creating an index with the
 * [[com.microsoft.hyperspace.Hyperspace.createIndex()]] API.
 *
 * Implementation note: the name of this trait might be changed to
 * `IndexConfig` in v1.0.
 */
trait IndexConfigTrait {

  /**
   * Returns the name of the index.
   *
   * Index names are case-insensitive. An index name should not contain
   * disallowed characters for the file system, as index data is stored
   * in the file system with the index name in the path.
   */
  def indexName: String

  /**
   * Returns the column names which the index references.
   */
  def referencedColumns: Seq[String]

  /**
   * Creates an index from this configuration.
   *
   * @param ctx Helper object for indexing operations
   * @param sourceData Source data to index
   * @param properties Properties for [[Index]]
   * @return Created index and index data
   */
  def createIndex(
      ctx: IndexerContext,
      sourceData: DataFrame,
      properties: Map[String, String]): (Index, DataFrame)
}
