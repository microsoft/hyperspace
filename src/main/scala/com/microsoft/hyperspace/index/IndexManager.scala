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

import org.apache.spark.sql.DataFrame

/**
 * Interface that contains internal APIs to manage index.
 */
trait IndexManager {

  /**
   * Lists index objects on default index storage location and additional index path with
   * option to filter by index status.
   *
   * @return all the metadata for indexes as DataFrame.
   */
  def indexes: DataFrame

  /**
   * Create index.
   *
   * @param df the DataFrame object to build index on.
   * @param indexConfig the configuration of index to be created.
   * @return Index object that stores information about index created.
   */
  def create(df: DataFrame, indexConfig: IndexConfig): Unit

  /**
   * Soft deletes the index with given index name.
   */
  def delete(indexName: String): Unit

  /**
   * Restores index with given index name if marked for DELETE. If an index is 'delete'd, it
   * can be restored. If it has been 'vacuum'ed, it can't be restored.
   *
   * @param indexName Name of the index.
   */
  def restore(indexName: String): Unit

  /**
   * Does hard delete of indexes marked as `DELETED`. Once vacuumed, an index can't be 'restore'd.
   *
   * @param indexName Name of the index to vacuum.
   */
  def vacuum(indexName: String): Unit

  /**
   * Update indexes for the latest version of the data.
   *
   * @param indexName Name of the index to refresh.
   */
  def refresh(indexName: String, mode: String): Unit

  /**
   * Optimize index by changing the underlying index data layout (e.g., compaction).
   *
   * @param indexName Name of the index to optimize.
   * @param mode Optimize mode. "quick" mode refers to optimizing only small index files. "full"
   *             mode represents optimizing all index files.
   */
  def optimize(indexName: String, mode: String): Unit

  /**
   * Cancel api to bring back index from an inconsistent state to the last known stable state.
   * E.g. if operation fails during creation, thus in "CREATING" state, the index will not allow
   * any operations unless it is cancelled.
   *
   * Note: Cancelling "VACUUMING" state will move it forward to "DOESNOTEXIST" state.
   * Note: If no previous stable state exists, cancel will move it to "DOESNOTEXIST" state.
   *
   * @param indexName Name of the index to cancel.
   */
  def cancel(indexName: String): Unit

  /**
   * Get index objects on default index storage location and additional index path that matches
   * any of given states.
   *
   * @param states list of index states of interest
   * @return all indexes that match any of the given states
   */
  def getIndexes(states: Seq[String] = Seq()): Seq[IndexLogEntry]
}
