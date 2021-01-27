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

import org.apache.hadoop.fs.Path

/**
 * Case class representing index metadata and index statistics from latest index version.
 *
 * @param name Index name.
 * @param indexedColumns Indexed columns.
 * @param includedColumns Included columns.
 * @param numBuckets Number of buckets.
 * @param schema Index schema json.
 * @param indexLocation Path to parent directory containing index files for all versions.
 * @param state Index state.
 * @param kind Index kind.
 * @param hasLineage Lineage enabled on index.
 * @param numIndexFiles Total number of index files.
 * @param sizeIndexFiles Total size of index files.
 * @param numSourceFiles Total number of source data files.
 * @param sizeSourceFiles Total size of source data files.
 * @param numAppendedFiles Total number of appended source data files.
 * @param sizeAppendedFiles Total size of appended source data files.
 * @param numDeletedFiles Total number of deleted source data files.
 * @param sizeDeletedFiles Total size of deleted source data files.
 * @param indexContentPaths Path(s) to directories containing index files for latest version.
 */
private[hyperspace] case class IndexStatistics(
    name: String,
    indexedColumns: Seq[String],
    includedColumns: Seq[String],
    numBuckets: Int,
    schema: String,
    indexLocation: String,
    state: String,
    kind: String,
    hasLineage: Boolean,
    numIndexFiles: Int,
    sizeIndexFiles: Long,
    numSourceFiles: Int,
    sizeSourceFiles: Long,
    numAppendedFiles: Int,
    sizeAppendedFiles: Long,
    numDeletedFiles: Int,
    sizeDeletedFiles: Long,
    indexContentPaths: Seq[String])

private[hyperspace] object IndexStatistics {
  val INDEX_SUMMARY_COLUMNS: Seq[String] = Seq(
    "name",
    "indexedColumns",
    "includedColumns",
    "numBuckets",
    "schema",
    "indexLocation",
    "state")

  /**
   * Create IndexStatistics instance for a given IndexLogEntry.
   *
   * @param entry IndexLogEntry instance.
   * @param extended If true, all IndexStatistics fields are included;
   *                Otherwise only [[INDEX_SUMMARY_COLUMNS]] fields.
   * @return IndexStatistics instance generated from entry.
   */
  def apply(entry: IndexLogEntry, extended: Boolean = false): IndexStatistics = {
    if (extended) {
      IndexStatistics(
        entry.name,
        entry.derivedDataset.properties.columns.indexed,
        entry.derivedDataset.properties.columns.included,
        entry.numBuckets,
        entry.derivedDataset.properties.schemaString,
        indexDirPath(entry),
        entry.state,
        entry.derivedDataset.kind,
        entry.hasLineageColumn,
        entry.content.fileInfos.size,
        entry.content.fileInfos.foldLeft(0L)(_ + _.size),
        entry.sourceFileInfoSet.size,
        entry.sourceFileInfoSet.foldLeft(0L)(_ + _.size),
        entry.appendedFiles.size,
        entry.appendedFiles.foldLeft(0L)(_ + _.size),
        entry.deletedFiles.size,
        entry.deletedFiles.foldLeft(0L)(_ + _.size),
        getIndexContentDirectoryPaths(entry))
    } else {
      IndexStatistics(
        entry.name,
        entry.derivedDataset.properties.columns.indexed,
        entry.derivedDataset.properties.columns.included,
        entry.numBuckets,
        entry.derivedDataset.properties.schemaString,
        indexDirPath(entry),
        entry.state)
    }
  }

  /**
   * Create an IndexStatistics instance without extended members.
   *
   * @param name Index name.
   * @param indexedColumns Indexed columns list.
   * @param includedColumns Included columns list.
   * @param buckets Number of buckets.
   * @param schema Index schema.
   * @param location Index files location.
   * @param state Index state.
   * @return IndexStatistics instance created from given values.
   */
  def apply(
      name: String,
      indexedColumns: Seq[String],
      includedColumns: Seq[String],
      buckets: Int,
      schema: String,
      location: String,
      state: String): IndexStatistics = {
    new IndexStatistics(
      name,
      indexedColumns,
      includedColumns,
      buckets,
      schema,
      location,
      state,
      "",
      false,
      0,
      0L,
      0,
      0L,
      0,
      0L,
      0,
      0L,
      Nil)
  }

  /**
   * Extract top-most index directories which contain existing index files for
   * the latest version of index. When refreshing index, depending on the mode,
   * index files for the latest version of index may reside in multiple directories.
   * This function extracts paths to top-level directories which
   * contain those index files.
   *
   * @param entry Index log entry.
   * @return List of directory paths containing index files for latest index version.
   */
  private def getIndexContentDirectoryPaths(entry: IndexLogEntry): Seq[String] = {
    var root = entry.content.root
    var prefix = entry.content.root.name
    while (root.subDirs.size == 1 &&
           !root.subDirs.head.name.startsWith(IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX)) {
      prefix += s"${root.subDirs.head.name}/"
      root = root.subDirs.head
    }

    root.subDirs.map(d => s"$prefix${d.name}")
  }

  /**
   * This method extracts the most top-level (or top-most) index directory which
   * has either
   * - at least one leaf file, or
   * - more than one subdirectories, or
   * - no files and no subdirectories (this case will not happen for real index scenarios).
   *
   * @param entry Index log entry.
   * @return Path to the first leaf directory starting from the root.
   */
  private def indexDirPath(entry: IndexLogEntry): String = {
    var root = entry.content.root
    var indexDirPath = new Path(entry.content.root.name)
    while (root.files.isEmpty && root.subDirs.size == 1) {
      root = root.subDirs.head
      indexDirPath = new Path(indexDirPath, root.name)
    }
    indexDirPath.toString
  }
}
