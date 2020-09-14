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

package com.microsoft.hyperspace.actions

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.util.ResolverUtils

/**
 * Refresh index by removing index entries for deleted source data files.
 * If some original data file(s) are removed, in order to update index,
 * user can trigger an index refresh action during which:
 * 1. Deleted source data files are detected;
 * 2. Index records' lineage is leveraged to remove any index entry coming from deleted files.
 *
 * @param spark SparkSession
 * @param logManager Index LogManager for index being refreshed
 * @param dataManager Index DataManager for index being refreshed
 */
class RefreshDeleteAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager) {

  /**
   * For an index with lineage, find all the source data files which have been deleted,
   * and use index records' lineage to mark and remove index entries which belong to
   * deleted source data files as those entries are no longer valid.
   */
  final override def op(): Unit = {
    val indexDF = spark.read.parquet(previousIndexLogEntry.content.files.map(_.toString): _*)

    ResolverUtils
      .resolve(spark, IndexConstants.DATA_FILE_NAME_COLUMN, indexDF.schema.fieldNames) match {
      case Some(_) =>
        val refreshDF =
          indexDF.filter(
            !col(s"${IndexConstants.DATA_FILE_NAME_COLUMN}").isin(getDeletedFiles: _*))

        refreshDF.write.saveWithBuckets(
          refreshDF,
          indexDataPath.toString,
          logEntry.asInstanceOf[IndexLogEntry].numBuckets,
          indexConfig.indexedColumns)

      case None =>
        throw HyperspaceException(s"Refresh delete is only supported on an index with lineage.")
    }
  }

  /**
   * Compare list of source files from previous index log entry to list of current data files
   * and detect deleted source files.
   *
   * @return list of full paths of deleted source files.
   */
  private def getDeletedFiles: Seq[String] = {
    val rels = previousIndexLogEntry.relations
    // Currently we only support to create an index on a LogicalRelation.
    assert(rels.size == 1)

    val originalFiles = rels.head.data.properties.content.files.map(_.toString)
    val currentFiles = rels.head.rootPaths.flatMap { p =>
      Content
        .fromDirectory(path = new Path(p))
        .files
        .map(_.toString)
    }

    originalFiles diff currentFiles
  }
}
