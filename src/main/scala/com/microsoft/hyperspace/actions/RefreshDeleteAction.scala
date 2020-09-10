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

class RefreshDeleteAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager) {

  final override def op(): Unit = {
    val indexDF = spark.read.parquet(previousIndexLogEntry.content.files.map(_.toString): _*)

    ResolverUtils
      .resolve(spark, IndexConstants.DATA_FILE_NAME_COLUMN, indexDF.schema.fieldNames) match {
      case Some(_) =>
        val refreshDF =
          indexDF.filter(!col(s"${IndexConstants.DATA_FILE_NAME_COLUMN}").isin(getDeletedFiles: _*))

        refreshDF.write.saveWithBuckets(
          refreshDF,
          indexDataPath.toString,
          logEntry.asInstanceOf[IndexLogEntry].numBuckets,
          indexConfig.indexedColumns)

      case None =>
        throw HyperspaceException(s"Refresh delete is only supported on an index with lineage.")
    }
  }

  private def getDeletedFiles: Seq[String] = {
    val rels = previousIndexLogEntry.relations
    // Currently we only support to create an index on a LogicalRelation.
    assert(rels.size == 1)

    val originalFiles = rels.head.data.properties.content.files.map(_.toString)
    var currentFiles = Seq[String]()
    rels.head.rootPaths.foreach { p =>
      currentFiles ++= Content
        .fromDirectory(path = new Path(p))
        .files
        .map(_.toString)
    }

    originalFiles diff currentFiles
  }
}
