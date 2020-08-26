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

import scala.util.{Success, Try}

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.execution.datasources.{InMemoryFileIndex, PartitioningUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, REFRESHING}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, RefreshActionEvent}

class RefreshDeleteAction(
                           spark: SparkSession,
                           final override protected val logManager: IndexLogManager,
                           dataManager: IndexDataManager)
  extends CreateActionBase(dataManager)
    with Action {
  private lazy val previousLogEntry: LogEntry = {
    logManager.getLog(baseId).getOrElse {
      throw HyperspaceException("LogEntry must exist for refresh delete operation.")
    }
  }

  private lazy val previousIndexLogEntry = previousLogEntry.asInstanceOf[IndexLogEntry]

  private lazy val indexConfig: IndexConfig = {
    val ddColumns = previousIndexLogEntry.derivedDataset.properties.columns
    IndexConfig(previousIndexLogEntry.name, ddColumns.indexed, ddColumns.included)
  }

  // Reconstruct a df from schema
  private lazy val df = {
    val rels = previousIndexLogEntry.relations
    // Currently we only support to create an index on a LogicalRelation.
    assert(rels.size == 1)
    val dataSchema = DataType.fromJson(rels.head.dataSchemaJson).asInstanceOf[StructType]
    spark.read
      .schema(dataSchema)
      .format(rels.head.fileFormat)
      .options(rels.head.options)
      .load(rels.head.rootPaths: _*)
  }

  final override lazy val logEntry: LogEntry =
    getIndexLogEntry(spark, df, indexConfig, indexDataPath)

  final override def op(): Unit = {
    val diff = getDeletedFiles.map(FilenameUtils.getBaseName(_))
    if (diff.nonEmpty) {
      val indexDF = spark.read.parquet(previousIndexLogEntry.content.root)

      // import spark.implicits._
      indexDF.filter(col(s"${IndexConstants.DATA_FILE_NAME_COLUMN}").isin(diff: _*)).show()

      // PartitioningUtils.parsePathFragmentAsSeq()
    }
  }

  private def getDeletedFiles: Seq[String] = {
    val rels = previousIndexLogEntry.relations
    // Currently we only support to create an index on a LogicalRelation.
    assert(rels.size == 1)
    // pouriap fix this after recent checkins
    // rels.head.data.properties.content.directories.flatMap(_.files)
    val previousFiles = Seq[String]()

    //    val fs = PathResolver(spark.sessionState.conf).systemPath.getFileSystem(new Configuration)
    //    val currentFile = rels.head.rootPaths
    //      .flatMap(path => fs.listStatus(new Path(path)).map(_.getPath.toString))

    val currentFiles =
      new InMemoryFileIndex(spark, rels.head.rootPaths.map(new Path(_)), Map(), None)
        .allFiles()
        .map(_.getPath.toString)

    previousFiles diff currentFiles
  }

  //  private def getPartitionKeyValues(path: String): (String, String) = {
  //
  //  }

  final override val transientState: String = REFRESHING

  final override val finalState: String = ACTIVE

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    // TODO Change event type
    RefreshActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }

  // TODO Common code
  // private val fileName = udf((fullFilePath: String) => FilenameUtils.getBaseName(fullFilePath))
}
