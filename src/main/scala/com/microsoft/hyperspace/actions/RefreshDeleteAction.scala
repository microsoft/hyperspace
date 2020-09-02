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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, REFRESHING}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, RefreshActionEvent}

class RefreshDeleteAction(
    spark: SparkSession,
    final override protected val logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends CreateActionBase(dataManager)
    with Action {
  private lazy val previousLogEntry: LogEntry = {
    logManager.getLog(baseId).getOrElse {
      throw HyperspaceException("LogEntry must exist for refresh operation.")
    }
  }

  private lazy val previousIndexLogEntry = previousLogEntry.asInstanceOf[IndexLogEntry]

  private lazy val indexConfig: IndexConfig = {
    val ddColumns = previousIndexLogEntry.derivedDataset.properties.columns
    IndexConfig(previousIndexLogEntry.name, ddColumns.indexed, ddColumns.included)
  }

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
    val indexDF = spark.read.parquet(previousIndexLogEntry.content.root)
    val refreshDF =
      indexDF.filter(!col(s"${IndexConstants.DATA_FILE_NAME_COLUMN}").isin(getDeletedFiles: _*))

    refreshDF.write.saveWithBuckets(
      refreshDF,
      indexDataPath.toString,
      logEntry.asInstanceOf[IndexLogEntry].numBuckets,
      indexConfig.indexedColumns)
  }

  final override val transientState: String = REFRESHING

  final override val finalState: String = ACTIVE

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    RefreshActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }

  private def getDeletedFiles: Seq[String] = {
    val rels = previousIndexLogEntry.relations
    // Currently we only support to create an index on a LogicalRelation.
    assert(rels.size == 1)

    val originalFiles =
      rels.head.data.properties.content.directories.flatMap(_.files).map(_.name)

    var currentFiles = Seq[String]()
    rels.head.rootPaths.foreach { r =>
      val path = new Path(r)
      val fs = path.getFileSystem(new Configuration)
      currentFiles ++= listLeafFiles(path, fs)
    }

    originalFiles diff currentFiles
  }

  private def listLeafFiles(path: Path, fs: FileSystem): Seq[String] = {
    val (files, directories) = fs.listStatus(path).partition(_.isFile)
    files.map(_.getPath.toString) ++
      directories.flatMap(d => listLeafFiles(d.getPath, fs))
  }
}
