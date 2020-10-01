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
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, OptimizeActionEvent}
import com.microsoft.hyperspace.util.HyperspaceConf

class OptimizeAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager,
    mode: String)
    extends RefreshActionBase(spark, logManager, dataManager) {

  final override def op(): Unit = {
    // Rewrite index from small files
    val numBuckets = previousIndexLogEntry.numBuckets
    val indexDF = spark.read.parquet(smallFiles.map(_.name): _*)

    val repartitionedDf =
      indexDF.repartition(numBuckets, indexConfig.indexedColumns.map(indexDF(_)): _*)

    repartitionedDf.write.saveWithBuckets(
      repartitionedDf,
      indexDataPath.toString,
      logEntry.asInstanceOf[IndexLogEntry].numBuckets,
      indexConfig.indexedColumns)
  }

  private lazy val (smallFiles, largeFiles): (Seq[FileInfo], Seq[FileInfo]) = {
    if (mode.equals("quick")) {
      val threshold = HyperspaceConf.optimizeThreshold(spark)
      previousIndexLogEntry.content.fileInfos.toSeq.partition(_.size < threshold)
    } else {
      (previousIndexLogEntry.content.fileInfos.toSeq, Seq())
    }
  }

  override def logEntry: LogEntry = {
    // 1. Create an  entry with full data and newly created index files
    // 2. Create a Directory object using just largeFiles
    // 3. Merge them

    val entry = getIndexLogEntry(spark, df, indexConfig, indexDataPath)
    if (largeFiles.nonEmpty) {
      val largeFilesDirectory: Directory = {
        val fs = new Path(previousIndexLogEntry.content.fileInfos.head.name)
          .getFileSystem(new Configuration)
        val largeFileStatuses =
          largeFiles.map(fileInfo => fs.getFileStatus(new Path(fileInfo.name)))

        Directory.fromLeafFiles(largeFileStatuses)
      }
      val mergedContent = Content(entry.content.root.merge(largeFilesDirectory))
      entry.copy(content = mergedContent)
    } else {
      entry
    }
  }

  override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    OptimizeActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
