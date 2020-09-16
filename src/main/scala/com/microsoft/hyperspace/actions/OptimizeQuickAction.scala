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
// scalastyle:off
package com.microsoft.hyperspace.actions

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.index._

class OptimizeQuickAction(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager) {

  final override def op(): Unit = {
    val indexDF = spark.read.parquet(smallFiles.map(_.getPath.toString): _*)

    indexDF.write.saveWithBuckets(
      indexDF,
      indexDataPath.toString,
      logEntry.asInstanceOf[IndexLogEntry].numBuckets,
      indexConfig.indexedColumns)
  }

  lazy val (smallFiles, largeFiles): (Seq[FileStatus], Seq[FileStatus]) = ???
  // use previousIndexLogEntry to get smallFiles and LargeFiles

  override def logEntry: LogEntry = {
    // 1. Create an entry with full data and newly created index files
    // 2. Create a content object using just largeFiles
    // 3. Merge them

    val entry = getIndexLogEntry(spark, df, indexConfig, indexDataPath)
    val largeFilesDirectory: Directory = Directory.fromLeafFiles(largeFiles)
    val mergedContent = Content(entry.content.root.merge(largeFilesDirectory))
    entry.copy(content = mergedContent)
  }
}
