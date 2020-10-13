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

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, OPTIMIZING}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.index.IndexConstants.OPTIMIZE_MODES
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, OptimizeActionEvent}
import com.microsoft.hyperspace.util.{HyperspaceConf, PathUtils}

/**
 * Optimize Action provides an optimize support for indexes where small index files
 * can be merged into larger ones for better index performance.
 *
 * Algorithm outline:
 * 1. Collect all the currently valid index files.
 * 2. Split files into small and large, based on a threshold.
 * 3. Bucketwise combine smaller files into 1 file per bucket.
 * 4. Update index snapshot to remove small files and keep large files + newly created files.
 *
 * NOTE: This is an index-only operation. It does not look at the current state of
 * the data at all. If the data was changed after index creation, optimize will NOT
 * include the data changes.
 *
 * Available modes:
 * `Quick` mode: This mode allows for fast optimization. Files smaller than a
 * predefined threshold "spark.hyperspace.index.optimize.fileSizeThreshold" will be picked
 * for compaction.
 *
 * `Full` mode: This allows for slow but complete optimization. ALL index files are
 * picked for compaction.
 *
 * TODO: Optimize can be a no-op if there is at most one optimizable file per bucket.
 *  https://github.com/microsoft/hyperspace/issues/204.
 *
 * @param spark SparkSession.
 * @param logManager IndexLogManager for index being refreshed.
 * @param dataManager IndexDataManager for index being refreshed.
 * @param mode Acceptable optimize modes are `quick` and `full`.
 */
class OptimizeAction(
    spark: SparkSession,
    final override protected val logManager: IndexLogManager,
    dataManager: IndexDataManager,
    mode: String)
    extends CreateActionBase(dataManager)
    with Action {
  private lazy val previousIndexLogEntry = {
    logManager.getLog(baseId) match {
      case Some(e: IndexLogEntry) => e
      case _ =>
        throw HyperspaceException("LogEntry must exist for optimize operation")
    }
  }

  private lazy val indexConfig: IndexConfig = {
    val ddColumns = previousIndexLogEntry.derivedDataset.properties.columns
    IndexConfig(previousIndexLogEntry.name, ddColumns.indexed, ddColumns.included)
  }

  final override val transientState: String = OPTIMIZING

  final override val finalState: String = ACTIVE

  final override def op(): Unit = {
    // Rewrite index using the eligible files to optimize.
    val numBuckets = previousIndexLogEntry.numBuckets
    val indexDF = spark.read.parquet(filesToOptimize.map(_.name): _*)

    val repartitionedDf =
      indexDF.repartition(numBuckets, indexConfig.indexedColumns.map(indexDF(_)): _*)

    repartitionedDf.write.saveWithBuckets(
      repartitionedDf,
      indexDataPath.toString,
      logEntry.asInstanceOf[IndexLogEntry].numBuckets,
      indexConfig.indexedColumns)
  }

  override def validate(): Unit = {
    super.validate()

    if (!OPTIMIZE_MODES.exists(_.equalsIgnoreCase(mode))) {
      throw HyperspaceException(s"Unsupported optimize mode '$mode' found.")
    }

    if (filesToOptimize.isEmpty) {
      throw NoChangesException(
        "Optimize aborted as no index files smaller than " +
          s"${HyperspaceConf.optimizeFileSizeThreshold(spark)} found.")
    }
  }

  private lazy val (filesToOptimize, filesToIgnore): (Seq[FileInfo], Seq[FileInfo]) = {
    if (mode.equalsIgnoreCase(IndexConstants.OPTIMIZE_MODE_QUICK)) {
      val threshold = HyperspaceConf.optimizeFileSizeThreshold(spark)
      previousIndexLogEntry.content.fileInfos.toSeq.partition(_.size < threshold)
    } else {
      // For 'full' mode, put all the existing index files into 'filesToOptimize' partition so
      // that one file is created per bucket.
      (previousIndexLogEntry.content.fileInfos.toSeq, Seq())
    }
  }

  override def logEntry: LogEntry = {
    // Update `previousIndexLogEntry` to keep `filesToIngore` files and append to it
    // the list of newly created index files.
    val absolutePath = PathUtils.makeAbsolute(indexDataPath)
    val newContent = Content.fromDirectory(absolutePath)
    if (filesToIgnore.nonEmpty) {
      val filesToIgnoreDirectory = {
        val fs = new Path(filesToIgnore.head.name).getFileSystem(new Configuration)
        val filesToIgnoreStatuses =
          filesToIgnore.map(fileInfo => fs.getFileStatus(new Path(fileInfo.name)))

        Directory.fromLeafFiles(filesToIgnoreStatuses)
      }
      val mergedContent = Content(newContent.root.merge(filesToIgnoreDirectory))
      previousIndexLogEntry.copy(content = mergedContent)
    } else {
      previousIndexLogEntry.copy(content = newContent)
    }
  }

  override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    OptimizeActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
