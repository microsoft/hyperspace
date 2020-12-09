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
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.internal.SQLConf

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions._
import com.microsoft.hyperspace.actions.Constants.States.DOESNOTEXIST
import com.microsoft.hyperspace.index.IndexConstants.{REFRESH_MODE_FULL, REFRESH_MODE_INCREMENTAL, REFRESH_MODE_QUICK}

class IndexCollectionManager(
    spark: SparkSession,
    indexLogManagerFactory: IndexLogManagerFactory,
    indexDataManagerFactory: IndexDataManagerFactory,
    fileSystemFactory: FileSystemFactory)
    extends IndexManager {
  private val conf: SQLConf = spark.sessionState.conf

  override def create(df: DataFrame, indexConfig: IndexConfig): Unit = {
    val indexPath = PathResolver(spark.sessionState.conf).getIndexPath(indexConfig.indexName)
    val dataManager = indexDataManagerFactory.create(indexPath)
    val logManager = getLogManager(indexConfig.indexName) match {
      case Some(manager) => manager
      case None => indexLogManagerFactory.create(indexPath)
    }

    new CreateAction(spark, df, indexConfig, logManager, dataManager).run()
  }

  override def delete(indexName: String): Unit = {
    withLogManager(indexName) { logManager =>
      new DeleteAction(logManager).run()
    }
  }

  override def restore(indexName: String): Unit = {
    withLogManager(indexName) { logManager =>
      new RestoreAction(logManager).run()
    }
  }

  override def vacuum(indexName: String): Unit = {
    withLogManager(indexName) { logManager =>
      val indexPath = PathResolver(spark.sessionState.conf).getIndexPath(indexName)
      val dataManager = indexDataManagerFactory.create(indexPath)
      new VacuumAction(logManager, dataManager).run()
    }
  }

  override def refresh(indexName: String, mode: String): Unit = {
    withLogManager(indexName) { logManager =>
      val indexPath = PathResolver(spark.sessionState.conf).getIndexPath(indexName)
      val dataManager = indexDataManagerFactory.create(indexPath)
      if (mode.equalsIgnoreCase(REFRESH_MODE_INCREMENTAL)) {
        new RefreshIncrementalAction(spark, logManager, dataManager).run()
      } else if (mode.equalsIgnoreCase(REFRESH_MODE_FULL)) {
        new RefreshAction(spark, logManager, dataManager).run()
      } else if (mode.equalsIgnoreCase(REFRESH_MODE_QUICK)) {
        new RefreshQuickAction(spark, logManager, dataManager).run()
      } else {
        throw HyperspaceException(s"Unsupported refresh mode '$mode' found.")
      }
    }
  }

  override def optimize(indexName: String, mode: String): Unit = {
    withLogManager(indexName) { logManager =>
      val indexPath = PathResolver(spark.sessionState.conf).getIndexPath(indexName)
      val dataManager = indexDataManagerFactory.create(indexPath)
      new OptimizeAction(spark, logManager, dataManager, mode).run()
    }
  }

  override def cancel(indexName: String): Unit = {
    withLogManager(indexName) { logManager =>
      new CancelAction(logManager).run()
    }
  }

  override def indexes: DataFrame = {
    import spark.implicits._
    getIndexes()
      .filter(!_.state.equals(Constants.States.DOESNOTEXIST))
      .map(IndexSummary(spark, _))
      .toDF()
  }

  override def getIndexes(states: Seq[String] = Seq()): Seq[IndexLogEntry] = {
    indexLogManagers
      .map(_.getLatestLog())
      .filter(_.isDefined)
      .map(_.get)
      .filter(index => states.isEmpty || states.contains(index.state))
      .map(toIndexLogEntry)
  }

  override def getIndexStats(indexName: String): DataFrame = {
    getLogManager(indexName).fold(
      throw HyperspaceException(s"Index with name $indexName could not be found.")) {
      _.getLatestStableLog().filter(!_.state.equalsIgnoreCase(DOESNOTEXIST)) match {
        case Some(l) =>
          import spark.implicits._
          Seq(IndexStatistics(spark, toIndexLogEntry(l))).toDF()
        case None =>
          throw HyperspaceException(s"No latest stable log found for index $indexName.")
      }
    }
  }

  private def indexLogManagers: Seq[IndexLogManager] = {
    val rootPath = PathResolver(conf).systemPath
    val fs = fileSystemFactory.create(rootPath)
    val indexPaths: Seq[Path] = if (fs.exists(rootPath)) {
      fs.listStatus(rootPath).map(_.getPath)
    } else {
      Seq()
    }
    indexPaths.map(path => indexLogManagerFactory.create(path))
  }

  private def getLogManager(indexName: String): Option[IndexLogManager] = {
    val indexPath = PathResolver(spark.sessionState.conf).getIndexPath(indexName)
    val fs = fileSystemFactory.create(indexPath)
    if (fs.exists(indexPath)) {
      Some(indexLogManagerFactory.create(indexPath))
    } else {
      None
    }
  }

  private def withLogManager(indexName: String)(f: IndexLogManager => Unit): Unit = {
    getLogManager(indexName) match {
      case Some(logManager) => f(logManager)
      case None => throw HyperspaceException(s"Index with name $indexName could not be found")
    }
  }

  private def toIndexLogEntry(logEntry: LogEntry): IndexLogEntry = {
    logEntry.asInstanceOf[IndexLogEntry]
  }
}

object IndexCollectionManager {
  def apply(spark: SparkSession): IndexCollectionManager =
    new IndexCollectionManager(
      spark,
      IndexLogManagerFactoryImpl,
      IndexDataManagerFactoryImpl,
      FileSystemFactoryImpl)
}

/**
 * Case class representing index summary
 *
 * TODO: Finalize about adding these: data location, signatures, file lists etc.
 *
 * @param name index name
 * @param indexedColumns indexed columns
 * @param includedColumns included columns
 * @param numBuckets number of buckets
 * @param schema index schema json
 * @param indexLocation index location
 * @param state index state
 */
private[hyperspace] case class IndexSummary(
    name: String,
    indexedColumns: Seq[String],
    includedColumns: Seq[String],
    numBuckets: Int,
    schema: String,
    indexLocation: String,
    state: String)

private[hyperspace] object IndexSummary {
  def apply(spark: SparkSession, entry: IndexLogEntry): IndexSummary = {
    IndexSummary(
      entry.name,
      entry.derivedDataset.properties.columns.indexed,
      entry.derivedDataset.properties.columns.included,
      entry.numBuckets,
      entry.derivedDataset.properties.schemaString,
      indexDirPath(entry),
      entry.state)
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

/**
 * Case class representing index metadata and index statistics from latest index version.
 *
 * @param name Index name.
 * @param indexedColumns Indexed columns.
 * @param includedColumns Included columns.
 * @param numBuckets Number of buckets.
 * @param schema Index schema json.
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
 */
private[hyperspace] case class IndexStatistics(
    name: String,
    indexedColumns: Seq[String],
    includedColumns: Seq[String],
    numBuckets: Int,
    schema: String,
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
    indexRootPaths: Seq[String])

private[hyperspace] object IndexStatistics {
  def apply(spark: SparkSession, entry: IndexLogEntry): IndexStatistics = {
    val indexSummary = IndexSummary(spark, entry)
    IndexStatistics(
      indexSummary.name,
      indexSummary.indexedColumns,
      indexSummary.includedColumns,
      indexSummary.numBuckets,
      indexSummary.schema,
      indexSummary.state,
      entry.derivedDataset.kind,
      entry.hasLineageColumn,
      entry.content.fileInfos.size,
      entry.content.fileInfos.map(_.size).sum,
      entry.sourceFileInfoSet.size,
      entry.sourceFileInfoSet.map(_.size).sum,
      entry.appendedFiles.size,
      entry.appendedFiles.map(_.size).sum,
      entry.deletedFiles.size,
      entry.deletedFiles.map(_.size).sum,
      getIndexRootPaths(entry))
  }

  /**
   * Extract top-most index directories which contain index files. Each index directory
   * contains index files created for an active version of index.
   *
   * @param entry Index log entry.
   * @return List of paths to index directories for all active versions of index.
   */
  private def getIndexRootPaths(entry: IndexLogEntry): Seq[String] = {
    var root = entry.content.root
    var prefix = entry.content.root.name
    while (root.subDirs.size == 1 &&
           !root.subDirs.head.name.startsWith(IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX)) {
      prefix += s"/${root.subDirs.head.name}"
      root = root.subDirs.head
    }

    root.subDirs.map(d => prefix + s"/${d.name}")
  }
}
