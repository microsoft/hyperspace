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
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, DOESNOTEXIST}
import com.microsoft.hyperspace.index.IndexConstants.{REFRESH_MODE_FULL, REFRESH_MODE_INCREMENTAL, REFRESH_MODE_QUICK}

class IndexCollectionManager(
    spark: SparkSession,
    indexLogManagerFactory: IndexLogManagerFactory,
    indexDataManagerFactory: IndexDataManagerFactory,
    fileSystemFactory: FileSystemFactory)
    extends IndexManager {
  private val conf: SQLConf = spark.sessionState.conf

  override def create(df: DataFrame, indexConfig: IndexConfigTrait): Unit = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val indexPath = PathResolver(spark.sessionState.conf, hadoopConf)
      .getIndexPath(indexConfig.indexName)
    val dataManager =
      indexDataManagerFactory.create(indexPath, hadoopConf)
    val logManager = getLogManager(indexConfig.indexName) match {
      case Some(manager) => manager
      case None => indexLogManagerFactory.create(indexPath, hadoopConf)
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
    // Note that the behavior of vacuum index is different when the state is ACTIVE.
    // The event that action creates is also different.

    withLogManager(indexName) { logManager =>
      val hadoopConf = spark.sessionState.newHadoopConf()
      val indexPath = PathResolver(spark.sessionState.conf, hadoopConf)
        .getIndexPath(indexName)
      val dataManager =
        indexDataManagerFactory.create(indexPath, hadoopConf)

      logManager.getLatestLog() match {
        case Some(index) if index.state == ACTIVE =>
          // clean up only if state is ACTIVE.
          new VacuumOutdatedAction(logManager, dataManager).run()
        case _ =>
          new VacuumAction(logManager, dataManager).run()
      }
    }
  }

  override def refresh(indexName: String, mode: String): Unit = {
    withLogManager(indexName) { logManager =>
      val hadoopConf = spark.sessionState.newHadoopConf()
      val indexPath = PathResolver(spark.sessionState.conf, hadoopConf)
        .getIndexPath(indexName)
      val dataManager =
        indexDataManagerFactory.create(indexPath, hadoopConf)
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
      val hadoopConf = spark.sessionState.newHadoopConf()
      val indexPath = PathResolver(spark.sessionState.conf, hadoopConf)
        .getIndexPath(indexName)
      val dataManager =
        indexDataManagerFactory.create(indexPath, hadoopConf)
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
      .filter(!_.state.equals(DOESNOTEXIST))
      .map(IndexStatistics(_))
      .toDF()
      .select(
        IndexStatistics.INDEX_SUMMARY_COLUMNS.head,
        IndexStatistics.INDEX_SUMMARY_COLUMNS.tail: _*)
  }

  override def getIndexes(states: Seq[String] = Seq()): Seq[IndexLogEntry] = {
    indexLogManagers
      .map(_.getLatestLog())
      .filter(_.isDefined)
      .map(_.get)
      .filter(index => states.isEmpty || states.contains(index.state))
      .map(toIndexLogEntry)
  }

  override def index(indexName: String): DataFrame = {
    withLogManager(indexName) { logManager =>
      logManager.getLatestStableLog().filter(!_.state.equalsIgnoreCase(DOESNOTEXIST)) match {
        case Some(l) =>
          import spark.implicits._
          Seq(IndexStatistics(toIndexLogEntry(l), extended = true)).toDF()
        case None =>
          throw HyperspaceException(s"No latest stable log found for index $indexName.")
      }
    }
  }

  override def getIndex(indexName: String, logVersion: Int): Option[IndexLogEntry] = {
    withLogManager(indexName) { logManager =>
      logManager.getLog(logVersion).map(_.asInstanceOf[IndexLogEntry])
    }
  }

  override def getIndexVersions(indexName: String, states: Seq[String]): Seq[Int] = {
    withLogManager(indexName) { logManager =>
      logManager.getIndexVersions(states)
    }
  }

  private def indexLogManagers: Seq[IndexLogManager] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val rootPath = PathResolver(conf, hadoopConf).systemPath
    val fs = fileSystemFactory.create(rootPath, hadoopConf)
    val indexPaths: Seq[Path] = if (fs.exists(rootPath)) {
      fs.listStatus(rootPath).map(_.getPath)
    } else {
      Seq()
    }
    indexPaths.map(path => indexLogManagerFactory.create(path, hadoopConf))
  }

  private def getLogManager(indexName: String): Option[IndexLogManager] = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val indexPath = PathResolver(spark.sessionState.conf, hadoopConf)
      .getIndexPath(indexName)
    val fs = fileSystemFactory.create(indexPath, hadoopConf)
    if (fs.exists(indexPath)) {
      Some(indexLogManagerFactory.create(indexPath, hadoopConf))
    } else {
      None
    }
  }

  private def withLogManager[T](indexName: String)(f: IndexLogManager => T): T = {
    getLogManager(indexName) match {
      case Some(logManager) => f(logManager)
      case None => throw HyperspaceException(s"Index with name $indexName could not be found.")
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
