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
      .filter(!_.state.equals(DOESNOTEXIST))
      .map(IndexStatistics(spark, _))
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
    getLogManager(indexName).fold(
      throw HyperspaceException(s"Index with name $indexName could not be found.")) {
      _.getLatestStableLog().filter(!_.state.equalsIgnoreCase(DOESNOTEXIST)) match {
        case Some(l) =>
          import spark.implicits._
          Seq(IndexStatistics(spark, toIndexLogEntry(l), extended = true)).toDF()
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
