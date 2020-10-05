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

package com.microsoft.hyperspace

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.microsoft.hyperspace.index.IndexConstants.REFRESH_MODE_INCREMENTAL
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.plananalysis.PlanAnalyzer

class Hyperspace(spark: SparkSession) {
  private val indexManager: IndexManager = Hyperspace.getContext(spark).indexCollectionManager

  /**
   * Collect all the index metadata.
   *
   * @return all index metadata as a [[DataFrame]].
   */
  def indexes: DataFrame = indexManager.indexes

  /**
   * Create index.
   *
   * @param df the DataFrame object to build index on.
   * @param indexConfig the configuration of index to be created.
   */
  def createIndex(df: DataFrame, indexConfig: IndexConfig): Unit = {
    indexManager.create(df, indexConfig)
  }

  /**
   * Soft deletes the index with given index name.
   *
   * @param indexName the name of index to delete.
   */
  def deleteIndex(indexName: String): Unit = {
    indexManager.delete(indexName)
  }

  /**
   * Restores index with given index name.
   *
   * @param indexName Name of the index to restore.
   */
  def restoreIndex(indexName: String): Unit = {
    indexManager.restore(indexName)
  }

  /**
   * Does hard delete of indexes marked as `DELETED`.
   *
   * @param indexName Name of the index to restore.
   */
  def vacuumIndex(indexName: String): Unit = {
    indexManager.vacuum(indexName)
  }

  /**
   * Update indexes for the latest version of the data.
   *
   * @param indexName Name of the index to refresh.
   */
  def refreshIndex(indexName: String, mode: String = REFRESH_MODE_INCREMENTAL): Unit = {
    indexManager.refresh(indexName, mode)
  }

  /**
   * Cancel api to bring back index from an inconsistent state to the last known stable state.
   * E.g. if index fails during creation, in "CREATING" state.
   * The index will not allow any index modifying operations unless a cancel is called.
   *
   * Note: Cancel from "VACUUMING" state will move it forward to "DOESNOTEXIST" state.
   * Note: If no previous stable state exists, cancel will move it to "DOESNOTEXIST" state.
   *
   * @param indexName Name of the index to cancel.
   */
  def cancel(indexName: String): Unit = {
    indexManager.cancel(indexName)
  }

  /**
   * Explains how indexes will be applied to the given dataframe.
   *
   * @param df dataFrame.
   * @param redirectFunc optional function to redirect output of explain.
   * @param verbose Flag to enable verbose mode.
   */
  def explain(df: DataFrame, verbose: Boolean = false)(
      implicit redirectFunc: String => Unit = print): Unit = {
    redirectFunc(PlanAnalyzer.explainString(df, spark, indexManager.indexes, verbose))
  }
}

object Hyperspace {
  private lazy val context = new ThreadLocal[HyperspaceContext]

  private[hyperspace] def getContext(spark: SparkSession): HyperspaceContext = {
    val ctx = context.get()
    if (ctx == null || !ctx.spark.equals(spark)) {
      // Ensure the Spark session the caller provides is the same as
      // the one HyperspaceContext is using because Hyperspace depends on the
      // session's properties such as configs, etc.
      context.set(new HyperspaceContext(spark))
    }

    context.get()
  }

  def apply(): Hyperspace = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw HyperspaceException("Could not find active SparkSession.")
    }

    new Hyperspace(sparkSession)
  }
}

private[hyperspace] class HyperspaceContext(val spark: SparkSession) {
  val indexCollectionManager = CachingIndexCollectionManager(spark)
}
