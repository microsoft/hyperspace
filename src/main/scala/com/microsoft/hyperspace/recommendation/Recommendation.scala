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

package com.microsoft.hyperspace.recommendation

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.index.IndexConfig
import com.microsoft.hyperspace.recommendation.cost.{QueryImpact, WorkloadImpact}

/**
 * Represents recommendation results.
 */
class Recommendation {

  /**
   * The map from data frames to their recommended indexes.
   * The data frames here do not need be the same as the workload queries.
   * In fact, the data frames are either base tables or intermediate
   * subqueries (views) on top of which indexes are recommended.
   */
  private val dfToIndexConfigs = new mutable.HashMap[DataFrame, ListBuffer[IndexConfig]]()

  /**
   * Add a recommended index.
   *
   * @param df The table or data frame on which the index should be created.
   * @param indexConfig The recommended index.
   */
  def add(df: DataFrame, indexConfig: IndexConfig): Unit = {
    val indexConfigs = dfToIndexConfigs.getOrElseUpdate(df, new ListBuffer[IndexConfig])
    if (!indexConfigs.contains(indexConfig)) {
      indexConfigs += indexConfig
    }
  }

  /**
   * Get all recommended indexes for the given table or data frame.
   *
   * @param df The table or data frame.
   * @return The recommended indexes.
   */
  def get(df: DataFrame): Seq[IndexConfig] = dfToIndexConfigs(df)

  /**
   * Get all recommendations.
   *
   * @return The map from tables or data frames to their recommended indexes.
   */
  def getAll: Map[DataFrame, Seq[IndexConfig]] = dfToIndexConfigs.toMap

  /**
   * Get the number of indexes recommended.
   *
   * @return The number of indexes recommended.
   */
  def numIndexes: Int = getAllIndexes.length

  /**
   * Get all recommended indexes.
   *
   * @return A collection of all recommended indexes.
   */
  def getAllIndexes: Seq[IndexConfig] =
    dfToIndexConfigs.flatMap(x => x._2).toSeq
}

/**
 * Represents recommendation for a single query.
 *
 * @param recommendation The recommendation.
 * @param impact The impact on query execution cost.
 */
case class QueryRecommendation(recommendation: Recommendation, impact: Option[QueryImpact])

/**
 * Represents recommendation for a workload.
 *
 * @param recommendation The recommendation.
 * @param impact The impact on workload execution cost.
 */
case class WorkloadRecommendation(recommendation: Recommendation, impact: Option[WorkloadImpact])
