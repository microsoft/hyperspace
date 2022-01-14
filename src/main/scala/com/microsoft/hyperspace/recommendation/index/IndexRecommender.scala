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

package com.microsoft.hyperspace.recommendation.index

import scala.collection.mutable

import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.recommendation._

/**
 * An interface that all index recommenders should implement.
 */
trait IndexRecommender {

  /**
   * Recommend indexes for a single query with respect to the given recommendation options.
   *
   * @param df The query.
   * @return The recommendation for the query.
   */
  def recommend(df: DataFrame): QueryRecommendation

  /**
   * Recommend indexes for a workload with respect to the given recommendation options.
   *
   * @param workload The workload.
   * @return The recommendation for the workload.
   */
  def recommend(workload: Workload): WorkloadRecommendation
}
