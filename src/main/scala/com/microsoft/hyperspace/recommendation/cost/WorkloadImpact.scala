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

package com.microsoft.hyperspace.recommendation.cost

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.index.IndexConfig
import com.microsoft.hyperspace.recommendation.RecommendationConstants

/**
 * Represents impact on a single query.
 *
 * @param df          The query in its [[DataFrame]] representation.
 * @param cost        The [[CostImprovement]] of the query.
 * @param usedIndexes The indexes referenced by the query (plan).
 */
case class QueryImpact(
    df: DataFrame,
    cost: CostImprovement,
    usedIndexes: Option[Seq[IndexConfig]]) {

  override def toString: String = {
    s"cost: ${cost.toString}\nusedIndexes: ${if (usedIndexes.isEmpty) "None"
    else "[" + usedIndexes.get.map(x => x.toString).mkString(", ") + "]"}"
  }
}

/**
 * Represents impact on a workload.
 */
class WorkloadImpact {

  // The set of (QueryImpact, weight) in this workload.
  private val queryImpacts = new ListBuffer[(QueryImpact, Double)]

  /**
   * Add a [[QueryImpact]].
   *
   * @param impact The impact on the query.
   * @param weight The weight of the query (within the workload).
   */
  def addQueryImpact(impact: QueryImpact, weight: Double = 1.0): Unit = {
    queryImpacts += ((impact, weight))
  }

  /**
   * Get all [[QueryImpact]] and their associated weights.
   *
   * @return A [[Seq]] of [[QueryImpact]] with their associated weights.
   */
  def getQueryImpacts: Seq[(QueryImpact, Double)] = {
    queryImpacts.toList
  }

  override def toString: String = {
    val costImprovement = getCostImprovement
    s"Old cost: ${if (costImprovement.oldCost.nonEmpty) costImprovement.oldCost.get.getCost
    else "None"}, New cost: " +
      s"${if (costImprovement.newCost.nonEmpty) costImprovement.newCost.get.getCost
      else "None"}, Improvement: " +
      s"${if (costImprovement.getImprovement.nonEmpty) costImprovement.getImprovement.get
      else "None"}"
  }

  /**
   * Get [[CostImprovement]] of the whole workload.
   *
   * @return The cost improvement of the workload.
   */
  def getCostImprovement: CostImprovement = {
    if (queryImpacts.forall(
        impact => impact._1.cost.oldCost.isDefined && impact._1.cost.newCost.isDefined)) {
      var oldWorkloadCost, newWorkloadCost = 0.0
      queryImpacts.foreach { x =>
        oldWorkloadCost += x._1.cost.oldCost.get.getCost * x._2
        newWorkloadCost += x._1.cost.newCost.get.getCost * x._2
      }
      CostImprovement(
        Some(CostFactory.makeCost(oldWorkloadCost)),
        Some(CostFactory.makeCost(newWorkloadCost)))
    } else {
      CostImprovement(None, None)
    }
  }
}
