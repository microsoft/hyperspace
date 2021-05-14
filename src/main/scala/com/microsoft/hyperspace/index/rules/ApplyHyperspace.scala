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

package com.microsoft.hyperspace.index.rules

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.PlanToIndexesMap
import com.microsoft.hyperspace.telemetry.HyperspaceEventLogging

/**
 * Collect candidate indexes for each source plan.
 */
object CandidateIndexCollector extends ActiveSparkSession {
  private val sourceFilters = ColumnSchemaFilter :: FileSignatureFilter :: Nil

  private def initializePlanToIndexes(
      plan: LogicalPlan,
      indexes: Seq[IndexLogEntry]): PlanToIndexesMap = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    plan.collect {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        (l.asInstanceOf[LogicalPlan], indexes)
    }.toMap
  }

  /**
   * Extract candidate indexes for each source plan in the given query plan.
   *
   * @param plan Original query plan
   * @param allIndexes All indexes
   * @return Map of source plan to candidate indexes
   */
  def apply(plan: LogicalPlan, allIndexes: Seq[IndexLogEntry]): PlanToIndexesMap = {
    val planToIndexes = initializePlanToIndexes(plan, allIndexes)
    planToIndexes.flatMap {
      case (node, allIndexes) =>
        Some(node, sourceFilters.foldLeft(allIndexes) { (indexes, filter) =>
          filter(node, indexes)
        }).filter(_._2.nonEmpty)
    }
  }
}

/**
 * Apply Hyperspace indexes based on the score of each index application.
 */
class ScoreBasedIndexPlanOptimizer {
  // TODO: FilterIndexRule :: JoinIndexRule :: Nil
  private val rules = NoOpRule :: Nil

  // Map for memoization. The key is the logical plan before applying [[HyperspaceRule]]s
  // and its value is a pair of best transformed plan and its score.
  private val scoreMap: mutable.HashMap[LogicalPlan, (LogicalPlan, Int)] = mutable.HashMap()

  private def recApply(plan: LogicalPlan, indexes: PlanToIndexesMap): (LogicalPlan, Int) = {
    // If pre-calculated value exists, return it.
    scoreMap.get(plan).foreach(res => return res)

    val optResult = (plan, 0)
    // TODO apply indexes recursively.

    scoreMap.put(plan, optResult)
    optResult
  }

  /**
   * Transform the given query plan to use selected indexes based on score.
   *
   * @param plan Original query plan
   * @param candidateIndexes Map of source plan to candidate indexes
   * @return Transformed plan using selected indexes based on score
   */
  def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): LogicalPlan = {
    recApply(plan, candidateIndexes)._1
  }
}

/**
 * Transform the given plan to use Hyperspace indexes.
 */
object ApplyHyperspace
    extends Rule[LogicalPlan]
    with Logging
    with HyperspaceEventLogging
    with ActiveSparkSession {

  type PlanToIndexesMap = Map[LogicalPlan, Seq[IndexLogEntry]]
  type PlanToSelectedIndexMap = Map[LogicalPlan, IndexLogEntry]

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val indexManager = Hyperspace
      .getContext(spark)
      .indexCollectionManager
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))
    if (allIndexes.isEmpty) {
      plan
    } else {
      try {
        val candidateIndexes = CandidateIndexCollector(plan, allIndexes)
        new ScoreBasedIndexPlanOptimizer().apply(plan, candidateIndexes)
      } catch {
        case e: Exception =>
          logWarning("Cannot apply Hyperspace indexes: " + e.getMessage)
          plan
      }
    }
  }
}
