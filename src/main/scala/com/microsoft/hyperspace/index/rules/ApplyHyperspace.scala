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
  private val sourceFilters
    : Seq[SourcePlanIndexFilter] = ColumnSchemaFilter :: FileSignatureFilter :: Nil

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
  private val rules
    : Seq[HyperspaceRule] = disabled.FilterIndexRule :: disabled.JoinIndexRule :: NoOpRule :: Nil

  // Map for memoization. The key is the logical plan before applying [[HyperspaceRule]]s
  // and its value is a pair of best transformed plan and its score.
  private val scoreMap: mutable.HashMap[LogicalPlan, (LogicalPlan, Int)] = mutable.HashMap()

  private def recApply(plan: LogicalPlan, indexes: PlanToIndexesMap): (LogicalPlan, Int) = {
    // If pre-calculated value exists, return it.
    scoreMap.get(plan).foreach(res => return res)

    def recChildren(cur: LogicalPlan): (LogicalPlan, Int) = {
      // Get the best plan & score for each child node.
      var score = 0
      val resultPlan = cur.mapChildren { child =>
        val res = recApply(child, indexes)
        score += res._2
        res._1
      }
      (resultPlan, score)
    }

    var optResult = (plan, 0)
    rules.foreach { rule =>
      val (transformedPlan, curScore) = rule(plan, indexes)
      if (curScore > 0 || rule.equals(NoOpRule)) {
        // Positive curScore means the rule is applied.
        val result = recChildren(transformedPlan)
        if (optResult._2 < result._2 + curScore) {
          // Update if the total score is higher than the previous optimal.
          optResult = (result._1, result._2 + curScore)
        }
      }
    }

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

  // During index creation, refresh and optimize, disable ApplyHyperspace rule to avoid
  // unwanted plan transformation.
  private[hyperspace] val disableForIndexMaintenance = new ThreadLocal[Boolean]

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (disableForIndexMaintenance.get) {
      return plan
    }

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
