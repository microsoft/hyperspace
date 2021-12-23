/*
 * Copyright (2021) The Hyperspace Project Authors.
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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.covering.{FilterIndexRule, JoinIndexRule}
import com.microsoft.hyperspace.index.dataskipping.rules.ApplyDataSkippingIndex
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.PlanToIndexesMap
import com.microsoft.hyperspace.index.zordercovering.ZOrderFilterIndexRule

/**
 * Apply Hyperspace indexes based on the score of each index application.
 */
class ScoreBasedIndexPlanOptimizer {
  private val rules: Seq[HyperspaceRule] =
    Seq(FilterIndexRule, JoinIndexRule, ApplyDataSkippingIndex, ZOrderFilterIndexRule, NoOpRule)

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
