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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.ActiveSparkSession
import com.microsoft.hyperspace.index.rules.HyperspaceOneRule.PlanToIndexesMap

/**
 * Interface of exclusive type of indexes.
 */
trait HyperspaceRule extends ActiveSparkSession {

  /**
   * Sequence of conditions to apply indexes. Each filter contains conditions and
   * filter out candidate indexes based on the conditions. The order of the sequence does matter
   * because they are applied in order with the assumption that previous filter conditions met.
   */
  val filtersOnQueryPlan: Seq[IndexFilterOnQueryPlan]

  /**
   * Transform the plan to use the selected indexes after applying [[filtersOnQueryPlan]].
   * All selected indexes should be able to be applied to the plan.
   *
   * @param plan Original query plan.
   * @param indexes Selected indexes.
   * @return Transformed plan to use the selected indexes.
   */
  def applyIndex(plan: LogicalPlan, indexes: PlanToIndexesMap): LogicalPlan

  /**
   * Calculate the score of the selected indexes.
   *
   * @param plan Original query plan.
   * @param indexes Selected indexes.
   * @return Score of selected indexes.
   */
  def score(plan: LogicalPlan, indexes: PlanToIndexesMap): Int

  final def apply(plan: LogicalPlan, indexes: PlanToIndexesMap): (LogicalPlan, Int) = {
    if (indexes.isEmpty) {
      return (plan, 0)
    }

    val selectedIndexes = filtersOnQueryPlan.foldLeft(indexes) { (pti, filter) =>
      filter(plan, pti)
    }

    if (selectedIndexes.nonEmpty) {
      (applyIndex(plan, selectedIndexes), score(plan, selectedIndexes))
    } else {
      (plan, 0)
    }
  }
}

/**
 * No-op rule for traversal.
 */
object NoOpRule extends HyperspaceRule {
  override val filtersOnQueryPlan = Nil

  override def applyIndex(plan: LogicalPlan, indexes: PlanToIndexesMap): LogicalPlan = plan

  override def score(plan: LogicalPlan, indexes: PlanToIndexesMap): Int = 0
}
