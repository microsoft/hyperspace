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
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags}
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.{PlanToIndexesMap, PlanToSelectedIndexMap}

/**
 * Interface of exclusive type of indexes.
 */
trait HyperspaceRule extends ActiveSparkSession {

  /**
   * Sequence of conditions to apply indexes to the plan. Each filter contains conditions and
   * filters out candidate indexes based on the conditions. The order of the sequence does matter
   * because they are applied in order with the assumption that previous filter conditions were met.
   */
  protected val filtersOnQueryPlan: Seq[QueryPlanIndexFilter]

  /**
   * Index ranker to select the best index among applicable indexes
   * after applying [[filtersOnQueryPlan]]s.
   */
  protected val indexRanker: IndexRankFilter

  /**
   * Transform the plan to use the selected indexes.
   * All selected indexes should be able to be applied to the plan.
   *
   * @param plan Original query plan.
   * @param indexes Selected indexes.
   * @return Transformed plan to use the selected indexes.
   */
  def applyIndex(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): LogicalPlan

  /**
   * Calculate the score of the selected indexes.
   *
   * @param plan Original query plan.
   * @param indexes Selected indexes.
   * @return Score of selected indexes.
   */
  def score(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): Int

  final def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): (LogicalPlan, Int) = {
    if (candidateIndexes.isEmpty) {
      return (plan, 0)
    }

    val applicableIndexes = filtersOnQueryPlan
      .foldLeft(candidateIndexes) { (pti, filter) =>
        filter(plan, pti)
      }

    if (applicableIndexes.nonEmpty) {
      val selectedIndexes = indexRanker(plan, applicableIndexes)
      selectedIndexes.values.toSeq.distinct.foreach(setApplicableIndexTag(plan, _))
      (applyIndex(plan, selectedIndexes), score(plan, selectedIndexes))
    } else {
      (plan, 0)
    }
  }

  protected def setApplicableIndexTag(plan: LogicalPlan, index: IndexLogEntry): Unit = {
    if (index.getTagValue(IndexLogEntryTags.INDEX_PLAN_ANALYSIS_ENABLED).getOrElse(false)) {
      val prevRules =
        index.getTagValue(plan, IndexLogEntryTags.APPLICABLE_INDEX_RULES).getOrElse(Nil)
      index.setTagValue(
        plan,
        IndexLogEntryTags.APPLICABLE_INDEX_RULES,
        prevRules :+ getClass.getSimpleName.split("\\$").last)
    }
  }
}
