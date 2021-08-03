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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.index.plananalysis.FilterReasons
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.{PlanToIndexesMap, PlanToSelectedIndexMap}

/**
 * IndexFilter used in ranking applicable indexes.
 */
trait IndexRankFilter extends IndexFilter {

  /**
   * Rank best index for the given query plan.
   *
   * @param plan Query plan
   * @param applicableIndexes Map of source plan to applicable indexes
   * @return Map of source plan to selected index
   */
  def apply(plan: LogicalPlan, applicableIndexes: PlanToIndexesMap): PlanToSelectedIndexMap

  /**
   * Set FILTER_REASONS tag for unselected indexes.
   *
   * @param plan Plan to tag
   * @param indexes Indexes to tag
   * @param selectedIndex Selected index
   */
  protected def setFilterReasonTagForRank(
      plan: LogicalPlan,
      indexes: Seq[IndexLogEntry],
      selectedIndex: IndexLogEntry): Unit = {
    indexes.foreach { index =>
      setFilterReasonTag(
        selectedIndex.name.equals(index.name),
        plan,
        index,
        FilterReasons.AnotherIndexApplied(selectedIndex.name))
    }
  }
}
