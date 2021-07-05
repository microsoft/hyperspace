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

import com.microsoft.hyperspace.index.rules.ApplyHyperspace.{PlanToIndexesMap, PlanToSelectedIndexMap}

/**
 * No-op rule for traversal.
 */
object NoOpRule extends HyperspaceRule {

  object FilterAll extends QueryPlanIndexFilter {
    override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap =
      Map.empty
  }

  override val filtersOnQueryPlan = FilterAll :: Nil

  // As there's no applicable index after [[FilterAll]], indexRanker is not reachable.
  override val indexRanker = null

  override def applyIndex(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): LogicalPlan = plan

  override def score(plan: LogicalPlan, indexes: PlanToSelectedIndexMap): Int = 0
}
