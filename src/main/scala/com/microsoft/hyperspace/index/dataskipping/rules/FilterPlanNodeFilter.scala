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

package com.microsoft.hyperspace.index.dataskipping.rules

import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}

import com.microsoft.hyperspace.index.rules.{ExtractRelation, QueryPlanIndexFilter, RuleUtils}
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.PlanToIndexesMap

/**
 * FilterPlanNodeFilter filters indexes out if
 *   1) the given plan is not eligible filter plan node.
 *   2) the source plan of index is not part of the filter plan.
 */
object FilterPlanNodeFilter extends QueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    if (candidateIndexes.isEmpty) {
      return Map.empty
    }
    plan match {
      case Filter(_, ExtractRelation(relation)) if !RuleUtils.isIndexApplied(relation) =>
        candidateIndexes.filterKeys(relation.plan.equals)
      case _ => Map.empty
    }
  }
}
