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

import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.PlanToIndexesMap

/**
 * Collect candidate indexes for each source plan.
 */
object CandidateIndexCollector extends ActiveSparkSession {
  private val sourceFilters: Seq[SourcePlanIndexFilter] =
    ColumnSchemaFilter :: FileSignatureFilter :: Nil

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
        Some(
          node,
          sourceFilters.foldLeft(allIndexes) { (indexes, filter) =>
            filter(node, indexes)
          }).filter(_._2.nonEmpty)
    }
  }
}
