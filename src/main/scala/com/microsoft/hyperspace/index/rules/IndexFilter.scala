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
import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.index.rules.HyperspaceOneRule.PlanToIndexesMap

trait IndexFilter {

  /**
   * @return Failure reason for filtered out indexes.
   */
  def reason: String
}

/**
 * IndexFilter used in CandidateIndexCollector.
 */
trait SourcePlanIndexFilter extends IndexFilter with ActiveSparkSession {

  /**
   * Filter out candidate indexes for the given source plan.
   *
   * @param plan Source plan
   * @param indexes Candidate indexes
   * @return Indexes which meet conditions of Filter
   */
  def apply(plan: LogicalPlan, indexes: Seq[IndexLogEntry]): Seq[IndexLogEntry]
}

/**
 * IndexFilter used in HyperspaceRule.
 */
trait QueryPlanIndexFilter extends IndexFilter with ActiveSparkSession {

  /**
   * Filter out candidate indexes for the given query plan.
   *
   * @param plan Query plan.
   * @param indexes Map of source plan to candidate indexes.
   * @return Map of source plan to indexes which meet conditions of Filter.
   */
  def apply(plan: LogicalPlan, indexes: PlanToIndexesMap): PlanToIndexesMap
}
