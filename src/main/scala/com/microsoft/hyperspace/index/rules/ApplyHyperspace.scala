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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.telemetry.HyperspaceEventLogging
import com.microsoft.hyperspace.util.HyperspaceConf

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

  // Flag to disable ApplyHyperspace rule during index maintenance jobs such as createIndex,
  // refreshIndex and optimizeIndex.
  private[hyperspace] val disableForIndexMaintenance = new ThreadLocal[Boolean]

  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!HyperspaceConf.hyperspaceApplyEnabled(spark) || disableForIndexMaintenance.get) {
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

  def withHyperspaceRuleDisabled[T](f: => T): T = {
    try {
      disableForIndexMaintenance.set(true)
      f
    } finally {
      disableForIndexMaintenance.set(false)
    }
  }
}
