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
import com.microsoft.hyperspace.telemetry.HyperspaceEventLogging

/**
 * Collect candidate indexes for each relation.
 */
object CandidateIndexCollector extends ActiveSparkSession {
  // TODO: ColumnSchemaFilter :: FileSignatureFilter :: Nil
  val sourceFilters: Seq[SourceFilter] = Nil

  def initializePlanToIndexes(
      plan: LogicalPlan,
      indexes: Seq[IndexLogEntry]): Map[LogicalPlan, Seq[IndexLogEntry]] = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    plan.collect {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        (l.asInstanceOf[LogicalPlan], indexes)
    }.toMap
  }

  def apply(
      plan: LogicalPlan,
      allIndexes: Seq[IndexLogEntry]): Map[LogicalPlan, Seq[IndexLogEntry]] = {
    val planToIndexes = initializePlanToIndexes(plan, allIndexes)
    planToIndexes.flatMap { case (node, candidateIndexes) =>
      Some(node, sourceFilters.foldLeft(candidateIndexes) { (indexes, filter) =>
        val res = filter(node, indexes)
        res
      })
        .filterNot(_._2.isEmpty)
    }
  }
}

/**
 * Apply Hyperspace indexes based on the score of each index application.
 */
class ScoreBasedIndexApplication {
  // TODO: FilterIndexRule :: JoinIndexRule :: Nil
  val rules = NoOpRule :: Nil

  // Map for memoization.
  val scoreMap: mutable.HashMap[LogicalPlan, (LogicalPlan, Int)] = mutable.HashMap()

  def recApply(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): (LogicalPlan, Int) = {
    // If pre-calculated value exists, return it.
    scoreMap.get(plan).foreach(res =>
      return res
    )

    val optResult = (plan, 0)
    // TODO apply indexes recursively.

    scoreMap.put(plan, optResult)
    optResult
  }

  def apply(plan: LogicalPlan, indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): LogicalPlan = {
    recApply(plan, indexes)._1
  }
}

/**
 * Transform the given plan to use Hyperspace indexes.
 */
object HyperspaceOneRule
    extends Rule[LogicalPlan]
    with Logging
    with HyperspaceEventLogging
    with ActiveSparkSession {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    val indexManager = Hyperspace
      .getContext(spark)
      .indexCollectionManager
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))
    if (allIndexes.isEmpty) {
      plan
    } else {
      try {
        val candidateIndexes = CandidateIndexCollector(plan, allIndexes)
        val res = new ScoreBasedIndexApplication().apply(plan, candidateIndexes)
        res
      }
      catch { case _: Exception =>
        plan
      }
    }
  }
}
