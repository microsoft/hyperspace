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

object CandidateIndexCollector extends ActiveSparkSession {
  val checkBatch = ColumnSchemaCheck :: FileSignatureCheck :: IndexPriorityCheck :: Nil

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
    checkBatch.foldLeft(planToIndexes) { (pti, check) =>
      check(pti)
    }
  }
}

class ScoreBasedIndexApplication {
  val ruleBatch = NoOpBatch :: FilterIndexBatch :: Nil
  // Map for memoization
  val scoreMap: mutable.HashMap[LogicalPlan, (LogicalPlan, Int)] = mutable.HashMap()

  def rec(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): (LogicalPlan, Int) = {
    val prev = scoreMap.get(plan)
    if (prev.isDefined) {
      return prev.get
    }

    def recChildren(cur: LogicalPlan): (LogicalPlan, Int) = {
      var score = 0
      val resultPlan = cur.mapChildren { child =>
        val res = rec(child, indexes)
        score += res._2
        res._1
      }
      (resultPlan, score)
    }

    var optResult = (plan, 0)
    ruleBatch.foreach { check =>
      val (transformedPlan, curScore) = check(plan, indexes)
      if (!transformedPlan.equals(plan)) {
        val result = recChildren(transformedPlan)
        if (optResult._2 < result._2 + curScore) {
          optResult = (result._1, result._2 + curScore)
        }
      }
    }
    scoreMap.put(plan, optResult)
    optResult
  }

  def apply(plan: LogicalPlan, indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): LogicalPlan = {
    rec(plan, indexes)._1
  }
}

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
    val candidateIndexes = CandidateIndexCollector(plan, allIndexes)

    new ScoreBasedIndexApplication().apply(plan, candidateIndexes)
  }
}
