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

package com.microsoft.hyperspace.index.plananalysis

import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.exchange.{BroadcastExchangeExec, ShuffleExchangeExec}

case class PhysicalOperatorComparison(
    name: String,
    numOccurrencesInPlan1: Int,
    numOccurrencesInPlan2: Int)

case class PhysicalOperatorStats(comparisonStats: Seq[PhysicalOperatorComparison])

// `PhysicalOperatorAnalyzer` provides a functionality to analyze/compare two physical plans.
class PhysicalOperatorAnalyzer {
  // Analyzes two `SparkPlan`s and returns the stat on how many times each operator occurs
  // in each plan.
  def analyze(plan1: SparkPlan, plan2: SparkPlan): PhysicalOperatorStats = {
    val (stat1, stat2) = (compute(plan1), compute(plan2))

    val comparisonStats = (stat1.keys ++ stat2.keys).toSeq.distinct.map { k =>
      PhysicalOperatorComparison(k, stat1.getOrElse(k, 0), stat2.getOrElse(k, 0))
    }

    PhysicalOperatorStats(comparisonStats)
  }

  // Computes the number of occurrences of each operator.
  private def compute(plan: SparkPlan): Map[String, Int] = {
    val opStats = collection.mutable.Map[String, Int]().withDefaultValue(0)
    plan.foreach { node =>
      val name = node match {
        // `ShuffleExchangeExec` and `BroadcastExchangeExec` become `Exchange` in `nodeName`,
        // thus spell them out to be clear.
        case _: ShuffleExchangeExec => "ShuffleExchange"
        case _: BroadcastExchangeExec => "BroadcastExchange"
        case other => other.nodeName
      }
      opStats(name.trim) += 1
    }
    opStats.toMap
  }
}
