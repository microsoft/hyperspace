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

trait HyperspaceBatch extends ActiveSparkSession {
  val checkBatch: Seq[HyperspacePlanCheck]
  def applyIndex(plan: LogicalPlan, indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): LogicalPlan
  def score(plan: LogicalPlan, indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): Int
  final def apply(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): (LogicalPlan, Int) = {
    if (indexes.isEmpty) {
      return (plan, 0)
    }
    val candidateIndexes = checkBatch.foldLeft(indexes) { (pti, check) =>
      check(plan, pti)
    }
    if (candidateIndexes.nonEmpty) {
      (applyIndex(plan, candidateIndexes), score(plan, candidateIndexes))
    } else {
      (plan, 0)
    }
  }
}

object NoOpBatch extends HyperspaceBatch {
  val checkBatch = Nil

  override def applyIndex(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): LogicalPlan = plan

  override def score(plan: LogicalPlan, indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): Int = 0
}
