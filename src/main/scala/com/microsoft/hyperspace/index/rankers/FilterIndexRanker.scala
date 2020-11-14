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

package com.microsoft.hyperspace.index.rankers

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags}

/**
 * Ranker class for Filter rule indexes.
 */
object FilterIndexRanker {

  /**
   * Pick the best index among all available index options according to their cost.
   *
   * If hybridScanEnabled is true, pick the index which covers the most source data
   * so that we could minimize the amount of data to merge with index data.
   * Otherwise, return the head of the given index list.
   *
   * @param candidates List of all indexes that fully cover logical plan.
   * @param hybridScanEnabled HybridScan config.
   * @return Top-most index which is expected to maximize performance gain
   *         according to ranking algorithm.
   */
  def rank(
      plan: LogicalPlan,
      candidates: Seq[IndexLogEntry],
      hybridScanEnabled: Boolean): Option[IndexLogEntry] = {
    candidates match {
      case Nil => None
      case _ =>
        if (hybridScanEnabled) {
          Some(
            candidates.maxBy(_.getTagValue(plan, IndexLogEntryTags.COMMON_BYTES).getOrElse(0L)))
        } else {
          // TODO: Add ranking algorithm to sort candidates.
          //  See https://github.com/microsoft/hyperspace/issues/52
          Some(candidates.head)
        }
    }
  }
}
