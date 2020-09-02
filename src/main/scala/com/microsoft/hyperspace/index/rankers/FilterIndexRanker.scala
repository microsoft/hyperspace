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

import com.microsoft.hyperspace.index.IndexLogEntry

/**
 * Ranker class for Filter rule indexes
 */
object FilterIndexRanker {

  /**
   * Pick the best index among all available index options according to their cost.
   * Currently this function returns the head of the given index list.
   *
   * If hybridScanEnabled is true, pick the index which covers the most source data
   * so that we could minimize the amount of data for on-the-fly shuffle or merge.
   *
   * @param candidates List of all indexes that fully cover logical plan.
   * @param hybridScanEnabled HybridScan config.
   * @return top-most index which is expected to maximize performance gain
   *         according to ranking algorithm.
   */
  def rank(
      candidates: Seq[IndexLogEntry],
      hybridScanEnabled: Boolean = false): Option[IndexLogEntry] = {
    candidates match {
      case Nil => None
      case _ =>
        if (hybridScanEnabled) {
          Some(candidates.maxBy(_.allSourceFiles.size))
        } else {
          // TODO: Add ranking algorithm to sort candidates.
          Some(candidates.head)
        }
    }
  }

}
