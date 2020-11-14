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
import com.microsoft.hyperspace.index.rules.RuleUtils

/**
 * Ranker class for Join rule indexes.
 */
object JoinIndexRanker {

  /**
   * Rearranges all available index options according to their cost. The first one is the best
   * with minimum cost.
   *
   * If hybridScanEnabled is false, pick the one with least amount of shuffling and most number
   * of buckets. When two indices have same number of buckets, there is zero shuffling.
   * If number of buckets differ, one of the indices gets reshuffled into the number of buckets
   * equal to the other.
   * Secondly, more the number of buckets, better the parallelism achieved during join in general
   * assuming there is no resource constraint.
   *
   * If hybridScanEnabled is true, rank algorithm follows the algorithm above, but we prioritize
   * the index with larger and usable index data for each index, so as to minimize the amount of
   * data for on-the-fly shuffle or merge.
   *
   * @param indexPairs Index pairs for left and right side of the join. All index pairs are
   *                   compatible with each other.
   * @param hybridScanEnabled HybridScan config.
   * @return Rearranged index pairs according to their ranking. The first is the best.
   */
  def rank(
      leftPlan: LogicalPlan,
      rightPlan: LogicalPlan,
      indexPairs: Seq[(IndexLogEntry, IndexLogEntry)],
      hybridScanEnabled: Boolean): Seq[(IndexLogEntry, IndexLogEntry)] = {
    indexPairs.sortWith {
      case ((left1, left2), (right1, right2)) =>
        lazy val leftCommonBytes = left1
          .getTagValue(leftPlan, IndexLogEntryTags.COMMON_BYTES)
          .get +
          left2.getTagValue(leftPlan, IndexLogEntryTags.COMMON_BYTES).get
        lazy val rightCommonBytes = right1
          .getTagValue(rightPlan, IndexLogEntryTags.COMMON_BYTES)
          .get +
          right2.getTagValue(rightPlan, IndexLogEntryTags.COMMON_BYTES).get

        if (left1.numBuckets == left2.numBuckets && right1.numBuckets == right2.numBuckets) {
          if (!hybridScanEnabled || (leftCommonBytes == rightCommonBytes)) {
            left1.numBuckets > right1.numBuckets
          } else {
            leftCommonBytes > rightCommonBytes
          }
        } else if (left1.numBuckets == left2.numBuckets) {
          true
        } else if (right1.numBuckets == right2.numBuckets) {
          false
        } else {
          !hybridScanEnabled || leftCommonBytes > rightCommonBytes
        }
    }
  }
}
