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
 * Ranker class for Join rule indexes
 */
object JoinIndexRanker {

  /**
   * Rearranges all available index options according to their cost. The first one is the best
   * with minimum cost.
   *
   * Pick the one with least amount of shuffling and most number of buckets. When two indices have
   * same number of buckets, there is zero shuffling. If number of buckets differ, one of the
   * indices gets reshuffled into the number of buckets equal to the other. Secondly, more the
   * number of buckets, better the parallelism achieved during join in general, assuming there is
   * no resource constraint.
   *
   * If hybridScanEnabled is true, pick the index pair which covers the most source data
   * so that we could minimize the amount of data for on-the-fly shuffle or merge.
   *
   * @param indexPairs index pairs for left and right side of the join. All index pairs are
   *                   compatible with each other.
   * @param hybridScanEnabled HybridScan config.
   * @return rearranged index pairs according to their ranking. The first is the best.
   */
  def rank(
      indexPairs: Seq[(IndexLogEntry, IndexLogEntry)],
      hybridScanEnabled: Boolean = false): Seq[(IndexLogEntry, IndexLogEntry)] = {

    indexPairs.sortWith {
      case ((left1, left2), (right1, right2)) =>
        if (hybridScanEnabled && ((left1.allSourceFiles.size + left2.allSourceFiles.size) >
              (right1.allSourceFiles.size + right2.allSourceFiles.size))) {
          true
        } else if (hybridScanEnabled && ((left1.allSourceFiles.size + left2.allSourceFiles.size) <
                     (right1.allSourceFiles.size + right2.allSourceFiles.size))) {
          false
        } else if (left1.numBuckets == left2.numBuckets && right1.numBuckets == right2.numBuckets) {
          left1.numBuckets > right1.numBuckets
        } else if (left1.numBuckets == left2.numBuckets) {
          true
        } else if (right1.numBuckets == right2.numBuckets) {
          false
        } else {
          true
        }
    }
  }
}
