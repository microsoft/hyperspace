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

package com.microsoft.hyperspace.index.covering

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags}
import com.microsoft.hyperspace.util.HyperspaceConf

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
   * the index with larger common source data, so that we could minimize the amount of data
   * for on-the-fly shuffle or merge.
   *
   * @param spark SparkSession.
   * @param leftChild Logical relation of left child of the join.
   * @param rightChild Logical relation of right child of the join.
   * @param indexPairs Index pairs for left and right side of the join. All index pairs are
   *                   compatible with each other.
   * @return Rearranged index pairs according to their ranking. The first is the best.
   */
  def rank(
      spark: SparkSession,
      leftChild: LogicalPlan,
      rightChild: LogicalPlan,
      indexPairs: Seq[(IndexLogEntry, IndexLogEntry)]): Seq[(IndexLogEntry, IndexLogEntry)] = {
    val hybridScanEnabled = HyperspaceConf.hybridScanEnabled(spark)
    def getCommonSizeInBytes(logicalPlan: LogicalPlan, index: IndexLogEntry): Long = {
      index.getTagValue(logicalPlan, IndexLogEntryTags.COMMON_SOURCE_SIZE_IN_BYTES).getOrElse(0L)
    }

    indexPairs.sortWith {
      case ((left1, right1), (left2, right2)) =>
        // These common bytes were calculated and tagged in getCandidateIndexes.
        // The value is the summation of common source files of the given plan and each index.
        lazy val commonSizeInBytes1 =
          getCommonSizeInBytes(leftChild, left1) + getCommonSizeInBytes(rightChild, right1)
        lazy val commonSizeInBytes2 =
          getCommonSizeInBytes(leftChild, left2) + getCommonSizeInBytes(rightChild, right2)

        if (left1.numBuckets == right1.numBuckets && left2.numBuckets == right2.numBuckets) {
          if (!hybridScanEnabled || (commonSizeInBytes1 == commonSizeInBytes2)) {
            left1.numBuckets > left2.numBuckets
          } else {
            // If both index pairs have the same number of buckets and Hybrid Scan is enabled,
            // pick the pair with more common bytes with the given source plan, so as to
            // reduce the overhead from handling appended and deleted files.
            commonSizeInBytes1 > commonSizeInBytes2
          }
        } else if (left1.numBuckets == right1.numBuckets) {
          true
        } else if (left2.numBuckets == right2.numBuckets) {
          false
        } else {
          // At this point, both pairs have different number of buckets. If Hybrid Scan is enabled,
          // pick the pair with "more common bytes", otherwise pick the first pair.
          !hybridScanEnabled || commonSizeInBytes1 > commonSizeInBytes2
        }
    }
  }

  private implicit class CoveringIndexLogEntry(entry: IndexLogEntry) {
    def numBuckets: Int = entry.derivedDataset.asInstanceOf[CoveringIndex].numBuckets
  }
}
