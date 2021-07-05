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
   * @param spark Spark Session.
   * @param plan Logical relation of the filter.
   * @param candidates List of all indexes that fully cover logical plan.
   * @return Top-most index which is expected to maximize performance gain
   *         according to ranking algorithm.
   */
  def rank(
      spark: SparkSession,
      plan: LogicalPlan,
      candidates: Seq[IndexLogEntry]): Option[IndexLogEntry] = {
    candidates match {
      case Nil => None
      case _ =>
        if (HyperspaceConf.hybridScanEnabled(spark)) {
          Some(
            candidates.maxBy(
              _.getTagValue(plan, IndexLogEntryTags.COMMON_SOURCE_SIZE_IN_BYTES).getOrElse(0L)))
        } else {
          // TODO: Add ranking algorithm to sort candidates.
          //  See https://github.com/microsoft/hyperspace/issues/52

          // Pick the index with minimum size. If indexes with same size are found, pick the
          // one with lexicographically smaller name. This is required for deterministic selection
          // of indexes.
          Some(candidates.minBy(index => (index.indexFilesSizeInBytes, index.name)))
        }
    }
  }
}
