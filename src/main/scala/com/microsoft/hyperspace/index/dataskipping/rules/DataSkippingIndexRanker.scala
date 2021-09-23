/*
 * Copyright (2021) The Hyperspace Project Authors.
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

package com.microsoft.hyperspace.index.dataskipping.rules

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.dataskipping.DataSkippingIndex
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.{PlanToIndexesMap, PlanToSelectedIndexMap}
import com.microsoft.hyperspace.index.rules.IndexRankFilter

object DataSkippingIndexRanker extends IndexRankFilter {
  override def apply(
      plan: LogicalPlan,
      applicableIndexes: PlanToIndexesMap): PlanToSelectedIndexMap = {
    // TODO: Multiple data skipping index can be applied to the same plan node,
    // although the effectiveness decreases as more indexes are applied.
    // The framework should be updated to allow multiple indexes.
    // For now, simply choose the index with most sketches.
    applicableIndexes.collect {
      case (plan, indexes) if indexes.nonEmpty =>
        plan -> indexes.maxBy(_.derivedDataset.asInstanceOf[DataSkippingIndex].sketches.length)
    }
  }
}
