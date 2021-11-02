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

package com.microsoft.hyperspace.recommendation

/**
 * Represent constants used in recommendation options and settings.
 */
object RecommendationConstants {

  ////////////////////////////////////////////////////////////////////////
  // Index recommendation option constants.
  ////////////////////////////////////////////////////////////////////////

  // The maximum number of indexes to recommend.
  val INDEX_COUNT_MAX = "index.count.max"
  val INDEX_COUNT_MAX_DEFAULT = "50"

  // The index storage budget (in GB).
  val INDEX_STORAGE_BUDGET_GB = "index.storage.budget"
  val INDEX_STORAGE_BUDGET_GB_DEFAULT = "3000"

  ////////////////////////////////////////////////////////////////////////
  // Cost metric option constants.
  ////////////////////////////////////////////////////////////////////////

  val COST_METRIC = "cost.metric"
  val COST_METRIC_SIZE = "cost.metric.size"
  val COST_METRIC_CARD = "cost.metric.card"
  val COST_METRIC_DEFAULT: String = COST_METRIC_SIZE
}
