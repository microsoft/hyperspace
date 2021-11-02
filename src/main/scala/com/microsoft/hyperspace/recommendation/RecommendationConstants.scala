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
 * TODO: Merge it with the global constants.
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

  // The maximum number of indexed columns (for a single index).
  val COLUMN_INDEXED_COUNT_MAX = "column.indexed.count.max"
  val COLUMN_INDEXED_COUNT_MAX_DEFAULT = "5"

  // The maximum number of covering columns (for a single index).
  val COLUMN_COVERING_COUNT_MAX = "column.covering.count.max"
  val COLUMN_COVERING_COUNT_MAX_DEFAULT = "10"

  // Should only recommend indexed data frames or not.
  val IXDF_ONLY = "ixdf.only"
  val IXDF_ONLY_DEFAULT = "false"

  ////////////////////////////////////////////////////////////////////////
  // Cost metric option constants.
  ////////////////////////////////////////////////////////////////////////

  val COST_METRIC = "cost.metric"
  val COST_METRIC_SIZE = "cost.metric.size"
  val COST_METRIC_CARD = "cost.metric.card"
  val COST_METRIC_DEFAULT: String = COST_METRIC_SIZE

  // Cost model related constants.
  val COST_UNIT_SHUFFLE = "cost.unit.shuffle"
  val COST_UNIT_SHUFFLE_DEFAULT = "4.0"

  val COST_UNIT_SORT = "cost.unit.sort"
  val COST_UNIT_SORT_DEFAULT = "1.5"

  val COST_UNIT_SCAN = "cost.unit.scan"
  val COST_UNIT_SCAN_DEFAULT = "1.0"

  ////////////////////////////////////////////////////////////////////////
  // Index recommendation constants.
  ////////////////////////////////////////////////////////////////////////

  // The index types.
  val INDEX_TYPE_FILTER = "Filter"
  val INDEX_TYPE_JOIN = "Join"

  // The candidate index generators.
  val CANDIDATE_GEN = "candidate.gen"
  val CANDIDATE_GEN_COVERING_FILTER_JOIN = "candidate.gen.covering.filter.join"
  val CANDIDATE_GEN_COVERING_DTA = "candidate.gen.covering.dta"
  val CANDIDATE_GEN_DEFAULT: String = CANDIDATE_GEN_COVERING_FILTER_JOIN

  // The number of index buckets.
  val INDEX_BUCKETS = "index.buckets"
  val INDEX_BUCKETS_DEFAULT = "50"

  // The greedy algorithm.
  val GREEDY_SEED_CONFIG_MAX_SIZE = "greedy.seed.config.size.max"
  val GREEDY_SEED_CONFIG_MAX_SIZE_DEFAULT = "1"
  val GREEDY_SEED_CONFIG_GROWTH_MAX_SIZE = "greedy.seed.config.growth.size.max"
  val GREEDY_SEED_CONFIG_GROWTH_MAX_SIZE_DEFAULT = "2"
  val GREEDY_CONFIG_MAX_SIZE = "greedy.config.size.max"
  val GREEDY_CONFIG_MAX_SIZE_DEFAULT = "-1"
  val GREEDY_CANDIDATE_MAX_SIZE = "greedy.candidate.size.max"
  val GREEDY_CANDIDATE_MAX_SIZE_DEFAULT = "5"

  // The index recommenders.
  val INDEX_RECOMMENDER = "index.recommender"
  val INDEX_RECOMMENDER_ALL = "allIndex"
  val INDEX_RECOMMENDER_RULE_BASED = "ruleBased"
  val INDEX_RECOMMENDER_COST_BASED = "costBased"
  val INDEX_RECOMMENDER_COST_DIFF_BASED = "costDiffBased"
  val INDEX_RECOMMENDER_HYBRID = "hybrid"
  val INDEX_RECOMMENDER_HYBRID_GREEDY = "hybrid-greedy"
  val INDEX_RECOMMENDER_HYBRID_GREEDY_MERGED = "hybrid-greedy-merged"
  val INDEX_RECOMMENDER_IXDF_JOIN = "ixdf-join"
  val INDEX_RECOMMENDER_DEFAULT: String = INDEX_RECOMMENDER_RULE_BASED

  // The stats plan visitors.
  val STATS_VISITOR = "stats.visitor"
  val STATS_VISITOR_SIZE_IN_BYTES_ONLY = "stats.visitor.sizeInBytesOnly"
  val STATS_VISITOR_SIZE_IN_BYTES_PK_FK_JOIN = "stats.visitor.sizeInBytesPkFkJoin"
  val STATS_VISITOR_BASIC = "stats.visitor.basic"
  val STATS_VISITOR_BASIC_V2 = "stats.visitor.basicV2"
  val STATS_VISITOR_DEFAULT: String = STATS_VISITOR_SIZE_IN_BYTES_PK_FK_JOIN

  // The "what if" analyzer.
  val WHAT_IF_ANALYZER = "whatIf.analyzer"
  val WHAT_IF_ANALYZER_BASIC = "whatIf.analyzer.basic"
  val WHAT_IF_ANALYZER_BASIC2 = "whatIf.analyzer.basic2"
  val WHAT_IF_ANALYZER_DEFAULT: String = WHAT_IF_ANALYZER_BASIC

  // The ("what if") cost calculator.
  val WHAT_IF_COST_CALCULATOR = "whatIf.cost.calculator"
  val WHAT_IF_COST_CALCULATOR_BASIC = "whatIf.cost.calculator.basic"
  val WHAT_IF_COST_CALCULATOR_DEFAULT: String = WHAT_IF_COST_CALCULATOR_BASIC

  // The ("what if") cost difference calculator.
  val WHAT_IF_COST_DIFF_CALCULATOR = "whatIf.cost.diff.calculator"
  val WHAT_IF_COST_DIFF_CALCULATOR_BASIC = "whatIf.cost.diff.calculator.basic"
  val WHAT_IF_COST_DIFF_CALCULATOR_DEFAULT: String = WHAT_IF_COST_DIFF_CALCULATOR_BASIC

  ///////////////////////////////////////////////////////////////////////
  // Other constants
  ///////////////////////////////////////////////////////////////////////
  // Size in terms of KB, MB, GB.
  val SIZE_KB: Int = 1024
  val SIZE_MB: Int = 1024 * 1024
  val SIZE_GB: Int = 1024 * 1024 * 1024
}
