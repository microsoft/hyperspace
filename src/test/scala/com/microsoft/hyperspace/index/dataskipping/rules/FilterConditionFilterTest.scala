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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._

import com.microsoft.hyperspace.index.IndexLogEntryTags
import com.microsoft.hyperspace.index.dataskipping._
import com.microsoft.hyperspace.index.dataskipping.sketches._
import com.microsoft.hyperspace.index.plananalysis.FilterReasons.IneligibleFilterCondition

class FilterConditionFilterTest extends DataSkippingSuite {
  test("apply returns an empty map if there are no candidate indexes.") {
    val df = spark.range(10).toDF("A")
    assert(FilterConditionFilter(df.queryExecution.optimizedPlan, Map.empty) === Map.empty)
  }

  test("apply returns an empty map if the plan is not a filter.") {
    val df = createSourceData(spark.range(10).toDF("A"))
    val indexConfig = DataSkippingIndexConfig("myind", MinMaxSketch("A"))
    val indexLogEntry = createIndexLogEntry(indexConfig, df)
    val candidateIndexes = Map(df.queryExecution.optimizedPlan -> Seq(indexLogEntry))
    assert(FilterConditionFilter(df.queryExecution.optimizedPlan, candidateIndexes) === Map.empty)
  }

  test("apply creates an index data predicate if the index can be applied to the plan.") {
    val df = createSourceData(spark.range(10).toDF("A"))
    val indexConfig = DataSkippingIndexConfig("myind", MinMaxSketch("A"))
    val indexLogEntry = createIndexLogEntry(indexConfig, df)
    val candidateIndexes = Map(df.queryExecution.optimizedPlan -> Seq(indexLogEntry))
    val plan = df.filter("A = 1").queryExecution.optimizedPlan
    assert(FilterConditionFilter(plan, candidateIndexes) === candidateIndexes)
    val indexDataPredOpt =
      indexLogEntry.getTagValue(plan, IndexLogEntryTags.DATASKIPPING_INDEX_PREDICATE)
    assert(
      indexDataPredOpt === Some(Some(And(
        IsNotNull(UnresolvedAttribute("MinMax_A__0")),
        And(
          LessThanOrEqual(UnresolvedAttribute("MinMax_A__0"), Literal(1L)),
          GreaterThanOrEqual(UnresolvedAttribute("MinMax_A__1"), Literal(1L)))))))
  }

  test("apply returns an empty map if the filter condition is not suitable.") {
    val df = createSourceData(spark.range(10).selectExpr("id as A", "id * 2 as B"))
    val indexConfig = DataSkippingIndexConfig("myind", MinMaxSketch("A"))
    val indexLogEntry = createIndexLogEntry(indexConfig, df)
    indexLogEntry.setTagValue(IndexLogEntryTags.INDEX_PLAN_ANALYSIS_ENABLED, true)
    val candidateIndexes = Map(df.queryExecution.optimizedPlan -> Seq(indexLogEntry))
    val plan = df.filter("B = 1").queryExecution.optimizedPlan
    assert(FilterConditionFilter(plan, candidateIndexes) === Map.empty)
    val reason = indexLogEntry.getTagValue(plan, IndexLogEntryTags.FILTER_REASONS)
    assert(reason === Some(List(IneligibleFilterCondition("((`B` IS NOT NULL) AND (`B` = 1L))"))))
  }

  test("apply returns only the applicable indexes when there are multiple candidate indexes.") {
    val df = createSourceData(spark.range(10).selectExpr("id as A", "id * 2 as B"))
    val indexConfig1 = DataSkippingIndexConfig("myind", MinMaxSketch("A"))
    val indexConfig2 = DataSkippingIndexConfig("myind", MinMaxSketch("B"))
    val indexLogEntry1 = createIndexLogEntry(indexConfig1, df)
    val indexLogEntry2 = createIndexLogEntry(indexConfig2, df)
    val candidateIndexes =
      Map(df.queryExecution.optimizedPlan -> Seq(indexLogEntry1, indexLogEntry2))
    val plan = df.filter("A = 1").queryExecution.optimizedPlan
    assert(
      FilterConditionFilter(plan, candidateIndexes) === Map(
        df.queryExecution.optimizedPlan -> Seq(indexLogEntry1)))
  }
}
