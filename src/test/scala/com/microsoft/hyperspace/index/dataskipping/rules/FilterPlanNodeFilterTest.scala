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

import com.microsoft.hyperspace.index.dataskipping._
import com.microsoft.hyperspace.index.dataskipping.sketches._

class FilterPlanNodeFilterTest extends DataSkippingSuite {
  test("apply returns an empty map if there are no candidate indexes.") {
    val df = spark.range(10).toDF("A")
    assert(FilterPlanNodeFilter(df.queryExecution.optimizedPlan, Map.empty) === Map.empty)
  }

  test("apply returns an empty map if the plan is not a filter.") {
    val df = createSourceData(spark.range(10).toDF("A"))
    val indexConfig = DataSkippingIndexConfig("myind", MinMaxSketch("A"))
    val indexLogEntry = createIndexLogEntry(indexConfig, df)
    val candidateIndexes = Map(df.queryExecution.optimizedPlan -> Seq(indexLogEntry))
    val plan = df.groupBy("A").count().queryExecution.optimizedPlan
    assert(FilterPlanNodeFilter(plan, candidateIndexes) === Map.empty)
  }

  test("apply returns applicable indexes only.") {
    val df1 = createSourceData(spark.range(10).toDF("A"), "T1")
    val df2 = createSourceData(spark.range(10).toDF("A"), "T2")
    val indexConfig = DataSkippingIndexConfig("myind", MinMaxSketch("A"))
    val indexLogEntry1 = createIndexLogEntry(indexConfig, df1)
    val indexLogEntry2 = createIndexLogEntry(indexConfig, df2)
    val candidateIndexes = Map(
      df1.queryExecution.optimizedPlan -> Seq(indexLogEntry1),
      df2.queryExecution.optimizedPlan -> Seq(indexLogEntry2))
    val plan = df1.filter("A = 1").queryExecution.optimizedPlan
    assert(
      FilterPlanNodeFilter(plan, candidateIndexes) === Map(
        df1.queryExecution.optimizedPlan -> Seq(indexLogEntry1)))
  }
}
