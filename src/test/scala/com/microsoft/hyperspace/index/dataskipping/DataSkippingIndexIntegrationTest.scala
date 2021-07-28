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

package com.microsoft.hyperspace.index.dataskipping

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.dataskipping.sketch.MinMaxSketch

class DataSkippingIndexIntegrationTest extends DataSkippingSuite {
  import spark.implicits._

  override val numParallelism: Int = 10

  test("Non-deterministic expression is blocked.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    val ex = intercept[HyperspaceException](
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A + rand()"))))
    assert(
      ex.msg.contains("DataSkippingIndex does not support indexing an expression " +
        "which is non-deterministic: A + rand()"))
  }

  test("Subquery expression is blocked.") {
    withTable("T") {
      spark.range(100).toDF("B").write.saveAsTable("T")
      val df = createSourceData(spark.range(100).toDF("A"))
      val ex = intercept[HyperspaceException](
        hs.createIndex(
          df,
          DataSkippingIndexConfig("myind", MinMaxSketch("A + (select max(B) from T)"))))
      assert(
        ex.msg.contains("DataSkippingIndex does not support indexing an expression " +
          "which has a subquery: A + (select max(B) from T)"))
    }
  }

  test("Foldable expression is blocked.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    val ex = intercept[HyperspaceException](
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("1+1"))))
    assert(
      ex.msg.contains("DataSkippingIndex does not support indexing an expression " +
        "which is evaluated to a constant: 1+1"))
  }

  test("Aggregate function is blocked.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    val ex = intercept[HyperspaceException](
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("min(A)"))))
    assert(
      ex.msg.contains("DataSkippingIndex does not support indexing aggregate functions: " +
        "min(A)"))
  }

  test("Window function is blocked.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    val ex = intercept[HyperspaceException](
      hs.createIndex(
        df,
        DataSkippingIndexConfig(
          "myind",
          MinMaxSketch("min(a) over (rows between 1 preceding and 1 following)"))))
    assert(
      ex.msg.contains("DataSkippingIndex does not support indexing window functions: " +
        "min(a) over (rows between 1 preceding and 1 following)"))
  }
}
