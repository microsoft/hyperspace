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

package com.microsoft.hyperspace.index.dataskipping.expressions

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.index.dataskipping.BloomFilterTestUtils

class BloomFilterAggTest extends HyperspaceSuite with BloomFilterTestUtils {
  import spark.implicits._

  test("BloomFilterAgg computes BloomFilter correctly.") {
    val n = 10000
    val m = 3000
    val fpp = 0.01

    val agg = new Column(BloomFilterAgg(col("a").expr, m, fpp).toAggregateExpression())
    val df = spark
      .range(n)
      .toDF("a")
      .filter(col("a") % 3 === 0)
      .union(Seq[Integer](null).toDF("a"))
      .agg(agg)
    val bfData = df.collect()(0).getAs[Any](0)

    val expectedBf = BloomFilter.create(m, fpp)
    for (i <- 0 until n) {
      if (i % 3 == 0) {
        expectedBf.put(i.toLong)
      }
    }
    assert(bfData === encodeExternal(expectedBf))
  }
}
