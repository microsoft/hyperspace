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

package com.microsoft.hyperspace.index.dataskipping.util

import org.apache.spark.sql.functions.{count, max, min}

import com.microsoft.hyperspace.index.dataskipping.DataSkippingSuite

class DataFrameUtilsTest extends DataSkippingSuite {
  test("getSizeInBytes returns an estimated size of the dataframe in bytes.") {
    val df = spark
      .range(100000000)
      .selectExpr("id as A", "cast(id / 100 as int) as B")
      .groupBy("B")
      .agg(count("A").as("count"), min("A").as("min"), max("A").as("max"))
    df.cache()
    df.count() // force cache
    val estimate = DataFrameUtils.getSizeInBytes(df)
    df.repartition(1).write.parquet(dataPath().toString)
    df.unpersist()
    val real = listFiles(dataPath()).filter(isParquet).map(_.getLen).sum
    assert(real / 2 <= estimate && estimate <= real * 2)
  }
}
