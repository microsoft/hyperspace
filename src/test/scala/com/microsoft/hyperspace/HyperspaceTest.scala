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

package com.microsoft.hyperspace

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.index.{HyperspaceSuite, IndexConfig}
import com.microsoft.hyperspace.util.FileUtils

class HyperspaceTest extends HyperspaceSuite {
  private val sampleData = SampleData.testData
  private val sampleParquetDataLocation = inTempDir("sampleparquet")
  private val indexConfig1 = IndexConfig("index1", Seq("RGUID"), Seq("Date"))
  private val indexConfig2 = IndexConfig("index2", Seq("Query"), Seq("imprs"))
  private var df: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val sparkSession = spark
    import sparkSession.implicits._
    FileUtils.delete(new Path(sampleParquetDataLocation))

    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    dfFromSample.write.parquet(sampleParquetDataLocation)

    df = spark.read.parquet(sampleParquetDataLocation)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(sampleParquetDataLocation))
    super.afterAll()
  }

  after {
    clearCache()
    FileUtils.delete(systemPath)
  }

  test(
    "Verify different hyperspace instances with the same Spark session " +
      "can access all indexes (Scenario 1).") {
    // Make a call so index cache gets populated.
    val originalCount = Hyperspace.getContext(spark).indexCollectionManager.indexes.count
    assert(originalCount == 0)

    // Create an index thru a new Hyperspace instance.
    val hs = new Hyperspace(spark)
    hs.createIndex(df, indexConfig1)
    assert(hs.indexes.count == 1)

    // Make sure new index is available to all.
    assert(Hyperspace.getContext(spark).indexCollectionManager.indexes.count == 1)
  }

  test(
    "Verify different hyperspace instances with the same Spark session " +
      "can access all indexes (Scenario 2).") {
    val hs1 = new Hyperspace(spark)
    val hs2 = new Hyperspace(spark)

    assert(hs1.indexes.count == 0)
    assert(hs2.indexes.count == 0)

    hs1.createIndex(df, indexConfig1)
    hs2.createIndex(df, indexConfig2)

    assert(hs1.indexes.count == 2)
    assert(hs2.indexes.count == 2)
  }
}
