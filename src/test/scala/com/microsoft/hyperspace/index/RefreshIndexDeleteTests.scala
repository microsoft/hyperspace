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

package com.microsoft.hyperspace.index

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources.PartitioningUtils

import com.microsoft.hyperspace.{Hyperspace, SampleData}
import com.microsoft.hyperspace.util.FileUtils

class RefreshIndexDeleteTests extends HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/indexLocation")
  private val sampleData = SampleData.testData
  private val sampleDataLocation = "src/test/resources/sampleParquet"
  private val partitionKey1 = "Date"
  private val partitionKey2 = "Query"
  private val indexConfig = IndexConfig("index1", Seq("Query"), Seq("imprs"))
  private var dataDF: DataFrame = _
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val sparkSession = spark
    import sparkSession.implicits._
    hyperspace = new Hyperspace(sparkSession)
    FileUtils.delete(new Path(sampleDataLocation))

    // save test data partitioned.
    // `Date` is the first partition key and `Query` is the second partition key.
    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    dfFromSample.select(partitionKey1).distinct().collect().foreach { d =>
      val date = d.get(0)
      dfFromSample
        .filter($"date" === date)
        .select(partitionKey2)
        .distinct()
        .collect()
        .foreach { q =>
          val query = q.get(0)
          val partitionPath =
            s"$sampleDataLocation/$partitionKey1=$date/$partitionKey2=$query"
          dfFromSample
            .filter($"date" === date && $"Query" === query)
            .select("RGUID", "imprs", "clicks")
            .write
            .parquet(partitionPath)
        }
    }
    dataDF = spark.read.parquet(sampleDataLocation)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(sampleDataLocation))
    super.afterAll()
  }

  after {
    FileUtils.delete(systemPath)
  }

  test("Validate refresh index with deleted files.") {
    // scalastyle:off

    //    val df = spark.read.parquet("C:\\Users\\pouriap\\Desktop\\scratch\\samplepartitionedparquet")
    //    df.show(false)

    val pCols = Seq("Date", "Query", "ColA", "ColB", "ColC")

    // val p = "C:/Users/pouriap/Desktop/scratch/samplepartitionedparquet/Date=2017-09-03/Query=donde"
    val p = "C:/Users/pouriap/Desktop/scratch/samplepartitionedparquet/Date=2017-09-03/Query=donde/ColA=12/ColB=pirz/ColC=Hello/f1.parquet"
    // val s = p.split("/").map { kv => kv.split("=", 2) }
    //val p = "Date=2017-09-03/Query=donde"
    //val x = PartitioningUtils.parsePathFragmentAsSeq(p)


    val x = p.split("/").map { kv =>
      kv.split("=", 2)
    }.filter(k => pCols.contains(k(0))).map(t => (t(0), t(1)))
    x.foreach(t => println(s"${t._1} , ${t._2}"))

    //    spark.conf.set(IndexConstants.REFRESH_DELETE_ENABLED, "true")
    //    hyperspace.createIndex(dataDF, indexConfig)
    //
    //    FileUtils.delete(new Path(s"$sampleDataLocation/$partitionKey1=2018-09-03"))
    //
    //    hyperspace.refreshIndex(indexConfig.indexName)

    // scalastyle:on
  }
}
