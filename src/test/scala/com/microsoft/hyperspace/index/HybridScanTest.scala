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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.execution.datasources._

import com.microsoft.hyperspace.{Hyperspace, SampleData}
import com.microsoft.hyperspace.index.Content.Directory.FileInfo
import com.microsoft.hyperspace.index.rules.{FilterIndexRule, JoinIndexRule}
import com.microsoft.hyperspace.util.FileUtils

class HybridScanTest extends HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/hybridScanTest")

  private val sampleData = SampleData.testData
  private val sampleParquetDataLocation = "src/test/resources/sampleparquet2"
  private var df: DataFrame = _
  private var hyperspace: Hyperspace = _
  private var files: Seq[FileInfo] = _
  private val indexConfig1 = IndexConfig("index1", Seq("clicks"), Seq("query"))

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sparkSession = spark
    import sparkSession.implicits._
    hyperspace = new Hyperspace(spark)
    FileUtils.delete(new Path(sampleParquetDataLocation))
    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    dfFromSample.write.parquet(sampleParquetDataLocation)

    df = spark.read.parquet(sampleParquetDataLocation)
    hyperspace.createIndex(df, indexConfig1)

    files = df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(
          HadoopFsRelation(
            location: PartitioningAwareFileIndex,
            _,
            dataSchema,
            _,
            fileFormat,
            options),
          _,
          _,
          _) =>
        location.allFiles.map(FileInfo(_))
    }.flatten
    val sourcePath = new Path(files.head.name)
    val destPath = new Path(files.head.name + ".copy")
    sourcePath.getFileSystem(new Configuration).copyToLocalFile(sourcePath, destPath)
  }

  before {}

  after {}

  override def afterAll(): Unit = {
    super.afterAll()
    spark.sessionState.conf.unsetConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED)
    spark.sessionState.conf.unsetConf("spark.sql.autoBroadcastJoinThreshold")
    FileUtils.delete(new Path(sampleParquetDataLocation))
  }

  test("HybridScan filter rule test") {
    // append data by copying
    spark.conf.set("spark.hyperspace.index.hybridscan.enabled", false)

    df = spark.read.parquet(sampleParquetDataLocation)
    val query = df.filter(df("clicks") <= 2000).select(df("query"))
    val transformed = FilterIndexRule(query.queryExecution.optimizedPlan)
    assert(transformed.equals(query.queryExecution.optimizedPlan), "No plan transformation.")

    spark.conf.set("spark.hyperspace.index.hybridscan.enabled", true)
    val planWithHybridScan = FilterIndexRule(query.queryExecution.optimizedPlan)
    assert(
      !planWithHybridScan.equals(query.queryExecution.optimizedPlan),
      "Plan should be transformed.")
  }

  test("HybridScan join rule test") {
    // append data by copying
    spark.conf.set("spark.hyperspace.index.hybridscan.enabled", false)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val sourcePath = new Path(files.head.name)
    val destPath = new Path(files.head.name.replace("part-00000", "part-00100"))
    sourcePath.getFileSystem(new Configuration).copyToLocalFile(sourcePath, destPath)

    df = spark.read.parquet(sampleParquetDataLocation)
    val query = df.filter(df("clicks") <= 2000).select(df("clicks"), df("query"))
    val query2 = df.filter(df("clicks") >= 4000).select(df("clicks"), df("query"))
    val join = query.join(query2, "clicks")
    val transformed = JoinIndexRule(join.queryExecution.optimizedPlan)
    assert(transformed.equals(join.queryExecution.optimizedPlan), "No plan transformation.")

    spark.conf.set("spark.hyperspace.index.hybridscan.enabled", true)
    val planWithHybridScan = JoinIndexRule(join.queryExecution.optimizedPlan)
    assert(
      !planWithHybridScan.equals(join.queryExecution.optimizedPlan),
      "Plan should be transformed.")
  }
}
