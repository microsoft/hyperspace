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

package com.microsoft.hyperspace.index.covering

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{IntegerType, StringType}

import com.microsoft.hyperspace.index.{FileInfo, HyperspaceRuleSuite, IndexConstants}
import com.microsoft.hyperspace.util.FileUtils

class FilterIndexRankerTest extends HyperspaceRuleSuite {
  override val indexLocationDirName = "FilterRankerTest"
  var tempPlan: LogicalPlan = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.createFile(
      systemPath.getFileSystem(new Configuration),
      new Path(systemPath, "dummy"),
      "dummy string")
    val df = spark.read.text(systemPath.toString)
    tempPlan = df.queryExecution.optimizedPlan
  }

  val t1c1 = AttributeReference("t1c1", IntegerType)()
  val t1c2 = AttributeReference("t1c2", StringType)()
  val t2c1 = AttributeReference("t2c1", IntegerType)()
  val t2c2 = AttributeReference("t2c2", StringType)()

  test("rank() should return the index with smallest size by default.") {
    // Index with only 1 file of size 20
    val ind1 = createIndexLogEntry(
      "ind1",
      Seq(t1c1),
      Seq(t1c2),
      tempPlan,
      writeLog = false,
      filenames = Seq("f1.parquet", "f2.parquet"))

    // Index with only 2 files of total size 10
    val ind2 = createIndexLogEntry(
      "ind2",
      Seq(t1c1),
      Seq(t1c2),
      tempPlan,
      writeLog = false,
      filenames = Seq("f1.parquet"))

    // Index with only 3 files of total size 30
    val ind3 = createIndexLogEntry(
      "ind3",
      Seq(t2c1),
      Seq(t2c2),
      tempPlan,
      writeLog = false,
      filenames = Seq("f1.parquet", "f2.parquet", "f3.parquet"))

    val indexes = Seq(ind1, ind2, ind3)
    assert(FilterIndexRanker.rank(spark, tempPlan, indexes).get.equals(ind2))
  }

  test(
    "rank() should return the index with the largest common bytes of source files " +
      "if HybridScan is enabled.") {

    val fileList1 = Seq(FileInfo("a", 1, 1, 1), FileInfo("b", 1, 1, 3))
    val fileList2 = Seq(FileInfo("c", 1, 1, 2), FileInfo("d", 1, 1, 4))

    val ind1 = createIndexLogEntry(
      "ind1",
      Seq(t1c1),
      Seq(t1c2),
      tempPlan,
      inputFiles = fileList1,
      writeLog = false)
    setCommonSourceSizeInBytesTag(ind1, tempPlan, fileList1)
    val ind2 = createIndexLogEntry(
      "ind2",
      Seq(t1c1),
      Seq(t1c2),
      tempPlan,
      inputFiles = fileList1 ++ fileList2,
      writeLog = false)
    setCommonSourceSizeInBytesTag(ind2, tempPlan, fileList1 ++ fileList2)
    val ind3 = createIndexLogEntry(
      "ind3",
      Seq(t2c1),
      Seq(t1c2),
      tempPlan,
      inputFiles = fileList2,
      writeLog = false)
    setCommonSourceSizeInBytesTag(ind3, tempPlan, fileList2)

    val indexes = Seq(ind1, ind2, ind3)

    spark.conf.set(IndexConstants.INDEX_HYBRID_SCAN_ENABLED, "true")
    assert(FilterIndexRanker.rank(spark, tempPlan, indexes).get === ind2)
    spark.conf.set(IndexConstants.INDEX_HYBRID_SCAN_ENABLED, "false")
    assert(FilterIndexRanker.rank(spark, tempPlan, indexes).get === ind1)
  }
}
