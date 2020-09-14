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

package com.microsoft.hyperspace.index.rankers

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{IntegerType, StringType}

import com.microsoft.hyperspace.index.{FileInfo, HyperspaceSuite}
import com.microsoft.hyperspace.util.FileUtils

class FilterIndexRankerTest extends HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/FilterRankerTest")
  var dummy: LogicalPlan = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.createFile(
      systemPath.getFileSystem(new Configuration),
      new Path(systemPath, "dummy"),
      "dummy string")
    val df = spark.read.text(systemPath.toString)
    dummy = df.queryExecution.optimizedPlan
  }

  val t1c1 = AttributeReference("t1c1", IntegerType)()
  val t1c2 = AttributeReference("t1c2", StringType)()
  val t2c1 = AttributeReference("t2c1", IntegerType)()
  val t2c2 = AttributeReference("t2c2", StringType)()

  test("rank() should return the head of the list by default") {
    val ind1 = RankerTestHelper.createIndex("ind1", Seq(t1c1), Seq(t1c2))
    val ind2 = RankerTestHelper.createIndex("ind2", Seq(t1c1), Seq(t1c2))
    val ind3 = RankerTestHelper.createIndex("ind3", Seq(t2c1), Seq(t2c2))

    val indexes = Seq(ind1, ind2, ind3)
    assert(FilterIndexRanker.rank(dummy, indexes, hybridScanEnabled = false).get.equals(ind1))
  }

  test(
    "rank() should return the index with the largest common bytes of source files" +
      "if HybridScan is enabled") {

    val fileList1 = Seq(FileInfo("a", 1, 1), FileInfo("b", 1, 1))
    val fileList2 = Seq(FileInfo("c", 1, 1), FileInfo("d", 1, 1))

    val ind1 =
      RankerTestHelper.createIndex(
        "ind1",
        Seq(t1c1),
        Seq(t1c2),
        inputFiles = fileList1,
        plan = dummy)
    val ind2 = RankerTestHelper.createIndex(
      "ind2",
      Seq(t1c1),
      Seq(t1c2),
      inputFiles = fileList1 ++ fileList2,
      plan = dummy)
    val ind3 = RankerTestHelper.createIndex(
      "ind3",
      Seq(t2c1),
      Seq(t1c2),
      inputFiles = fileList2,
      plan = dummy)

    val indexes = Seq(ind1, ind2, ind3)
    assert(FilterIndexRanker.rank(dummy, indexes, hybridScanEnabled = true).get === ind2)
    assert(FilterIndexRanker.rank(dummy, indexes, hybridScanEnabled = false).get === ind1)
  }
}
