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

class JoinIndexRankerTest extends HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/JoinRankerTest")
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

  test("rank() should prefer equal-bucket index pairs over unequal-bucket") {
    val l_10 =
      RankerTestHelper.createIndex("l1", Seq(t1c1), Seq(t1c2), numBuckets = 10, plan = dummy)
    val l_20 =
      RankerTestHelper.createIndex("l2", Seq(t1c1), Seq(t1c2), numBuckets = 20, plan = dummy)
    val r_20 =
      RankerTestHelper.createIndex("r1", Seq(t2c1), Seq(t2c2), numBuckets = 20, plan = dummy)

    val indexPairs = Seq((l_10, r_20), (l_20, r_20))

    val expectedOrder = Seq((l_20, r_20), (l_10, r_20))
    val actualOrder =
      JoinIndexRanker.rank(dummy, dummy, indexPairs, hybridScanEnabled = false)
    assert(actualOrder.equals(expectedOrder))
  }

  test("rank() should prefer higher number of buckets if multiple equal-bucket index pairs found") {
    val l_10 = RankerTestHelper.createIndex("l1", Seq(t1c1), Seq(t1c2), 10, plan = dummy)
    val l_20 = RankerTestHelper.createIndex("l2", Seq(t1c1), Seq(t1c2), 20, plan = dummy)
    val r_10 = RankerTestHelper.createIndex("r1", Seq(t2c1), Seq(t2c2), 10, plan = dummy)
    val r_20 = RankerTestHelper.createIndex("r2", Seq(t2c1), Seq(t2c2), 20, plan = dummy)

    val indexPairs = Seq((l_10, r_10), (l_10, r_20), (l_20, r_20))

    val expectedOrder = Seq((l_20, r_20), (l_10, r_10), (l_10, r_20))
    val actualOrder =
      JoinIndexRanker.rank(dummy, dummy, indexPairs, hybridScanEnabled = false)
    assert(actualOrder.equals(expectedOrder))
  }

  test("rank() should prefer the largest common bytes if HybridScan is enabled") {
    val fileList1 = Seq(FileInfo("a", 1, 1, 1), FileInfo("b", 2, 1, 2))
    val fileList2 = Seq(FileInfo("c", 1, 1, 3), FileInfo("d", 1, 1, 4))
    val l_10 = RankerTestHelper.createIndex("l1", Seq(t1c1), Seq(t1c2), 10, fileList1, dummy)
    val l_20 = RankerTestHelper.createIndex("l2", Seq(t1c1), Seq(t1c2), 20, fileList2, dummy)
    val r_10 = RankerTestHelper.createIndex("r1", Seq(t2c1), Seq(t2c2), 10, fileList1, dummy)
    val r_20 = RankerTestHelper.createIndex("r2", Seq(t2c1), Seq(t2c2), 20, fileList2, dummy)

    {
      // Test original algorithm.
      val indexPairs = Seq((l_10, r_10), (l_10, r_20), (l_20, r_20))
      val expectedOrder = Seq((l_20, r_20), (l_10, r_10), (l_10, r_20))
      val actualOrder =
        JoinIndexRanker.rank(dummy, dummy, indexPairs, hybridScanEnabled = false)
      assert(actualOrder.equals(expectedOrder))
    }

    {
      // Test if hybridScanEnabled is true.
      val indexPairs = Seq((l_10, r_10), (l_10, r_20), (l_20, r_20))
      val expectedOrder = Seq((l_10, r_10), (l_20, r_20), (l_10, r_20))
      val actualOrder =
        JoinIndexRanker.rank(dummy, dummy, indexPairs, hybridScanEnabled = true)
      assert(actualOrder.equals(expectedOrder))
    }

    {
      // If both indexes have the same number of source files, follow the original algorithm.
      val l_10 = RankerTestHelper.createIndex("l1", Seq(t1c1), Seq(t1c2), 10, fileList1, dummy)
      val l_20 = RankerTestHelper.createIndex("l2", Seq(t1c1), Seq(t1c2), 20, fileList1, dummy)
      val r_10 = RankerTestHelper.createIndex("r1", Seq(t2c1), Seq(t2c2), 10, fileList1, dummy)
      val r_20 = RankerTestHelper.createIndex("r2", Seq(t2c1), Seq(t2c2), 20, fileList1, dummy)

      val indexPairs = Seq((l_10, r_10), (l_10, r_20), (l_20, r_20))
      val expectedOrder = Seq((l_20, r_20), (l_10, r_10), (l_10, r_20))
      val actualOrder =
        JoinIndexRanker.rank(dummy, dummy, indexPairs, hybridScanEnabled = true)
      assert(actualOrder.equals(expectedOrder))
    }
  }
}
