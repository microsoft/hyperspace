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

import com.microsoft.hyperspace.index.{FileInfo, IndexConstants}
import com.microsoft.hyperspace.index.rules.HyperspaceRuleTestSuite
import com.microsoft.hyperspace.util.FileUtils

class JoinIndexRankerTest extends HyperspaceRuleTestSuite {
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

  before {
    spark.conf.set(IndexConstants.INDEX_HYBRID_SCAN_ENABLED, "false")
  }

  after {
    spark.conf.set(IndexConstants.INDEX_HYBRID_SCAN_ENABLED, "false")
  }

  val t1c1 = AttributeReference("t1c1", IntegerType)()
  val t1c2 = AttributeReference("t1c2", StringType)()
  val t2c1 = AttributeReference("t2c1", IntegerType)()
  val t2c2 = AttributeReference("t2c2", StringType)()

  test("rank() should prefer equal-bucket index pairs over unequal-bucket.") {
    val l_10 = createIndexLogEntry("l1", Seq(t1c1), Seq(t1c2), dummy, 10, writeLog = false)
    val l_20 = createIndexLogEntry("l2", Seq(t1c1), Seq(t1c2), dummy, 20, writeLog = false)
    val r_20 = createIndexLogEntry("r1", Seq(t2c1), Seq(t2c2), dummy, 20, writeLog = false)

    val indexPairs = Seq((l_10, r_20), (l_20, r_20))

    val expectedOrder = Seq((l_20, r_20), (l_10, r_20))
    val actualOrder = JoinIndexRanker.rank(spark, dummy, dummy, indexPairs)
    assert(actualOrder.equals(expectedOrder))
  }

  test(
    "rank() should prefer higher number of buckets if multiple equal-bucket index pairs found.") {
    val l_10 = createIndexLogEntry("l1", Seq(t1c1), Seq(t1c2), dummy, 10, writeLog = false)
    val l_20 = createIndexLogEntry("l2", Seq(t1c1), Seq(t1c2), dummy, 20, writeLog = false)
    val r_10 = createIndexLogEntry("r1", Seq(t2c1), Seq(t2c2), dummy, 10, writeLog = false)
    val r_20 = createIndexLogEntry("r2", Seq(t2c1), Seq(t2c2), dummy, 20, writeLog = false)

    val indexPairs = Seq((l_10, r_10), (l_10, r_20), (l_20, r_20))

    val expectedOrder = Seq((l_20, r_20), (l_10, r_10), (l_10, r_20))
    val actualOrder = JoinIndexRanker.rank(spark, dummy, dummy, indexPairs)
    assert(actualOrder.equals(expectedOrder))
  }

  test("rank() should prefer the largest common bytes if HybridScan is enabled.") {
    val fileList1 = Seq(FileInfo("a", 1, 1, 1), FileInfo("b", 2, 1, 2))
    val fileList2 = Seq(FileInfo("c", 1, 1, 3), FileInfo("d", 1, 1, 4))
    val l_10 = createIndexLogEntry("l1", Seq(t1c1), Seq(t1c2), dummy, 10, fileList1, false)
    val l_20 = createIndexLogEntry("l2", Seq(t1c1), Seq(t1c2), dummy, 20, fileList2, false)
    val r_10 = createIndexLogEntry("r1", Seq(t2c1), Seq(t2c2), dummy, 10, fileList1, false)
    val r_20 = createIndexLogEntry("r2", Seq(t2c1), Seq(t2c2), dummy, 20, fileList2, false)

    {
      // Test rank algorithm without Hybrid Scan.
      val indexPairs = Seq((l_10, r_10), (l_10, r_20), (l_20, r_20))
      val expectedOrder = Seq((l_20, r_20), (l_10, r_10), (l_10, r_20))
      val actualOrder = JoinIndexRanker.rank(spark, dummy, dummy, indexPairs)
      assert(actualOrder.equals(expectedOrder))
    }

    {
      // Test if hybridScanEnabled is true.
      val indexPairs = Seq((l_10, r_10), (l_10, r_20), (l_20, r_20))
      val expectedOrder = Seq((l_10, r_10), (l_20, r_20), (l_10, r_20))
      spark.conf.set(IndexConstants.INDEX_HYBRID_SCAN_ENABLED, "true")
      val actualOrder = JoinIndexRanker.rank(spark, dummy, dummy, indexPairs)
      assert(actualOrder.equals(expectedOrder))
    }

    {
      // If both indexes have the same number of source files, follow the original algorithm.
      val l_10 = createIndexLogEntry("l1", Seq(t1c1), Seq(t1c2), dummy, 10, fileList1, false)
      val l_20 = createIndexLogEntry("l2", Seq(t1c1), Seq(t1c2), dummy, 20, fileList1, false)
      val r_10 = createIndexLogEntry("r1", Seq(t2c1), Seq(t2c2), dummy, 10, fileList1, false)
      val r_20 = createIndexLogEntry("r2", Seq(t2c1), Seq(t2c2), dummy, 20, fileList1, false)

      val indexPairs = Seq((l_10, r_10), (l_10, r_20), (l_20, r_20))
      val expectedOrder = Seq((l_20, r_20), (l_10, r_10), (l_10, r_20))
      val actualOrder = JoinIndexRanker.rank(spark, dummy, dummy, indexPairs)
      assert(actualOrder.equals(expectedOrder))
    }
  }
}
