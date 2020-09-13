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
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.catalog.BucketSpec

import com.microsoft.hyperspace.index.execution.{BucketUnionExec, BucketUnionRDD, BucketUnionRDDPartition, BucketUnionStrategy}
import com.microsoft.hyperspace.index.plans.logical.BucketUnion

class BucketUnionTest extends HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/bucketUnionTest")

  test("BucketUnion test for operator pre-requisites") {
    import spark.implicits._
    val df1 = Seq((1, "name1"), (2, "name2")).toDF("id", "name")
    val df1_1 = Seq((1, "name1"), (2, "name2")).toDF("id", "name")
    val df1_2 = Seq(("name1", 1), ("name2", 2)).toDF("name", "id")
    val df2 = Seq((1, "name1", 20), (2, "name2", 10)).toDF("id", "name", "age")

    // different column schema
    intercept[IllegalArgumentException] {
      BucketUnion(
        Seq(df1.queryExecution.optimizedPlan, df2.queryExecution.optimizedPlan),
        BucketSpec(1, Seq(), Seq()))
    }

    // different order of columns
    intercept[IllegalArgumentException] {
      BucketUnion(
        Seq(df1.queryExecution.optimizedPlan, df1_2.queryExecution.optimizedPlan),
        BucketSpec(1, Seq(), Seq()))
    }

    BucketUnion(
      Seq(df1.queryExecution.optimizedPlan, df1_1.queryExecution.optimizedPlan),
      BucketSpec(1, Seq(), Seq()))
  }

  test("BucketUnionStrategy test if strategy introduces BucketUnionExec in the Spark Plan") {
    import spark.implicits._
    val df1 = Seq((1, "name1"), (2, "name2")).toDF("id", "name")
    val df1_1 = Seq((1, "name1"), (2, "name2")).toDF("id", "name")
    val bucket = BucketUnion(
      Seq(df1.queryExecution.optimizedPlan, df1_1.queryExecution.optimizedPlan),
      BucketSpec(1, Seq(), Seq()))

    assert(BucketUnionStrategy(bucket).collect {
      case BucketUnionExec(_, _) => true
    }.length == 1)

    assert(BucketUnionStrategy(df1.queryExecution.optimizedPlan).collect {
      case BucketUnionExec(_, _) => true
    }.isEmpty)
  }

  test("BucketUnionExec test") {
    import spark.implicits._
    val df1 = Seq((1, "name1"), (2, "name2")).toDF("id", "name")
    val p1 = df1.repartition(10)
    val df2 = Seq((1, "name1"), (2, "name2")).toDF("id", "name")
    val p2_1 = df2.repartition(9)
    val p2_2 = df2.repartition(10)

    // different number of partition
    intercept[AssertionError] {
      val bucket = BucketUnion(
        Seq(p1.queryExecution.optimizedPlan, p2_1.queryExecution.optimizedPlan),
        BucketSpec(10, Seq(), Seq()))
      spark.sessionState.executePlan(bucket).sparkPlan
    }

    val bucket = BucketUnion(
      Seq(p1.queryExecution.optimizedPlan, p2_2.queryExecution.optimizedPlan),
      BucketSpec(10, Seq(), Seq()))

    assert(BucketUnionStrategy(bucket).collect {
      case p: BucketUnionExec =>
        assert(p.bucketSpec.numBuckets == 10)
        assert(p.children.length == 2)
        assert(p.output.length == p1.schema.fields.length)
      true
    }.length == 1)
  }

  test("BucketUnionRDD test that partition columns with same value fall in the same partition") {
    import spark.implicits._
    val df1 = Seq((2, "name1"), (3, "name2")).toDF("id", "name")
    val p1 = df1.repartition(10, $"id")
    val df2 = Seq((2, "name1"), (3, "name2")).toDF("id", "name")
    val p2 = df2.repartition(10, $"id")
    val bucketSpec = BucketSpec(10, Seq("id"), Seq())

    val rdd = new BucketUnionRDD[Row](spark.sparkContext, Seq(p1.rdd, p2.rdd), bucketSpec)
    assert(rdd.getPartitions.length == 10)
    assert(rdd.collect.length == 4)
    assert(rdd.partitions.head.isInstanceOf[BucketUnionRDDPartition])

    val partitionSum: Seq[Int] = rdd
      .mapPartitions(it => Iterator.single(it.map(r => r.getInt(0)).sum))
      .collect()
      .toSeq

    // since rows with id=2 assigned to the same partition and likewise rows with id=3,
    // summation of each partition should be one of [0, 4, 6, 10]
    val availableSum = Set(0, 4, 6, 10)
    assert(partitionSum.forall(p => availableSum.contains(p)))
  }
}
