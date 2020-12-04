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

package com.microsoft.hyperspace.index.rules

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, IsNotNull}
import org.apache.spark.sql.catalyst.plans.{JoinType, SQLHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, NoopCache}
import org.apache.spark.sql.types.{IntegerType, StringType}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{FileInfo, IndexCollectionManager, IndexConfig, IndexConstants, IndexLogEntryTags, LogicalPlanFingerprint, Signature}
import com.microsoft.hyperspace.index.IndexConstants.INDEX_HYBRID_SCAN_ENABLED
import com.microsoft.hyperspace.util.{FileUtils, PathUtils}

class RuleUtilsTest extends HyperspaceRuleTestSuite with SQLHelper {
  override val systemPath = PathUtils.makeAbsolute("src/test/resources/ruleUtilsTest")

  val t1c1 = AttributeReference("t1c1", IntegerType)()
  val t1c2 = AttributeReference("t1c2", StringType)()
  val t1c3 = AttributeReference("t1c3", IntegerType)()
  val t1c4 = AttributeReference("t1c4", StringType)()
  val t2c1 = AttributeReference("t2c1", IntegerType)()
  val t2c2 = AttributeReference("t2c2", StringType)()
  val t2c3 = AttributeReference("t2c3", IntegerType)()
  val t2c4 = AttributeReference("t2c4", StringType)()

  val t1Schema = schemaFromAttributes(t1c1, t1c2, t1c3, t1c4)
  val t2Schema = schemaFromAttributes(t2c1, t2c2, t2c3, t2c4)

  var t1Relation: HadoopFsRelation = _
  var t2Relation: HadoopFsRelation = _
  var t1ScanNode: LogicalRelation = _
  var t2ScanNode: LogicalRelation = _
  var t1FilterNode: Filter = _
  var t2FilterNode: Filter = _
  var t1ProjectNode: Project = _
  var t2ProjectNode: Project = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val t1Location =
      new InMemoryFileIndex(spark, Seq(new Path("t1")), Map.empty, Some(t1Schema), NoopCache)
    val t2Location =
      new InMemoryFileIndex(spark, Seq(new Path("t2")), Map.empty, Some(t2Schema), NoopCache)

    t1Relation = baseRelation(t1Location, t1Schema)
    t2Relation = baseRelation(t2Location, t2Schema)

    t1ScanNode = LogicalRelation(t1Relation, Seq(t1c1, t1c2, t1c3, t1c4), None, false)
    t2ScanNode = LogicalRelation(t2Relation, Seq(t2c1, t2c2, t2c3, t2c4), None, false)

    t1FilterNode = Filter(IsNotNull(t1c1), t1ScanNode)
    t2FilterNode = Filter(IsNotNull(t2c1), t2ScanNode)

    t1ProjectNode = Project(Seq(t1c1, t1c3), t1FilterNode)
    // Project [t1c1#0, t1c3#2]
    //  +- Filter isnotnull(t1c1#0)
    //   +- Relation[t1c1#0,t1c2#1,t1c3#2,t1c4#3] parquet

    t2ProjectNode = Project(Seq(t2c1, t2c3), t2FilterNode)
    // Project [t2c1#4, t2c3#6]
    //  +- Filter isnotnull(t2c1#4)
    //   +- Relation[t2c1#4,t2c2#5,t2c3#6,t2c4#7] parquet

    createIndexLogEntry("t1i1", Seq(t1c1), Seq(t1c3), t1ProjectNode)
    createIndexLogEntry("t1i2", Seq(t1c1, t1c2), Seq(t1c3), t1ProjectNode)
    createIndexLogEntry("t1i3", Seq(t1c2), Seq(t1c3), t1ProjectNode)
    createIndexLogEntry("t2i1", Seq(t2c1), Seq(t2c3), t2ProjectNode)
    createIndexLogEntry("t2i2", Seq(t2c1, t2c2), Seq(t2c3), t2ProjectNode)
  }

  test("Verify indexes are matched by signature correctly.") {
    val indexManager = IndexCollectionManager(spark)
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    assert(RuleUtils.getCandidateIndexes(spark, allIndexes, t1ProjectNode).length === 3)
    assert(RuleUtils.getCandidateIndexes(spark, allIndexes, t2ProjectNode).length === 2)

    // Delete an index for t1ProjectNode
    indexManager.delete("t1i1")
    val allIndexes2 = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    assert(RuleUtils.getCandidateIndexes(spark, allIndexes2, t1ProjectNode).length === 2)
  }

  test("Verify get logical relation for single logical relation node plan.") {
    validateLogicalRelation(t1ScanNode, t1ScanNode)
  }

  test("Verify get logical relation for multi-node linear plan.") {
    validateLogicalRelation(t1ProjectNode, t1ScanNode)
  }

  test("Verify get logical relation for non-linear plan.") {
    val joinNode = Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), None)
    val r = RuleUtils.getLogicalRelation(Project(Seq(t1c3, t2c3), joinNode))
    assert(r.isEmpty)
  }

  test("Verify getCandidateIndex for hybrid scan") {
    withTempPath { tempPath =>
      val indexManager = IndexCollectionManager(spark)
      val df = spark.range(1, 5).toDF("id")
      val dataPath = tempPath.getAbsolutePath
      df.write.parquet(dataPath)

      withIndex("index1") {
        val readDf = spark.read.parquet(dataPath)
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
          indexManager.create(readDf, IndexConfig("index1", Seq("id")))
        }
        val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

        def verify(
            plan: LogicalPlan,
            hybridScanEnabled: Boolean,
            hybridScanDeleteEnabled: Boolean,
            expectCandidateIndex: Boolean,
            expectedHybridScanTag: Option[Boolean]): Unit = {
          withSQLConf(
            "spark.hyperspace.index.hybridscan.enabled" -> hybridScanEnabled.toString,
            "spark.hyperspace.index.hybridscan.delete.enabled" ->
              hybridScanDeleteEnabled.toString) {
            val indexes = RuleUtils
              .getCandidateIndexes(spark, allIndexes, plan)
            if (expectCandidateIndex) {
              assert(indexes.length === 1)
              assert(indexes.head.name === "index1")
              assert(
                indexes.head.getTagValue(plan, IndexLogEntryTags.HYBRIDSCAN_REQUIRED)
                  === expectedHybridScanTag)
            } else {
              assert(indexes.isEmpty)
            }
          }
        }

        // Verify that a candidate index is returned with the unmodified data files whether
        // hybrid scan is enabled or not.
        {
          val optimizedPlan = spark.read.parquet(dataPath).queryExecution.optimizedPlan
          verify(
            optimizedPlan,
            hybridScanEnabled = false,
            hybridScanDeleteEnabled = false,
            expectCandidateIndex = true,
            expectedHybridScanTag = None)
          verify(
            optimizedPlan,
            hybridScanEnabled = true,
            hybridScanDeleteEnabled = false,
            expectCandidateIndex = true,
            expectedHybridScanTag = Some(false))
        }

        // Scenario #1: Append new files.
        df.write.mode("append").parquet(dataPath)

        {
          val optimizedPlan = spark.read.parquet(dataPath).queryExecution.optimizedPlan
          verify(
            optimizedPlan,
            hybridScanEnabled = false,
            hybridScanDeleteEnabled = false,
            expectCandidateIndex = false,
            expectedHybridScanTag = None)
          verify(
            optimizedPlan,
            hybridScanEnabled = true,
            hybridScanDeleteEnabled = false,
            expectCandidateIndex = true,
            expectedHybridScanTag = Some(true))
        }

        // Scenario #2: Delete 1 file.
        FileUtils.delete(new Path(readDf.inputFiles.head))

        {
          val optimizedPlan = spark.read.parquet(dataPath).queryExecution.optimizedPlan
          verify(
            optimizedPlan,
            hybridScanEnabled = false,
            hybridScanDeleteEnabled = false,
            expectCandidateIndex = false,
            expectedHybridScanTag = None)
          verify(
            optimizedPlan,
            hybridScanEnabled = true,
            hybridScanDeleteEnabled = false,
            expectCandidateIndex = false,
            expectedHybridScanTag = None)
          verify(
            optimizedPlan,
            hybridScanEnabled = true,
            hybridScanDeleteEnabled = true,
            expectCandidateIndex = true,
            expectedHybridScanTag = Some(true))
        }

        // Scenario #3: Replace all files.
        df.write.mode("overwrite").parquet(dataPath)

        {
          val optimizedPlan = spark.read.parquet(dataPath).queryExecution.optimizedPlan
          verify(
            optimizedPlan,
            hybridScanEnabled = false,
            hybridScanDeleteEnabled = false,
            expectCandidateIndex = false,
            expectedHybridScanTag = None)
          verify(
            optimizedPlan,
            hybridScanEnabled = true,
            hybridScanDeleteEnabled = true,
            expectCandidateIndex = false,
            expectedHybridScanTag = None)
        }
      }
    }
  }

  test("Verify the location of injected shuffle for Hybrid Scan.") {
    withTempPath { tempPath =>
      val dataPath = tempPath.getAbsolutePath
      import spark.implicits._
      Seq((1, "name1", 12), (2, "name2", 10))
        .toDF("id", "name", "age")
        .write
        .mode("overwrite")
        .parquet(dataPath)

      val df = spark.read.parquet(dataPath)
      val query = df.filter(df("id") >= 3).select("id", "name")
      val bucketSpec = BucketSpec(100, Seq("id"), Seq())
      val shuffled = RuleUtils.transformPlanToShuffleUsingBucketSpec(
        bucketSpec,
        query.queryExecution.optimizedPlan)

      // Plan: Project ("id", "name") -> Filter ("id") -> Relation
      // should be transformed to:
      //   Shuffle ("id") -> Project("id", "name") -> Filter ("id") -> Relation
      assert(shuffled.collect {
        case RepartitionByExpression(attrs, p: Project, numBuckets) =>
          assert(numBuckets == 100)
          assert(attrs.size == 1)
          assert(attrs.head.asInstanceOf[Attribute].name.contains("id"))
          assert(
            p.projectList.exists(_.name.equals("id")) && p.projectList.exists(
              _.name.equals("name")))
          true
      }.length == 1)

      // Check if the shuffle node should be injected where all bucket columns
      // are available as its input.
      // For example,
      // Plan: Project ("id", "name") -> Filter ("id") -> Relation
      // should be transformed:
      //   Project ("id", "name") -> Shuffle ("age") -> Filter ("id") -> Relation
      // , NOT:
      //   Shuffle ("age") -> Project("id", "name") -> Filter ("id") -> Relation
      // since Project doesn't include "age" column; Shuffle will be RoundRobinPartitioning

      val bucketSpec2 = BucketSpec(100, Seq("age"), Seq())
      val query2 = df.filter(df("id") <= 3).select("id", "name")
      val shuffled2 =
        RuleUtils.transformPlanToShuffleUsingBucketSpec(
          bucketSpec2,
          query2.queryExecution.optimizedPlan)
      assert(shuffled2.collect {
        case Project(_, RepartitionByExpression(attrs, _: Filter, numBuckets)) =>
          assert(numBuckets == 100)
          assert(attrs.size == 1)
          assert(attrs.head.asInstanceOf[Attribute].name.contains("age"))
          true
      }.length == 1)
    }
  }

  private def validateLogicalRelation(plan: LogicalPlan, expected: LogicalRelation): Unit = {
    val r = RuleUtils.getLogicalRelation(plan)
    assert(r.isDefined)
    assert(r.get.equals(expected))
  }
}
