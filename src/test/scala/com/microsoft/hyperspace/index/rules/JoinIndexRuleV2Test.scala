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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.{QueryTest, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.util.{FileUtils, PathUtils}

class JoinIndexRuleV2Test extends QueryTest with HyperspaceRuleTestSuite {
  val systemPath = PathUtils.makeAbsolute("src/test/resources/joinIndexRuleTest")

  val t1c1 = AttributeReference("t1c1", IntegerType)()
  val t1c2 = AttributeReference("t1c2", StringType)()
  val t1c3 = AttributeReference("t1c3", IntegerType)()
  val t1c4 = AttributeReference("t1c4", StringType)()
  val t2c1 = AttributeReference("t2c1", IntegerType)()
  val t2c2 = AttributeReference("t2c2", StringType)()
  val t2c3 = AttributeReference("t2c3", IntegerType)()
  val t2c4 = AttributeReference("t2c4", StringType)()
  val t3c1 = AttributeReference("t3c1", IntegerType)()
  val t3c2 = AttributeReference("t3c2", StringType)()
  val t3c3 = AttributeReference("t3c3", IntegerType)()
  val t3c4 = AttributeReference("t3c4", StringType)()

  val t1Schema = schemaFromAttributes(t1c1, t1c2, t1c3, t1c4)
  val t2Schema = schemaFromAttributes(t2c1, t2c2, t2c3, t2c4)
  val t3Schema = schemaFromAttributes(t3c1, t3c2, t3c3, t3c4)

  var t1Relation: HadoopFsRelation = _
  var t2Relation: HadoopFsRelation = _
  var t3Relation: HadoopFsRelation = _
  var t1ScanNode: LogicalRelation = _
  var t2ScanNode: LogicalRelation = _
  var t3ScanNode: LogicalRelation = _
  var t1FilterNode: Filter = _
  var t2FilterNode: Filter = _
  var t3FilterNode: Filter = _
  var t1ProjectNode: Project = _
  var t2ProjectNode: Project = _
  var t3ProjectNode: Project = _

  /**
   * Test Setup:
   *
   * The basic scenario tested here is a [[Join]] logical plan node which consists of two children
   * Left child is a [[Project]] -> [[Filter]] -> [[LogicalRelation]] subplan which reads data
   * from files on disk.
   * Right child is also a [[Project]] -> [[Filter]] -> [[LogicalRelation]] subplan same as left.
   *
   * If the Join node satisfies the requirements for [[JoinIndexRuleV2]], the plan must get updated
   * to use available indexes from the system. If not, the plan should remain unaffected on
   * application of the rule.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.delete(systemPath)

    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, systemPath.toUri.toString)

    val t1Location =
      new InMemoryFileIndex(spark, Seq(new Path("t1")), Map.empty, Some(t1Schema), NoopCache) {
        // Update allFiles() so that length of files is greater than spark's broadcast threshold.
        override def allFiles(): Seq[FileStatus] = {
          val length = spark.conf
            .get("spark.sql.autoBroadcastJoinThreshold")
            .toLong * 10
          Seq(new FileStatus(length, false, 1, 100, 100, new Path("t1")))
        }
      }

    val t2Location =
      new InMemoryFileIndex(spark, Seq(new Path("t2")), Map.empty, Some(t1Schema), NoopCache) {
        // Update allFiles() so that length of files is greater than spark's broadcast threshold.
        override def allFiles(): Seq[FileStatus] = {
          val length = spark.conf
            .get("spark.sql.autoBroadcastJoinThreshold")
            .toLong * 10
          Seq(new FileStatus(length, false, 1, 100, 100, new Path("t2")))
        }
      }

    val t3Location =
      new InMemoryFileIndex(spark, Seq(new Path("t3")), Map.empty, Some(t1Schema), NoopCache) {
        // Update allFiles() so that length of files is less than spark's broadcast threshold.
        override def allFiles(): Seq[FileStatus] = {
          val length = spark.conf
            .get("spark.sql.autoBroadcastJoinThreshold")
            .toLong / 10
          Seq(new FileStatus(length, false, 1, 100, 100, new Path("t3")))
        }
      }

    t1Relation = baseRelation(t1Location, t1Schema, spark)
    t2Relation = baseRelation(t2Location, t2Schema, spark)
    t3Relation = baseRelation(t3Location, t3Schema, spark)

    t1ScanNode = LogicalRelation(t1Relation, Seq(t1c1, t1c2, t1c3, t1c4), None, false)
    t2ScanNode = LogicalRelation(t2Relation, Seq(t2c1, t2c2, t2c3, t2c4), None, false)
    t3ScanNode = LogicalRelation(t3Relation, Seq(t3c1, t3c2, t3c3, t3c4), None, false)

    t1FilterNode = Filter(IsNotNull(t1c1), t1ScanNode)
    t2FilterNode = Filter(IsNotNull(t2c1), t2ScanNode)
    t3FilterNode = Filter(IsNotNull(t3c1), t3ScanNode)

    // Project [t1c1#0, t1c3#2]
    //  +- Filter isnotnull(t1c1#0)
    //   +- Relation[t1c1#0,t1c2#1,t1c3#2,t1c4#3] parquet
    t1ProjectNode = Project(Seq(t1c1, t1c3), t1FilterNode)

    t2ProjectNode = Project(Seq(t2c1, t2c3), t2FilterNode)
    // Project [t2c1#4, t2c3#6]
    //  +- Filter isnotnull(t2c1#4)
    //   +- Relation[t2c1#4,t2c2#5,t2c3#6,t2c4#7] parquet

    t3ProjectNode = Project(Seq(t3c1, t3c3), t3FilterNode)
    // Project [t3c1#4, t3c3#6]
    //  +- Filter isnotnull(t3c1#4)
    //   +- Relation[t3c1#4,t3c2#5,t3c3#6,t3c4#7] parquet

    createIndexLogEntry("t1i1", Seq(t1c1), Seq(t1c3), t1ProjectNode)
    createIndexLogEntry("t1i2", Seq(t1c1, t1c2), Seq(t1c3), t1ProjectNode)
    createIndexLogEntry("t1i3", Seq(t1c2), Seq(t1c3), t1ProjectNode)
    createIndexLogEntry("t2i1", Seq(t2c1), Seq(t2c3), t2ProjectNode)
    createIndexLogEntry("t2i2", Seq(t2c1, t2c2), Seq(t2c3), t2ProjectNode)
    createIndexLogEntry("t3i1", Seq(t3c1), Seq(t3c3), t3ProjectNode)
  }

  before {
    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, systemPath.toUri.toString)
    clearCache()
  }

  private def baseRelation(
      location: FileIndex,
      schema: StructType,
      spark: SparkSession): HadoopFsRelation = {
    HadoopFsRelation(location, new StructType(), schema, None, new ParquetFileFormat, Map.empty)(
      spark)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(systemPath)
    super.afterAll()
  }

  test("Join rule does not update plan for Broadcast Hash Join compatible joins") {
    withSQLConf("spark.hyperspace.rule.joinV2.enabled" -> "true") {
      val joinCondition = EqualTo(t1c1, t3c1)
      val originalPlan =
        Join(t1ProjectNode, t3ProjectNode, JoinType("inner"), Some(joinCondition))
      val updatedPlan = JoinIndexRuleV2(originalPlan)
      assert(updatedPlan.equals(originalPlan))
    }
  }

  test("Join rule works if indexes exist and configs are set correctly") {
    withSQLConf("spark.hyperspace.rule.joinV2.enabled" -> "true") {
      val joinCondition = EqualTo(t1c1, t2c1)
      val originalPlan =
        Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition))
      val updatedPlan = JoinIndexRuleV2(originalPlan)
      assert(!updatedPlan.equals(originalPlan))

      val indexPaths = getIndexDataFilesPaths("t1i1") ++ getIndexDataFilesPaths("t2i1")
      verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths)
    }
  }

  test("Join rule updates sort merge join part of a plan with both smj and bhj.") {
    withSQLConf("spark.hyperspace.rule.joinV2.enabled" -> "true") {
      val bhjCondition = EqualTo(t2c1, t3c1)
      val bhjJoin = Join(t2ProjectNode, t3ProjectNode, JoinType("inner"), Some(bhjCondition))

      val smjCondition = EqualTo(t1c1, t2c1)
      val originalPlan = Join(t1ProjectNode, bhjJoin, JoinType("inner"), Some(smjCondition))

      val updatedPlan = JoinIndexRuleV2(originalPlan)
      assert(!updatedPlan.equals(originalPlan))

      val indexPaths =
        getIndexDataFilesPaths("t1i1") ++ getIndexDataFilesPaths("t2i1")
      verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths ++ Seq(new Path("t3")))
    }
  }

  private def verifyUpdatedIndex(
      originalPlan: Join,
      updatedPlan: LogicalPlan,
      indexPaths: Seq[Path]): Unit = {
    assert(treeStructureEquality(originalPlan, updatedPlan))
    assert(basePaths(updatedPlan) == indexPaths)
  }

  /** Method to check if tree structures of two logical plans are the same. */
  private def treeStructureEquality(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    val originalNodeCount = plan1.treeString.split("\n").length
    val updatedNodeCount = plan2.treeString.split("\n").length

    (originalNodeCount == updatedNodeCount) &&
    (0 until originalNodeCount).forall { i =>
      plan1(i) match {
        // For LogicalRelation, we just check if the updated also has LogicalRelation. If the
        // updated plan uses index, the root paths will be different here.
        case _: LogicalRelation => plan2(i).isInstanceOf[LogicalRelation]

        // For other node types, we compare exact matching between original and updated plans.
        case node => node.simpleString.equals(plan2(i).simpleString)
      }
    }
  }

  /** Returns tuple of left and right base relation paths for a logical plan. */
  private def basePaths(plan: LogicalPlan): Seq[Path] = {
    plan
      .collectLeaves()
      .collect {
        case LogicalRelation(HadoopFsRelation(location, _, _, _, _, _), _, _, _) =>
          location.rootPaths
      }
      .flatten
  }
}
