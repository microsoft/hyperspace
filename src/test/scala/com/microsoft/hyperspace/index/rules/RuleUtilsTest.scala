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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, IsNotNull}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, NoopCache}
import org.apache.spark.sql.types.{IntegerType, StringType}

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.{HyperspaceSuite, IndexConstants}
import com.microsoft.hyperspace.util.FileUtils

class RuleUtilsTest extends HyperspaceSuite {
  val parentPath = new Path("src/test/resources/ruleUtilsTest")
  val systemPath = new Path(parentPath, "systemPath")

  val t1c1 = AttributeReference("t1c1", IntegerType)()
  val t1c2 = AttributeReference("t1c2", StringType)()
  val t1c3 = AttributeReference("t1c3", IntegerType)()
  val t1c4 = AttributeReference("t1c4", StringType)()
  val t2c1 = AttributeReference("t2c1", IntegerType)()
  val t2c2 = AttributeReference("t2c2", StringType)()
  val t2c3 = AttributeReference("t2c3", IntegerType)()
  val t2c4 = AttributeReference("t2c4", StringType)()

  val t1Schema = RuleTestHelper.schemaFromAttributes(t1c1, t1c2, t1c3, t1c4)
  val t2Schema = RuleTestHelper.schemaFromAttributes(t2c1, t2c2, t2c3, t2c4)

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
    FileUtils.delete(parentPath)

    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, systemPath.toUri.toString)

    val t1Location =
      new InMemoryFileIndex(spark, Seq(new Path("t1")), Map.empty, Some(t1Schema), NoopCache)

    val t2Location =
      new InMemoryFileIndex(spark, Seq(new Path("t2")), Map.empty, Some(t2Schema), NoopCache)

    t1Relation = RuleTestHelper.baseRelation(t1Location, t1Schema, spark)
    t2Relation = RuleTestHelper.baseRelation(t2Location, t2Schema, spark)

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

    RuleTestHelper.createIndex(systemPath, spark, "t1i1", Seq(t1c1), Seq(t1c3), t1ProjectNode)
    RuleTestHelper.createIndex(systemPath, spark, "t1i2", Seq(t1c1, t1c2), Seq(t1c3), t1ProjectNode)
    RuleTestHelper.createIndex(systemPath, spark, "t1i3", Seq(t1c2), Seq(t1c3), t1ProjectNode)
    RuleTestHelper.createIndex(systemPath, spark, "t2i1", Seq(t2c1), Seq(t2c3), t2ProjectNode)
    RuleTestHelper.createIndex(systemPath, spark, "t2i2", Seq(t2c1, t2c2), Seq(t2c3), t2ProjectNode)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(parentPath)
    super.afterAll()
  }

  before {
    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, systemPath.toUri.toString)
    clearCache()
  }

  test("Verify indexes are matched by signature correctly.") {
    val indexManager = Hyperspace
      .getContext(SparkSession.getActiveSession.get)
      .indexCollectionManager

    val indexPlan = Project(Seq(t1c1, t1c2, t1c3), t1ProjectNode)
    val resultLen = RuleUtils.getCandidateIndexes(indexManager, indexPlan).length

    val indexPlan2 = Project(Seq(t2c1, t2c4), t2ProjectNode)
    val resultsLen2 = RuleUtils.getCandidateIndexes(indexManager, indexPlan2).length

    assert(resultLen == 3)
    assert(resultsLen2 == 2)

    indexManager.delete("t1i1")

    val resultLen3 = RuleUtils.getCandidateIndexes(indexManager, indexPlan).length
    assert(resultLen3 == 2)
  }

}
