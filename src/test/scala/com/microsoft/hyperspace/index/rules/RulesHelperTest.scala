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
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types.StringType

import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.util.FileUtils

class RulesHelperTest extends HyperspaceSuite {
  val originalLocation = new Path("baseTableLocation")
  val parentPath = new Path("src/test/resources/rulesHelperTest")

  val c1 = AttributeReference("c1", StringType)()
  val c2 = AttributeReference("c2", StringType)()

  val tableSchema = RuleTestHelper.schemaFromAttributes(c1, c2)
  var scanNode: LogicalRelation = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.delete(parentPath)

    val tableLocation =
      new InMemoryFileIndex(spark, Seq(originalLocation), Map.empty, Some(tableSchema), NoopCache)
    val relation = RuleTestHelper.baseRelation(tableLocation, tableSchema, spark)
    scanNode = LogicalRelation(relation, Seq(c1, c2), None, false)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(parentPath)
    super.afterAll()
  }

  test("Verify get logical relation for single logical relation node plan.") {
    validateLogicalRelation(scanNode)
  }

  test("Verify get logical relation for multi-node linear plan.") {
    val filterCondition = And(IsNotNull(c2), EqualTo(c2, Literal("abc")))
    val filterNode = Filter(filterCondition, scanNode)
    validateLogicalRelation(Project(Seq(c1), filterNode))
  }

  test("Verify get logical relation for non-linear plan.") {
    val filterCondition = And(IsNotNull(c2), EqualTo(c2, Literal("abc")))
    val filterNode = Filter(filterCondition, scanNode)
    val joinNode = Join(filterNode, scanNode, JoinType("inner"), None)
    val r = RulesHelper.getLogicalRelation(Project(Seq(c1), joinNode))
    assert(r.isEmpty)
  }

  private def validateLogicalRelation(plan: LogicalPlan): Unit = {
    val r = RulesHelper.getLogicalRelation(plan)
    assert(r.isDefined)
    assert(r.get.equals(scanNode))
  }
}
