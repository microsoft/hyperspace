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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, EqualTo, IsNotNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index._

class FilterIndexRuleTest extends HyperspaceRuleTestSuite {
  override val systemPath = new Path("src/test/resources/joinIndexTest")
  val indexName1 = "filterIxTestIndex1"
  val indexName2 = "filterIxTestIndex2"

  val c1 = AttributeReference("c1", StringType)()
  val c2 = AttributeReference("c2", StringType)()
  val c3 = AttributeReference("c3", StringType)()
  val c4 = AttributeReference("c4", IntegerType)()

  val tableSchema = schemaFromAttributes(c1, c2, c3, c4)
  var scanNode: LogicalRelation = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val originalLocation = new Path("baseTableLocation")
    val tableLocation =
      new InMemoryFileIndex(spark, Seq(originalLocation), Map.empty, Some(tableSchema), NoopCache)
    val relation = baseRelation(tableLocation, tableSchema)
    scanNode = LogicalRelation(relation, Seq(c1, c2, c3, c4), None, false)

    val indexPlan = Project(Seq(c1, c2, c3), scanNode)
    createIndex(indexName1, Seq(c3, c2), Seq(c1), indexPlan)

    val index2Plan = Project(Seq(c1, c2, c3, c4), scanNode)
    createIndex(indexName2, Seq(c4, c2), Seq(c1, c3), index2Plan)
  }

  before {
    clearCache()
  }

  test("Verify FilterIndex rule is applied correctly.") {
    val filterCondition = And(IsNotNull(c3), EqualTo(c3, Literal("facebook")))
    val filterNode = Filter(filterCondition, scanNode)

    val originalPlan = Project(Seq(c2, c3), filterNode)
    val transformedPlan = FilterIndexRule(originalPlan)

    assert(!transformedPlan.equals(originalPlan), "No plan transformation.")
    verifyTransformedPlanWithIndex(transformedPlan, indexName1)
  }

  test("Verify FilterIndex rule is applied correctly to plans with alias.") {
    val aliasExpr = Alias(c3, "QueryAlias")().asInstanceOf[NamedExpression]
    val filterCondition = And(IsNotNull(aliasExpr), EqualTo(aliasExpr, Literal("facebook")))
    val filterNode = Filter(filterCondition, scanNode)

    val originalPlan = Project(Seq(c2, aliasExpr), filterNode)
    val transformedPlan = FilterIndexRule(originalPlan)

    assert(!transformedPlan.equals(originalPlan), "No plan transformation.")
    verifyTransformedPlanWithIndex(transformedPlan, indexName1)
  }

  test("Verify FilterIndex rule does not apply if all columns are not covered.") {
    val filterCondition = And(IsNotNull(c3), EqualTo(c3, Literal("facebook")))
    val filterNode = Filter(filterCondition, scanNode)

    val originalPlan = Project(Seq(c2, c3, c4), filterNode) // c4 is not covered by index
    val transformedPlan = FilterIndexRule(originalPlan)
    assert(transformedPlan.equals(originalPlan), "Plan should not transform.")
  }

  test("Verify FilterIndex rule does not apply if filter does not contain first indexed column.") {
    val filterCondition =
      And(IsNotNull(c2), EqualTo(c2, Literal("RGUID_VALUE"))) // c2 is not first indexed column
    val filterNode = Filter(filterCondition, scanNode)

    val originalPlan = Project(Seq(c2, c3), filterNode)
    val transformedPlan = FilterIndexRule(originalPlan)
    assert(transformedPlan.equals(originalPlan), "Plan should not transform.")
  }

  test("Verify FilterIndex rule is applied when all columns are selected.") {
    val filterCondition = And(IsNotNull(c4), EqualTo(c4, Literal(10, IntegerType)))
    val originalPlan = Filter(filterCondition, scanNode)

    val transformedPlan = FilterIndexRule(originalPlan)
    assert(!transformedPlan.equals(originalPlan), "No plan transformation.")
    verifyTransformedPlanWithIndex(transformedPlan, indexName2)
  }

  private def verifyTransformedPlanWithIndex(
      logicalPlan: LogicalPlan,
      indexName: String): Unit = {
    val relation = logicalPlan.collect {
      case l: LogicalRelation => l
    }
    assert(relation.length == 1)
    relation.head match {
      case l @ LogicalRelation(
            HadoopFsRelation(
              newLocation: InMemoryFileIndex,
              newPartitionSchema: StructType,
              dataSchema: StructType,
              bucketSpec: Option[BucketSpec],
              _: ParquetFileFormat,
              _),
            _,
            _,
            _) =>
        verifyIndexProperties(indexName, newLocation, newPartitionSchema, dataSchema, bucketSpec)
        assert(dataSchema.fieldNames.toSet.equals(l.output.map(_.name).toSet))
      case _ => fail("Unexpected plan.")
    }
  }

  private def verifyIndexProperties(
      indexName: String,
      location: InMemoryFileIndex,
      partitionSchema: StructType,
      dataSchema: StructType,
      bucketSpec: Option[BucketSpec]): Unit = {
    val allIndexes = IndexCollectionManager(spark).getIndexes(Seq(Constants.States.ACTIVE))
    val expectedLocation = getIndexDataFilesPath(indexName)
    assert(location.rootPaths.head.equals(expectedLocation))
    assert(partitionSchema.equals(new StructType()))
    assert(dataSchema.equals(allIndexes.filter(_.name.equals(indexName)).head.schema))
    assert(bucketSpec.isEmpty)
  }
}
