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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualTo, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.expressions.aggregate.Count
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import com.microsoft.hyperspace.SparkInvolvedSuite
import com.microsoft.hyperspace.shim.JoinWithoutHint

class IndexSignatureProviderTest extends SparkFunSuite with SparkInvolvedSuite {
  private val fileLength1 = 100
  private val fileModificationTime1 = 10000
  private val filePath1 = new Path("originalLocation1")

  private val fileLength2 = 250
  private val fileModificationTime2 = 35000
  private val filePath2 = new Path("originalLocation2")

  test("Verify signature for a plan with same logical relation node.") {
    val r1 = createSimplePlan(fileLength1, fileModificationTime1, filePath1)
    val r2 = createSimplePlan(fileLength1, fileModificationTime1, filePath1)
    assert(createIndexSignature(r1).equals(createIndexSignature(r2)))
  }

  test("Verify signature for a multi-node plan.") {
    val p1 = createComplexPlan
    val p2 = createComplexPlan
    assert(createIndexSignature(p1).equals(createIndexSignature(p2)))
  }

  test("Verify signature inequality for plans with different logical relation nodes.") {
    val r1 = createSimplePlan(fileLength1, fileModificationTime1, filePath1)
    val r2 = createSimplePlan(fileLength2, fileModificationTime2, filePath2)
    assert(!createIndexSignature(r1).equals(createIndexSignature(r2)))
  }

  test(
    "Verify signature inequality for plans with same logical relation and different structure.") {
    val r1 = createSimplePlan(fileLength1, fileModificationTime1, filePath1)
    val r2 = createLinearPlan(fileLength2, fileModificationTime2, filePath2)
    assert(!createIndexSignature(r1).equals(createIndexSignature(r2)))
  }

  private def createSimplePlan(
      length: Long,
      modificationTime: Long,
      path: Path): LogicalRelation = {
    val c1 = AttributeReference("c1", StringType)()
    val c2 = AttributeReference("c2", StringType)()

    val tableSchema = schemaFromAttributes(c1, c2)
    SignatureProviderTestUtils.createLogicalRelation(
      spark,
      Seq(createFileStatus(length, modificationTime, path)),
      tableSchema)
  }

  private def createLinearPlan(length: Long, modificationTime: Long, path: Path): LogicalPlan = {
    val t1c1 = AttributeReference("t1c1", StringType)()
    val t1c2 = AttributeReference("t1c2", StringType)()
    val t1c3 = AttributeReference("t1c3", IntegerType)()

    val tableSchema = schemaFromAttributes(t1c1, t1c2, t1c3)
    val scanNode = SignatureProviderTestUtils.createLogicalRelation(
      spark,
      Seq(createFileStatus(length, modificationTime, path)),
      tableSchema)

    val filterCondition = And(EqualTo(t1c1, Literal("ABC")), IsNotNull(t1c1))
    val filterNode = Filter(filterCondition, scanNode)

    Project(Seq(t1c2, t1c3), filterNode)
  }

  private def createComplexPlan: LogicalPlan = {
    val t1c1 = AttributeReference("t1c1", StringType)()
    val t1c2 = AttributeReference("t1c2", StringType)()
    val t1c3 = AttributeReference("t1c3", IntegerType)()

    val t1Schema = schemaFromAttributes(t1c1, t1c2, t1c3)
    val r1 = SignatureProviderTestUtils.createLogicalRelation(
      spark,
      Seq(createFileStatus(fileLength1, fileModificationTime1, filePath1)),
      t1Schema)

    val t2c1 = AttributeReference("t2c1", IntegerType)()
    val t2c2 = AttributeReference("t2c2", IntegerType)()

    val t2Schema = schemaFromAttributes(t2c1, t2c2)
    val r2 = SignatureProviderTestUtils.createLogicalRelation(
      spark,
      Seq(createFileStatus(fileLength2, fileModificationTime2, filePath2)),
      t2Schema)

    val joinCondition = EqualTo(t1c3, t2c2)
    val joinNode = JoinWithoutHint(r1, r2, JoinType("inner"), Some(joinCondition))

    val filterCondition = And(EqualTo(t1c1, Literal("ABC")), IsNotNull(t1c1))
    val filterNode = Filter(filterCondition, joinNode)

    val projectNode = Project(Seq(t1c2, t1c3, t2c1), filterNode)

    val grpExpression = Seq(t1c2, t1c3)
    val aggExpression =
      Seq(t1c2, t1c3, Alias(Count(Seq(t2c1)), "count(t2c1)")())

    Aggregate(grpExpression, aggExpression, projectNode)
  }

  private def createIndexSignature(logicalPlan: LogicalPlan): String =
    new IndexSignatureProvider().signature(logicalPlan) match {
      case Some(s) => s
      case None => fail("Invalid plan for signature generation.")
    }

  private def schemaFromAttributes(attributes: Attribute*): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  private def createFileStatus(length: Long, modificationTime: Long, path: Path): FileStatus =
    SignatureProviderTestUtils.createFileStatus(length, modificationTime, path)
}
