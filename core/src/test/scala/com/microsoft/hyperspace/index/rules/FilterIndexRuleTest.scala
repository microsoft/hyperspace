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
import org.apache.spark.sql.catalyst.expressions.{Alias, And, Attribute, AttributeReference, EqualTo, IsNotNull, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.serde.LogicalPlanSerDeUtils
import com.microsoft.hyperspace.util.FileUtils

@RunWith(classOf[JUnitRunner])
class FilterIndexRuleTest extends HyperspaceSuite {
  val originalLocation = new Path("baseTableLocation")
  val parentPath = new Path("src/test/resources/filterIndexTest")
  val systemPath = new Path(parentPath, "systemPath")

  val indexName = "filterIxTestIndex"

  val c1 = AttributeReference("c1", StringType)()
  val c2 = AttributeReference("c2", StringType)()
  val c3 = AttributeReference("c3", StringType)()
  val c4 = AttributeReference("c4", IntegerType)()

  val tableSchema = schemaFromAttributes(c1, c2, c3, c4)
  var scanNode: LogicalRelation = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.delete(parentPath)

    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, systemPath.toUri.toString)

    val tableLocation =
      new InMemoryFileIndex(spark, Seq(originalLocation), Map.empty, Some(tableSchema), NoopCache)
    val relation = baseRelation(tableLocation, tableSchema)
    scanNode = LogicalRelation(relation, Seq(c1, c2, c3, c4), None, false)

    // TODO: Refactor common test code for Rules
    def createIndex(
        name: String,
        indexCols: Seq[AttributeReference],
        includedCols: Seq[AttributeReference],
        plan: LogicalPlan): IndexLogEntry = {
      val signClass = new RuleTestHelper.TestSignatureProvider().getClass.getName
      val sign: LogicalPlan => String = LogicalPlanSignatureProvider.create(signClass).signature

      val sourcePlanProperties = SparkPlan.Properties(
        LogicalPlanSerDeUtils.serialize(plan, spark),
        LogicalPlanFingerprint(
          LogicalPlanFingerprint.Properties(Seq(Signature(signClass, sign(plan))))))
      val sourceDataProperties =
        Hdfs.Properties(Content("", Seq(Content.Directory("", Seq(), NoOpFingerprint()))))

      val indexLogEntry = IndexLogEntry(
        name,
        CoveringIndex(
          CoveringIndex.Properties(
            CoveringIndex.Properties
              .Columns(indexCols.map(_.name), includedCols.map(_.name)),
            IndexLogEntry.schemaString(schemaFromAttributes(indexCols ++ includedCols: _*)),
            10)),
        Content(getIndexDataFilesPath(name).toUri.toString, Seq()),
        Source(SparkPlan(sourcePlanProperties), Seq(Hdfs(sourceDataProperties))),
        Map())

      val logManager = new IndexLogManagerImpl(getIndexRootPath(name))
      indexLogEntry.state = Constants.States.ACTIVE
      logManager.writeLog(0, indexLogEntry)

      indexLogEntry
    }

    val indexPlan = Project(Seq(c1, c2, c3), scanNode)
    createIndex(indexName, Seq(c3, c2), Seq(c1), indexPlan)
  }

  before {
    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, systemPath.toUri.toString)
    clearCache()
  }

  override def afterAll(): Unit = {
    FileUtils.delete(parentPath)
    super.afterAll()
  }

  test("Verify FilterIndex rule is applied correctly.") {
    val filterCondition = And(IsNotNull(c3), EqualTo(c3, Literal("facebook")))
    val filterNode = Filter(filterCondition, scanNode)

    val originalPlan = Project(Seq(c2, c3), filterNode)
    val transformedPlan = FilterIndexRule(originalPlan)

    assert(!transformedPlan.equals(originalPlan), "No plan transformation.")
    verifyTransformedPlan(transformedPlan)
  }

  test("Verify FilterIndex rule is applied correctly to plans with alias.") {
    val aliasExpr = Alias(c3, "QueryAlias")().asInstanceOf[NamedExpression]
    val filterCondition = And(IsNotNull(aliasExpr), EqualTo(aliasExpr, Literal("facebook")))
    val filterNode = Filter(filterCondition, scanNode)

    val originalPlan = Project(Seq(c2, aliasExpr), filterNode)
    val transformedPlan = FilterIndexRule(originalPlan)

    assert(!transformedPlan.equals(originalPlan), "No plan transformation.")
    verifyTransformedPlan(transformedPlan)
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

  private def verifyTransformedPlan(logicalPlan: LogicalPlan): Unit = {
    logicalPlan match {
      case Project(
          _,
          Filter(
            _,
            LogicalRelation(
              HadoopFsRelation(
                newLocation: InMemoryFileIndex,
                newPartitionSchema: StructType,
                dataSchema: StructType,
                bucketSpec: Option[BucketSpec],
                _: ParquetFileFormat,
                _),
              _,
              _,
              _))) =>
        val allIndexes = IndexCollectionManager(spark).getIndexes(Seq(Constants.States.ACTIVE))
        val expectedLocation = getIndexDataFilesPath(indexName)
        assert(newLocation.rootPaths.head.equals(expectedLocation), "Invalid location.")
        assert(newPartitionSchema.equals(new StructType()), "Invalid partition schema.")
        assert(dataSchema.equals(allIndexes.head.schema), "Invalid schema.")
        assert(bucketSpec.isEmpty, "Invalid bucket spec.")

      case _ => fail("Unexpected plan.")
    }
  }

  private def baseRelation(location: FileIndex, schema: StructType): HadoopFsRelation = {
    HadoopFsRelation(location, new StructType(), schema, None, new ParquetFileFormat, Map.empty)(
      spark)
  }

  private def schemaFromAttributes(attributes: Attribute*): StructType = {
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
  }

  private def getIndexDataFilesPath(indexName: String): Path = {
    new Path(getIndexRootPath(indexName), s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
  }

  private def getIndexRootPath(indexName: String): Path = {
    new Path(systemPath, indexName)
  }
}
