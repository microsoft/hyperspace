/*
 * Copyright (2021) The Hyperspace Project Authors.
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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.GetStructField
import org.apache.spark.sql.catalyst.plans.logical.Filter
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import com.microsoft.hyperspace.SampleNestedData
import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.util.PathUtils

class PlanUtilsTest extends HyperspaceSuite {
  override val indexLocationDirName = "planUtilsTest/samplenestedplanutils"
  var df: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    val fileSystem = new Path(indexLocationDirName).getFileSystem(new Configuration)
    val dataColumns = Seq("Date", "RGUID", "Query", "imprs", "clicks", "nested")
    fileSystem.delete(new Path(indexLocationDirName), true)
    SampleNestedData.save(spark, indexLocationDirName, dataColumns)
    df = spark.read.parquet(indexLocationDirName)
  }

  before {
    clearCache()
  }

  test("testPrefixNestedField") {
    assert(PlanUtils.prefixNestedField("a") === "a")
    assert(PlanUtils.prefixNestedField("a.b.c") === "__hs_nested.a.b.c")
    intercept[AssertionError] {
      assert(PlanUtils.prefixNestedField("__hs_nested.d.e.f") === "")
    }
  }

  test("testExtractTypeFromExpression") {
    val query = df
      .filter("nested.leaf.cnt * 10 > 100 and nested.leaf.id == 'leaf_id9' and clicks > 0")
      .select(col("Date"), concat(col("nested.leaf.cnt"), col("nested.leaf.id")).as("cnctstr"))
    val condition =
      query.queryExecution.optimizedPlan.children.head.asInstanceOf[Filter].condition
    val res1 = PlanUtils.extractTypeFromExpression(condition, "nested.leaf.cnt")
    assert(res1.isInstanceOf[IntegerType])
    val res2 = PlanUtils.extractTypeFromExpression(condition, "nested.leaf.id")
    assert(res2.isInstanceOf[StringType])
    val res3 = PlanUtils.extractTypeFromExpression(condition, "clicks")
    assert(res3.isInstanceOf[IntegerType])

    intercept[NoSuchElementException] {
      PlanUtils.extractTypeFromExpression(condition, "NotThere")
    }
  }

  test("testExtractAttributeRef") {
    val query = df
      .filter("nested.leaf.cnt * 10 > 100 and nested.leaf.id == 'leaf_id9' and clicks > 0")
      .select(col("Date"), concat(col("nested.leaf.cnt"), col("nested.leaf.id")).as("cnctstr"))
    val condition =
      query.queryExecution.optimizedPlan.children.head.asInstanceOf[Filter].condition
    val res1 = PlanUtils.extractAttributeRef(condition, "nested.leaf.cnt")
    assert(res1.dataType.isInstanceOf[StructType])
    assert(res1.name === "nested")
    val res2 = PlanUtils.extractAttributeRef(condition, "nested.leaf.id")
    assert(res2.dataType.isInstanceOf[StructType])
    assert(res2.name === "nested")
    val res3 = PlanUtils.extractAttributeRef(condition, "clicks")
    assert(res3.dataType.isInstanceOf[IntegerType])
    assert(res3.name === "clicks")

    intercept[NoSuchElementException] {
      PlanUtils.extractAttributeRef(condition, "NotThere")
    }
  }

  test("testGetChildNameFromStruct") {
    val query = df
      .filter("nested.leaf.cnt * 10 > 100 and nested.leaf.id == 'leaf_id9' and clicks > 0")
      .select(col("Date"), concat(col("nested.leaf.cnt"), col("nested.leaf.id")).as("cnctstr"))
    var gStructElems = Seq.empty[GetStructField]
    query.queryExecution.optimizedPlan.children.head.asInstanceOf[Filter].condition.foreach {
      case g: GetStructField => gStructElems :+= g
      case _ =>
    }
    assert(PlanUtils.getChildNameFromStruct(gStructElems(0)) === "nested.leaf.cnt")
    assert(PlanUtils.getChildNameFromStruct(gStructElems(1)) === "nested.leaf")
    assert(PlanUtils.getChildNameFromStruct(gStructElems(2)) === "nested.leaf.id")
    assert(PlanUtils.getChildNameFromStruct(gStructElems(3)) === "nested.leaf")
  }

  test("testExtractSearchQuery") {
    val query = df
      .filter("nested.leaf.cnt * 10 > 100 and nested.leaf.id == 'leaf_id9'")
      .select(col("Date"), concat(col("nested.leaf.cnt"), col("nested.leaf.id")).as("cnctstr"))
    val (parent, exp) = PlanUtils.extractSearchQuery(
      query.queryExecution.optimizedPlan.children.head.asInstanceOf[Filter].condition,
      "nested.leaf.id")
    assert(parent.toString().matches("\\(nested#\\d+\\.leaf\\.id = leaf_id9\\)"))
    assert(exp.toString().matches("nested#\\d+\\.leaf\\.id"))

    val query2 = df
      .select(
        col("Date"),
        concat(col("nested.leaf.cnt"), lit("-"), col("nested.leaf.id")).as("cnctstr"))
      .filter("cnctstr == 'leaf_id9-21'")
    val condition2 =
      query2.queryExecution.optimizedPlan.children.head.asInstanceOf[Filter].condition
    val (parent2, exp2) = PlanUtils.extractSearchQuery(condition2, "nested.leaf.id")
    assert(
      parent2
        .toString()
        .matches(
          "concat\\(cast\\(nested#\\d+\\.leaf\\.cnt as string\\), \\-, nested#\\d+\\.leaf\\.id\\)"))
    assert(exp2.toString().matches("nested#\\d+\\.leaf\\.id"))
    val (parent3, exp3) = PlanUtils.extractSearchQuery(condition2, "nested.leaf.cnt")
    assert(parent3.toString().matches("cast\\(nested#\\d+\\.leaf\\.cnt as string\\)"))
    assert(exp3.toString().matches("nested#\\d+\\.leaf\\.cnt"))
  }

  test("testExtractNamesFromExpression") {
    // Filter with `and`
    val query1 = df
      .filter("nested.leaf.cnt > 10 and nested.leaf.id == 'leaf_id9'")
      .select("Date", "nested.leaf.cnt", "nested.leaf.id")
    val res1 =
      PlanUtils.extractNamesFromExpression(
        query1.queryExecution.optimizedPlan.children.head.asInstanceOf[Filter].condition)
    assert(res1.toKeep.nonEmpty)
    assert(res1.toKeep === Set("nested.leaf.cnt", "nested.leaf.id"))
    assert(res1.toDiscard.nonEmpty)
    assert(res1.toDiscard === Set("nested"))
    // Filter with multiplication in filter and to support `concat` in select
    val query2 = df
      .filter("nested.leaf.cnt * 10 > 100 and nested.leaf.id == 'leaf_id9'")
      .select(col("Date"), concat(col("nested.leaf.cnt"), col("nested.leaf.id")).as("cnctstr"))
    val res2 =
      PlanUtils.extractNamesFromExpression(
        query2.queryExecution.optimizedPlan.children.head.asInstanceOf[Filter].condition)
    assert(res2.toKeep.nonEmpty)
    assert(res2.toKeep === Set("nested.leaf.cnt", "nested.leaf.id"))
    assert(res2.toDiscard.nonEmpty)
    assert(res2.toDiscard === Set("nested"))
    // Filter with `concat`
    val query3 = df
      .select(
        col("Date"),
        concat(col("nested.leaf.cnt"), lit("-"), col("nested.leaf.id")).as("cnctstr"))
      .filter("cnctstr == 'leaf_id9-21'")
    val res3 =
      PlanUtils.extractNamesFromExpression(
        query3.queryExecution.optimizedPlan.children.head.asInstanceOf[Filter].condition)
    assert(res3.toKeep.nonEmpty)
    assert(res3.toKeep === Set("nested.leaf.cnt", "nested.leaf.id"))
    assert(res3.toDiscard.isEmpty)
  }

  test("testReplaceExpression") {
    val query = df
      .select(
        col("Date"),
        concat(col("nested.leaf.cnt"), lit("-"), col("nested.leaf.id")).as("cnctstr"))
      .filter("cnctstr == 'leaf_id9-21'")
    val condition =
      query.queryExecution.optimizedPlan.children.head.asInstanceOf[Filter].condition
    val (parent, exp) = PlanUtils.extractSearchQuery(condition, "nested.leaf.id")
    val res = PlanUtils.replaceExpression(parent, exp, lit("REPLACED").expr)
    assert(
      res
        .toString()
        .matches("concat\\(cast\\(nested#\\d+\\.leaf\\.cnt as string\\), \\-, REPLACED\\)"))
    val condition2 =
      query.queryExecution.optimizedPlan.children.head.asInstanceOf[Filter].condition
    val (parent2, exp2) = PlanUtils.extractSearchQuery(condition2, "nested.leaf.cnt")
    val res2 = PlanUtils.replaceExpression(parent2, exp2, lit("REPLACED").expr)
    assert(res2.toString().matches("cast\\(REPLACED as string\\)"))
  }
}
