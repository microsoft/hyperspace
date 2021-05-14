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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.{Alias, And, AttributeReference, Cast, EqualTo, GetStructField, GreaterThan, IsNotNull, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, NoopCache}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.index.sources.default.DefaultFileBasedRelation

class NestedRuleHelperTest extends HyperspaceRuleSuite {

  override val indexLocationDirName = "ruleUtilsTest"

  val simpleAttr1 = AttributeReference("simpleAttr", StringType)()
  val nestedStructSchema = StructType(
    Seq(
      StructField(
        "leaf",
        StructType(
          Seq(StructField("id", StringType, true), StructField("cnt", IntegerType, true))),
        true)))
  val nestedAttrParent = AttributeReference("nestedAttr", nestedStructSchema)()
  val nestedField_Id = GetStructField(GetStructField(nestedAttrParent, 0), 0)
  val nestedField_Cnt = GetStructField(GetStructField(nestedAttrParent, 0), 1)

  val nestedField_Id_Transformed_4_Rel =
    AttributeReference("__hs_nested.nestedAttr.leaf.id", StringType)()
  val nestedField_Cnt_Transformed_4_Rel =
    AttributeReference("__hs_nested.nestedAttr.leaf.cnt", IntegerType)()

  val fullSchema = schemaFromAttributes(simpleAttr1, nestedAttrParent)
  val fullSchemaTransformed = schemaFromAttributes(
    simpleAttr1,
    nestedField_Id_Transformed_4_Rel,
    nestedField_Cnt_Transformed_4_Rel)

  var rel1: HadoopFsRelation = _
  var rel1Transformed: HadoopFsRelation = _
  var scanNode: LogicalRelation = _
  var scanNodeTransformed: LogicalRelation = _
  var filterNode: Filter = _
  var projectNode: Project = _

  var nestedIndexLogEntry: IndexLogEntry = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val loc1 =
      new InMemoryFileIndex(spark, Seq(new Path("t1")), Map.empty, Some(fullSchema), NoopCache)

    rel1 = baseRelation(loc1, fullSchema)
    scanNode = LogicalRelation(rel1, Seq(simpleAttr1, nestedAttrParent), None, false)

    rel1Transformed = baseRelation(loc1, fullSchemaTransformed)
    scanNodeTransformed = LogicalRelation(
      rel1,
      Seq(simpleAttr1, nestedField_Id_Transformed_4_Rel, nestedField_Cnt_Transformed_4_Rel),
      None,
      false)

    /**
     * Filter ((isnotnull(nested#106) AND (nested#106.leaf.cnt > 10)) AND
     *         (nested#106.leaf.id = leaf_id9))
     */
    filterNode = Filter(
      And(
        And(IsNotNull(nestedAttrParent), GreaterThan(nestedField_Cnt, Literal(10))),
        EqualTo(nestedField_Id, Literal("leaf_id9"))),
      scanNodeTransformed)

    /**
     * Project [simpleAttr#101, nested#106.leaf.cnt AS cnt#1030, nested#106.leaf.id AS id#1031]
     * +- Filter ((isnotnull(nested#106) AND (nested#106.leaf.cnt > 10)) AND
     *            (nested#106.leaf.id = leaf_id9))
     */
    projectNode = Project(
      Seq(simpleAttr1, Alias(nestedField_Cnt, "cnt")(), Alias(nestedField_Id, "id")()),
      filterNode)

    nestedIndexLogEntry = createIndexLogEntryFromStringsAndSchemas(
      "q1",
      Seq("__hs_nested.nestedAttr.leaf.cnt"),
      Seq("__hs_nested.nestedAttr.leaf.id"),
      Seq(StructField("__hs_nested.nestedAttr.leaf.cnt", IntegerType)),
      Seq(StructField("__hs_nested.nestedAttr.leaf.id", StringType)),
      projectNode)
  }

  test("testTransformPlanToUseIndexOnlyScan") {
    val nestedRuleHelper = new NestedRuleHelper(spark)
    val r =
      nestedRuleHelper.transformPlanToUseIndexOnlyScan(nestedIndexLogEntry, projectNode, false)
    val expected = "\\!Project \\[simpleAttr#\\d+, " +
        "nestedAttr#\\d+\\.leaf\\.cnt AS cnt#\\d+, nestedAttr#\\d+\\.leaf\\.id AS id#\\d+]\n\\" +
        "+\\- \\!Filter \\(\\(nestedAttr#\\d+\\.leaf\\.cnt > 10\\) (AND|&&) " +
        "\\(nestedAttr#\\d+\\.leaf\\.id = leaf_id9\\)\\)\n" +
        "   \\+\\- Relation\\[] Hyperspace\\(Type: CI, Name: q1, LogVersion: 0\\)"
    assert(expected.r.findFirstIn(r.treeString).isDefined)
  }

  test("testGetFsRelationSchema - general use case") {
    val nestedRuleHelper = new NestedRuleHelper(spark)
    val fileBasedRelation = new DefaultFileBasedRelation(spark, scanNodeTransformed)
    val r1 = nestedRuleHelper.getFsRelationSchema(fileBasedRelation, nestedIndexLogEntry)
    assert(r1.fieldNames.toSeq ==
        Seq("__hs_nested.nestedAttr.leaf.cnt", "__hs_nested.nestedAttr.leaf.id"))
  }

  test("testGetFsRelationSchema - empty") {
    val nestedRuleHelper = new NestedRuleHelper(spark)
    val fileBasedRelation =
      new DefaultFileBasedRelation(spark, LogicalRelation(rel1, Seq(simpleAttr1), None, false))
    val r1 = nestedRuleHelper.getFsRelationSchema(fileBasedRelation, nestedIndexLogEntry)
    assert(r1.fieldNames.isEmpty)
  }

  test("testHasNestedColumns - filter w/ nested fields") {
    val nestedRuleHelper = new NestedRuleHelper(spark)
    assert(nestedRuleHelper.hasNestedColumns(filterNode, nestedIndexLogEntry))
  }

  test("testHasNestedColumns - filter w/o nested fields") {

    /**
     * Filter (isnotnull(simpleAttr#1) AND (simpleAttr#1 = a))
     */
    val filterNode2 = Filter(
      And(IsNotNull(simpleAttr1), EqualTo(simpleAttr1, Literal("a"))),
      LogicalRelation(rel1, Seq(simpleAttr1), None, false))

    /**
     * Project [simpleAttr#101, nested#106.leaf.cnt AS cnt#1030, nested#106.leaf.id AS id#1031]
     * +- Filter ((isnotnull(nested#106) AND (nested#106.leaf.cnt > 10)) AND
     *            (nested#106.leaf.id = leaf_id9))
     */
    val projectNode2 = Project(Seq(simpleAttr1, Alias(simpleAttr1, "another")()), filterNode2)

    val nestedIndexLogEntry2 =
      createIndexLogEntry("q2", Seq(simpleAttr1), Seq(simpleAttr1), projectNode2)

    val nestedRuleHelper = new NestedRuleHelper(spark)
    assert(!nestedRuleHelper.hasNestedColumns(filterNode2, nestedIndexLogEntry2))
  }

  test("testHasNestedColumns - project w/ nested fields") {
    val nestedRuleHelper = new NestedRuleHelper(spark)
    assert(nestedRuleHelper.hasNestedColumns(projectNode, nestedIndexLogEntry))
  }

  test("testHasNestedColumns - project w/o nested fields") {

    /**
     * Filter (isnotnull(simpleAttr#1) AND (simpleAttr#1 = a))
     */
    val filterNode2 = Filter(
      And(IsNotNull(simpleAttr1), EqualTo(simpleAttr1, Literal("a"))),
      LogicalRelation(rel1, Seq(simpleAttr1), None, false))

    /**
     * Project [simpleAttr#101, nested#106.leaf.cnt AS cnt#1030, nested#106.leaf.id AS id#1031]
     * +- Filter ((isnotnull(nested#106) AND (nested#106.leaf.cnt > 10)) AND
     *            (nested#106.leaf.id = leaf_id9))
     */
    val projectNode2 = Project(Seq(simpleAttr1, Alias(simpleAttr1, "another")()), filterNode2)

    nestedIndexLogEntry =
      createIndexLogEntry("q3", Seq(simpleAttr1), Seq(simpleAttr1), projectNode2)

    val nestedRuleHelper = new NestedRuleHelper(spark)
    assert(!nestedRuleHelper.hasNestedColumns(projectNode2, nestedIndexLogEntry))
  }

  test("testTransformFilter - general use case") {
    val nestedRuleHelper = new NestedRuleHelper(spark)
    val newFilter = nestedRuleHelper.transformFilter(filterNode)

    val expectedTreeString =
      "Filter \\(isnotnull\\(__hs_nested.nestedAttr.leaf.cnt#\\d+\\) (AND|&&) " +
          "\\(isnotnull\\(__hs_nested\\.nestedAttr\\.leaf\\.id#\\d+\\) (AND|&&) " +
          "\\(\\(__hs_nested\\.nestedAttr\\.leaf\\.cnt#\\d+ > 10\\) (AND|&&) " +
          "\\(__hs_nested\\.nestedAttr\\.leaf\\.id#\\d+ = leaf_id9\\)\\)\\)\\)"
    assert(expectedTreeString.r.findFirstIn(newFilter.treeString).isDefined)
  }

  test("testTransformFilter - two column equality and cast") {

    /**
     * Filter (isnotnull(nestedAttr#1) AND
     *         (cast(nestedAttr#1.leaf.id as string) = nestedAttr#1.leaf.cnt))
     */
    val filterNode2 = Filter(
      And(
        IsNotNull(nestedAttrParent),
        EqualTo(Cast(nestedField_Id, StringType), nestedField_Cnt)),
      scanNodeTransformed)

    /**
     * Project [simpleAttr#101, nested#106.leaf.cnt AS cnt#1030, nested#106.leaf.id AS id#1031]
     * +- Filter ((isnotnull(nested#106) AND (nested#106.leaf.cnt > 10)) AND
     *            (nested#106.leaf.id = leaf_id9))
     */
    val projectNode2 = Project(
      Seq(simpleAttr1, Alias(nestedField_Cnt, "cnt")(), Alias(nestedField_Id, "id")()),
      filterNode2)

    createIndexLogEntryFromStringsAndSchemas(
      "q4",
      Seq("__hs_nested.nestedAttr.leaf.cnt"),
      Seq("__hs_nested.nestedAttr.leaf.id"),
      Seq(StructField("__hs_nested.nestedAttr.leaf.cnt", IntegerType)),
      Seq(StructField("__hs_nested.nestedAttr.leaf.id", StringType)),
      projectNode2)

    val nestedRuleHelper = new NestedRuleHelper(spark)
    val newFilter = nestedRuleHelper.transformFilter(filterNode2)

    val expectedTreeString =
      "'Filter \\(isnotnull\\(__hs_nested\\.nestedAttr\\.leaf\\.id#\\d+\\) (AND|&&) " +
          "\\(isnotnull\\(__hs_nested\\.nestedAttr\\.leaf\\.cnt#\\d+\\) (AND|&&) " +
          "\\(cast\\(__hs_nested\\.nestedAttr\\.leaf\\.id#\\d+ as string\\) = " +
          "__hs_nested\\.nestedAttr\\.leaf\\.cnt#\\d+\\)\\)\\)"
    assert(expectedTreeString.r.findFirstIn(newFilter.treeString).isDefined)
  }

  test("testGetUpdatedOutput") {}

  test("testTransformProject - general use case") {
    val nestedRuleHelper = new NestedRuleHelper(spark)
    val newProject = nestedRuleHelper.transformProject(projectNode)

    val expectedTreeString = "Project \\[simpleAttr#\\d+, " +
      "__hs_nested\\.nestedAttr\\.leaf\\.cnt#\\d+ AS cnt#\\d+, " +
      "__hs_nested\\.nestedAttr\\.leaf\\.id#\\d+ AS id#\\d+]"
    assert(expectedTreeString.r.findFirstIn(newProject.treeString).isDefined)
  }

  test("testTransformProject - different alias use case") {

    /**
     * Project [simpleAttr#101, nested#106.leaf.cnt AS cnt_2#1030, nested#106.leaf.id AS id_2#1031]
     * +- Filter ((isnotnull(nested#106) AND (nested#106.leaf.cnt > 10)) AND
     *            (nested#106.leaf.id = leaf_id9))
     */
    val projectNode2 = Project(
      Seq(simpleAttr1, Alias(nestedField_Cnt, "cnt_2")(), Alias(nestedField_Id, "id_2")()),
      filterNode)

    createIndexLogEntryFromStringsAndSchemas(
      "q5",
      Seq("__hs_nested.nestedAttr.leaf.cnt"),
      Seq("__hs_nested.nestedAttr.leaf.id"),
      Seq(StructField("__hs_nested.nestedAttr.leaf.cnt", IntegerType)),
      Seq(StructField("__hs_nested.nestedAttr.leaf.id", StringType)),
      projectNode2)

    val nestedRuleHelper = new NestedRuleHelper(spark)
    val newProject = nestedRuleHelper.transformProject(projectNode2)

    val expectedTreeString = "Project \\[simpleAttr#\\d+, " +
      "__hs_nested\\.nestedAttr\\.leaf\\.cnt#\\d+ AS cnt_2#\\d+, " +
      "__hs_nested\\.nestedAttr\\.leaf\\.id#\\d+ AS id_2#\\d+]"
    assert(expectedTreeString.r.findFirstIn(newProject.treeString).isDefined)
  }

  test("testTransformProject - no nested fields") {

    /**
     * Project [simpleAttr#101, nested#106.leaf.cnt AS cnt#1030, nested#106.leaf.id AS id#1031]
     * +- Filter ((isnotnull(nested#106) AND (nested#106.leaf.cnt > 10)) AND
     *            (nested#106.leaf.id = leaf_id9))
     */
    val projectNode2 = Project(Seq(simpleAttr1, Alias(simpleAttr1, "another")()), filterNode)

    nestedIndexLogEntry =
      createIndexLogEntry("q6", Seq(simpleAttr1), Seq(simpleAttr1), projectNode2)

    val nestedRuleHelper = new NestedRuleHelper(spark)
    val newProject = nestedRuleHelper.transformProject(projectNode2)

    val expectedTreeString = "Project \\[simpleAttr#\\d+, simpleAttr#\\d+ AS another#\\d+]"
    assert(expectedTreeString.r.findFirstIn(newProject.treeString).isDefined)
  }
}
