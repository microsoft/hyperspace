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

package com.microsoft.hyperspace.util

import org.apache.spark.SparkFunSuite

import com.microsoft.hyperspace.SparkInvolvedSuite

class SchemaUtilsTest extends SparkFunSuite with SparkInvolvedSuite {

  test("flatten - no nesting") {
    import spark.implicits._

    val dfNoNesting = Seq(
      (1, "name1", "b1"),
      (2, "name2", "b2"),
      (3, "name3", "b3"),
      (4, "name4", "b4")
    ).toDF("id", "name", "other")

    val flattenedNoNesting = SchemaUtils.flatten(dfNoNesting.schema)

    assert(flattenedNoNesting.length == 3)
    assert(flattenedNoNesting(0) == "id")
    assert(flattenedNoNesting(1) == "name")
    assert(flattenedNoNesting(2) == "other")
  }

  test("flatten - struct") {
    import spark.implicits._

    val df1 = Seq(
      (1, "name1", NestedType4("nf1", NestedType("n1", 1L))),
      (2, "name2", NestedType4("nf2", NestedType("n2", 2L))),
      (3, "name3", NestedType4("nf3", NestedType("n3", 3L))),
      (4, "name4", NestedType4("nf4", NestedType("n4", 4L)))
    ).toDF("id", "name", "nested")

    val flattened = SchemaUtils.flatten(df1.schema)

    assert(flattened.length == 5)
    assert(flattened(0) == "id")
    assert(flattened(1) == "name")
    assert(flattened(2) == "nested.nf1_b")
    assert(flattened(3) == "nested.n.f1")
    assert(flattened(4) == "nested.n.f2")

    /**
     * Given the dataset with schema below
     *
     *   root
     *    |-- id: integer (nullable = false)
     *    |-- name: string (nullable = true)
     *    |-- nested: struct (nullable = true)
     *    |    |-- nf1: string (nullable = true)
     *    |    |-- n: struct (nullable = true)
     *    |    |    |-- nf_a: string (nullable = true)
     *    |    |    |-- n: struct (nullable = true)
     *    |    |    |    |-- nf1_b: string (nullable = true)
     *    |    |    |    |-- n: struct (nullable = true)
     *    |    |    |    |    |-- f1: string (nullable = true)
     *    |    |    |    |    |-- f2: long (nullable = false)
     *
     * The output should be the list of leaves maintaining the order
     *
     *   id
     *   name
     *   nested.nf1
     *   nested.n.nfa
     *   nested.n.n.nf1_b
     *   nested.n.n.n.f1
     *   nested.n.n.n.f2
     */
    val df2 = Seq(
      (1, "name1", NestedType2("nf1", NestedType3("n1", NestedType4("h1",
        NestedType("end1", 1L))))),
      (2, "name2", NestedType2("nf2", NestedType3("n2", NestedType4("h2",
        NestedType("end2", 1L))))),
      (3, "name3", NestedType2("nf3", NestedType3("n3", NestedType4("h3",
        NestedType("end3", 1L))))),
      (4, "name4", NestedType2("nf4", NestedType3("n4", NestedType4("h4",
        NestedType("end4", 1L)))))
    ).toDF("id", "name", "nested")

    val flattened2 = SchemaUtils.flatten(df2.schema)

    assert(flattened2.length == 7)
    assert(flattened2(0) == "id")
    assert(flattened2(1) == "name")
    assert(flattened2(2) == "nested.nf1")
    assert(flattened2(3) == "nested.n.nf_a")
    assert(flattened2(4) == "nested.n.n.nf1_b")
    assert(flattened2(5) == "nested.n.n.n.f1")
    assert(flattened2(6) == "nested.n.n.n.f2")
  }

  test("flatten - array") {
    import spark.implicits._

    val df1 = Seq(
      (1, "name1", Array[NestedType](NestedType("n1", 1L), NestedType("o1", 10L))),
      (2, "name2", Array[NestedType](NestedType("n2", 2L), NestedType("o2", 20L))),
      (3, "name3", Array[NestedType](NestedType("n3", 3L), NestedType("o3", 30L))),
      (4, "name4", Array[NestedType](NestedType("n4", 4L), NestedType("o4", 40L)))
    ).toDF("id", "name", "arrayOfNested")

    val flattened = SchemaUtils.flatten(df1.schema)

    assert(flattened.length == 4)
    assert(flattened(0) == "id")
    assert(flattened(1) == "name")
    assert(flattened(2) == "arrayOfNested.f1")
    assert(flattened(3) == "arrayOfNested.f2")

    /**
     * Given the dataset with schema below
     *
     * root
     *  |-- id: integer (nullable = false)
     *  |-- name: string (nullable = true)
     *  |-- arrayOfNested: array (nullable = true)
     *  |    |-- element: struct (containsNull = true)
     *  |    |    |-- nf1_b: string (nullable = true)
     *  |    |    |-- n: struct (nullable = true)
     *  |    |    |    |-- f1: string (nullable = true)
     *  |    |    |    |-- f2: long (nullable = false)
     *
     * The output should be the list of leaves maintaining the order
     *
     *   id
     *   name
     *   arrayOfNested.nf1_b
     *   arrayOfNested.n.f1
     *   arrayOfNested.n.f2
     */
    val df2 = Seq(
      (1, "name1", Array[NestedType4](
        NestedType4("n1", NestedType("o1", 11L)),
        NestedType4("a1", NestedType("b1", 1L)))),
      (2, "name2", Array[NestedType4](
        NestedType4("n2", NestedType("o2", 12L)),
        NestedType4("a2", NestedType("b2", 2L)))),
      (3, "name3", Array[NestedType4](
        NestedType4("n3", NestedType("o3", 13L)),
        NestedType4("a3", NestedType("b3", 3L)))),
      (4, "name4", Array[NestedType4](
        NestedType4("n4", NestedType("o4", 14L)),
        NestedType4("a4", NestedType("b4", 4L))))
    ).toDF("id", "name", "arrayOfNested")

    val flattened2 = SchemaUtils.flatten(df2.schema)

    assert(flattened2.length == 5)
    assert(flattened2(0) == "id")
    assert(flattened2(1) == "name")
    assert(flattened2(2) == "arrayOfNested.nf1_b")
    assert(flattened2(3) == "arrayOfNested.n.f1")
    assert(flattened2(4) == "arrayOfNested.n.f2")
  }

  test("escapeFieldName") {
    assert(SchemaUtils.escapeFieldName("a.b") == "a__b")
    assert(SchemaUtils.escapeFieldName("a.b.c.d") == "a__b__c__d")
    assert(SchemaUtils.escapeFieldName("a_b_c_d") == "a_b_c_d")
  }

  test("escapeFieldNames") {
    assert(SchemaUtils.escapeFieldNames(
      Seq("a.b.c.d", "a.b", "A_B")) == Seq("a__b__c__d", "a__b", "A_B"))
    assert(SchemaUtils.escapeFieldNames(Seq.empty[String]).isEmpty)
  }

  test("unescapeFieldName") {
    assert(SchemaUtils.unescapeFieldName("a__b") == "a.b")
    assert(SchemaUtils.unescapeFieldName("a__b__c__d") == "a.b.c.d")
    assert(SchemaUtils.unescapeFieldName("a_b_c_d") == "a_b_c_d")
  }

  test("unescapeFieldNames") {
    assert(SchemaUtils.unescapeFieldNames(
      Seq("a__b__c__d", "a__b", "A_B")) == Seq("a.b.c.d", "a.b", "A_B"))
    assert(SchemaUtils.escapeFieldNames(Seq.empty[String]).isEmpty)
  }
}

case class NestedType4(nf1_b: String, n: NestedType)
case class NestedType3(nf_a: String, n: NestedType4)
case class NestedType2(nf1: String, n: NestedType3)
case class NestedType(f1: String, f2: Long)
