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

import org.apache.spark.SparkFunSuite

class IndexConfigTest extends SparkFunSuite {
  test("Empty index name is not allowed.") {
    intercept[IllegalArgumentException](IndexConfig("", Seq("c1"), Seq("c2")))
    intercept[IllegalArgumentException](IndexConfig.builder.indexBy("c1").include("c2").create)
    intercept[IllegalArgumentException](IndexConfig.builder.indexName(""))
  }

  test("Empty indexed columns are not allowed.") {
    intercept[IllegalArgumentException](IndexConfig("name", Seq(), Seq("c1")))
    intercept[IllegalArgumentException](
      IndexConfig.builder.indexName("name").include("c1").create)
  }

  test("Same indexed column names (case-insensitive) are not allowed.") {
    intercept[IllegalArgumentException](IndexConfig("name", Seq("c1", "C1"), Seq("c2")))
    intercept[IllegalArgumentException](
      IndexConfig.builder.indexName("name").indexBy("c1", "C1").include("c2").create)
  }

  test("Same column names (case-insensitive) in indexed/included columns are not allowed.") {
    intercept[IllegalArgumentException](IndexConfig("name", Seq("c1"), Seq("C1", "c2")))
    intercept[IllegalArgumentException](
      IndexConfig.builder.indexName("name").indexBy("c1").include("C1", "c2").create)
  }

  test("Test equals() function.") {
    assert(
      !IndexConfig("name", Seq("c1", "c2"), Seq("c3", "c4")).equals(AnyRef),
      "An IndexConfig must not equal to AnyRef.")
    assert(
      !IndexConfig("name", Seq("c1", "c2"), Seq("c3", "c4"))
        .equals(IndexConfig("name", Seq("c2", "c1"), Seq("c3", "c4"))),
      "IndexConfigs with same indexed columns (but different order) must not be equal.")
    assert(
      !IndexConfig("name", Seq("c1", "c2"), Seq("c3", "c4"))
        .equals(IndexConfig("name", Seq("c1", "c5"), Seq("c3", "c4"))),
      "IndexConfigs with different indexed columns must not be equal.")
    assert(
      !IndexConfig("name", Seq("c1", "c2"), Seq("c3", "c4"))
        .equals(IndexConfig("name", Seq("c1", "c2"), Seq("c3", "c5"))),
      "IndexConfigs with different included columns must not be equal.")
    assert(
      !IndexConfig("Name1", Seq("c1", "c2"), Seq("c3", "c4"))
        .equals(IndexConfig("Name2", Seq("c1", "c2"), Seq("c3", "c4"))),
      "IndexConfigs with different index names must not be equal.")
    assert(
      IndexConfig("name", Seq("c1", "c2"), Seq("c3", "c4"))
        .equals(IndexConfig("name", Seq("c1", "c2"), Seq("c3", "c4"))),
      "IndexConfigs with the same index name, indexed columns " +
        "and the same included columns must be equal.")
    assert(
      IndexConfig("name", Seq("c1", "c2"), Seq("c3", "c4"))
        .equals(IndexConfig("name", Seq("c1", "c2"), Seq("c4", "c3"))),
      "IndexConfigs with the same index name, indexed columns " +
        "and included columns having same set of " +
        "columns (but different order) must be equal.")
    assert(
      IndexConfig("name", Seq("c1", "c2"), Seq("c3", "c4"))
        .equals(IndexConfig("Name", Seq("C1", "C2"), Seq("C3", "C4"))),
      "IndexConfigs with the same indexed columns and the same included columns " +
        "and index name (case-insensitive) must be equal.")
  }

  test("Test that if two IndexConfigs are equal, their hash code must be equal.") {
    val indexConfig1 = IndexConfig("name1", Seq("c1"), Seq("c2"))
    val indexConfig2 = IndexConfig("name1", Seq("C1"), Seq("c2"))
    assert(indexConfig1.equals(indexConfig2))
    assert(indexConfig1.hashCode == indexConfig2.hashCode)

    val indexConfig3 = IndexConfig("name3", Seq("c1"), Seq("c3", "c4"))
    val indexConfig4 = IndexConfig("name3", Seq("C1"), Seq("c4", "c3"))
    assert(indexConfig3.equals(indexConfig4))
    assert(indexConfig3.hashCode == indexConfig4.hashCode)
  }

  test("Test IndexConfig object with builder pattern.") {
    val indexName = "Name"
    val indexedColumns = Seq("C1", "c2", "C3")
    val includedColumns = Seq("C4", "c5", "C6")

    val indexConfig = IndexConfig.builder
      .indexName(indexName)
      .indexBy(indexedColumns.head, indexedColumns.tail: _*)
      .include(includedColumns.head, includedColumns.tail: _*)
      .create

    assert(indexConfig.indexName.equals(indexName))
    assert(indexConfig.indexedColumns.equals(indexedColumns))
    assert(indexConfig.includedColumns.equals(includedColumns))
  }

  test("Test exception on multiple indexBy, include and index name on IndexConfig builder.") {
    intercept[UnsupportedOperationException](
      IndexConfig.builder
        .indexName("name1")
        .indexName("name2")
        .indexBy("c1", "c2")
        .include("c3", "c4")
        .create)
    intercept[UnsupportedOperationException](
      IndexConfig.builder
        .indexName("name")
        .indexBy("c1")
        .indexBy("c2")
        .include("c3", "c4")
        .create)
    intercept[UnsupportedOperationException](
      IndexConfig.builder
        .indexName("name")
        .indexBy("c1")
        .include("c2")
        .include("c3")
        .create)
  }
}
