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

  val originals = Seq[(String, Boolean)](
    ("id", false),
    ("a.b.c", true),
    ("__hs_nested", false),
    ("__hs_nested_a", false),
    ("a.__hs_nested", true),
    ("a.__hs_nested.b", true),
    ("a.nested..b", true),
    ("a.`g.c`.b", true),
    ("a.g-c.b", true),
    ("a-b", false))
  val prefixed = Seq(
    "id",
    "__hs_nested.a.b.c",
    "__hs_nested",
    "__hs_nested_a",
    "__hs_nested.a.__hs_nested",
    "__hs_nested.a.__hs_nested.b",
    "__hs_nested.a.nested..b",
    "__hs_nested.a.`g.c`.b",
    "__hs_nested.a.g-c.b",
    "a-b")

  test("prefixNestedFieldName - default behavior") {
    originals.zipWithIndex.foreach {
      case (v, i) =>
        assert(SchemaUtils.prefixNestedFieldName(v._1) == prefixed(i))
    }
  }

  test("prefixNestedFieldName - already prefixed") {
    assert(
      SchemaUtils.prefixNestedFieldName("__hs_nested.already.prefixed") ==
        "__hs_nested.already.prefixed")
  }

  test("prefixNestedFieldNames") {
    assert(prefixed == SchemaUtils.prefixNestedFieldNames(originals))
  }

  test("removePrefixNestedFieldName") {
    prefixed.zipWithIndex.foreach {
      case (v, i) =>
        assert(SchemaUtils.removePrefixNestedFieldName(v) == originals.toSeq(i)._1)
    }
  }

  test("removePrefixNestedFieldNames") {
    assert(originals == SchemaUtils.removePrefixNestedFieldNames(prefixed))
  }

  test("isFieldNamePrefixed") {
    val expectedBooleans1 =
      Seq(false, false, false, false, false, false, false, false, false, false, false)
    originals.zipWithIndex.foreach {
      case (v, i) =>
        assert(SchemaUtils.isFieldNamePrefixed(v._1) == expectedBooleans1(i))
    }
    val expectedBooleans2 =
      Seq(false, true, false, false, true, true, true, true, true, false)
    prefixed.zipWithIndex.foreach {
      case (v, i) =>
        assert(SchemaUtils.isFieldNamePrefixed(v) == expectedBooleans2(i))
    }
  }

  test("containsNestedFieldNames") {
    assert(!SchemaUtils.containsNestedFieldNames(originals.map(_._1)))
    assert(SchemaUtils.containsNestedFieldNames(prefixed))
  }
}
