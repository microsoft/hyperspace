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

  val originals = Seq(
    "id",
    "a.b.c",
    "__hs_nested",
    "__hs_nested_a",
    "a.__hs_nested",
    "a.__hs_nested.b",
    "a.__hs_nested.b",
    "a.nested..b",
    "a.`g.c`.b",
    "a.g-c.b",
    "a-b")
  val prefixed = Seq(
    "id",
    "__hs_nested.a.b.c",
    "__hs_nested",
    "__hs_nested_a",
    "__hs_nested.a.__hs_nested",
    "__hs_nested.a.__hs_nested.b",
    "__hs_nested.a.__hs_nested.b",
    "__hs_nested.a.nested..b",
    "__hs_nested.a.`g.c`.b",
    "__hs_nested.a.g-c.b",
    "a-b")

  test("prefixNestedFieldName") {
    originals.zipWithIndex.foreach {
      case (v, i) =>
        assert(SchemaUtils.prefixNestedFieldName(v) == prefixed(i))
    }
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
        assert(SchemaUtils.removePrefixNestedFieldName(v) == originals(i))
    }
  }

  test("removePrefixNestedFieldNames") {
    assert(originals == SchemaUtils.removePrefixNestedFieldNames(prefixed))
  }

  test("isFieldNamePrefixed") {
    val expectedBools1 =
      Seq(false, false, false, false, false, false, false, false, false, false, false)
    originals.zipWithIndex.foreach {
      case (v, i) =>
        assert(SchemaUtils.isFieldNamePrefixed(v) == expectedBools1(i))
    }
    val expectedBools2 =
      Seq(false, true, false, false, true, true, true, true, true, true, false)
    prefixed.zipWithIndex.foreach {
      case (v, i) =>
        assert(SchemaUtils.isFieldNamePrefixed(v) == expectedBools2(i))
    }
  }

  test("containsNestedFieldNames") {
    assert(!SchemaUtils.containsNestedFieldNames(originals))
    assert(SchemaUtils.containsNestedFieldNames(prefixed))
  }
}
