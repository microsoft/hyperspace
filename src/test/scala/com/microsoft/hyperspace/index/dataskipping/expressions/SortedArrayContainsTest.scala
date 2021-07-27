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

package com.microsoft.hyperspace.index.dataskipping.expressions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.index.dataskipping.ArrayTestUtils

class SortedArrayContainsTest
    extends HyperspaceSuite
    with ArrayTestUtils
    with ExpressionEvalHelper {
  test("SortedArrayContains works correctly for an empty array.") {
    val array = createArray(Nil, IntegerType)
    checkEvaluation(SortedArrayContains(array, Literal(0, IntegerType)), false)
  }

  test("SortedArrayContains works correctly for a array of size 1.") {
    val array = createArray(Seq(1), IntegerType)
    checkEvaluation(SortedArrayContains(array, Literal(0, IntegerType)), false)
    checkEvaluation(SortedArrayContains(array, Literal(1, IntegerType)), true)
    checkEvaluation(SortedArrayContains(array, Literal(2, IntegerType)), false)
  }

  test("SortedArrayContains works correctly for a array of size 2.") {
    val array = createArray(Seq(1, 3), IntegerType)
    checkEvaluation(SortedArrayContains(array, Literal(0, IntegerType)), false)
    checkEvaluation(SortedArrayContains(array, Literal(1, IntegerType)), true)
    checkEvaluation(SortedArrayContains(array, Literal(2, IntegerType)), false)
    checkEvaluation(SortedArrayContains(array, Literal(3, IntegerType)), true)
    checkEvaluation(SortedArrayContains(array, Literal(4, IntegerType)), false)
  }

  test("SortedArrayContains works correctly for an int array.") {
    val values = Seq.range(0, 50).map(_ * 2)
    val array = createArray(values, IntegerType)
    values.foreach(v =>
      checkEvaluation(SortedArrayContains(array, Literal(v, IntegerType)), true))
    checkEvaluation(SortedArrayContains(array, Literal(-10, IntegerType)), false)
    checkEvaluation(SortedArrayContains(array, Literal(1, IntegerType)), false)
    checkEvaluation(SortedArrayContains(array, Literal(49, IntegerType)), false)
    checkEvaluation(SortedArrayContains(array, Literal(1000, IntegerType)), false)
  }

  test("SortedArrayContains works correctly for a long array.") {
    val values = Seq.range(0L, 50L).map(_ * 2)
    val array = createArray(values, LongType)
    values.foreach(v => checkEvaluation(SortedArrayContains(array, Literal(v, LongType)), true))
    checkEvaluation(SortedArrayContains(array, Literal(-10L, LongType)), false)
    checkEvaluation(SortedArrayContains(array, Literal(1L, LongType)), false)
    checkEvaluation(SortedArrayContains(array, Literal(49L, LongType)), false)
    checkEvaluation(SortedArrayContains(array, Literal(1000L, LongType)), false)
  }

  test("SortedArrayContains works correctly for a string array.") {
    val values = Seq("hello", "world", "foo", "bar", "footrix").sorted
    val array = createArray(values, StringType)
    values.foreach(v =>
      checkEvaluation(SortedArrayContains(array, Literal.create(v, StringType)), true))
    checkEvaluation(SortedArrayContains(array, Literal.create("abc", StringType)), false)
    checkEvaluation(SortedArrayContains(array, Literal.create("fooo", StringType)), false)
    checkEvaluation(SortedArrayContains(array, Literal.create("zoo", StringType)), false)
  }

  test("SortedArrayContains returns null if the value is null.") {
    val array = createArray(Seq(1), IntegerType)
    checkEvaluation(SortedArrayContains(array, Literal(null, IntegerType)), null)
  }
}
