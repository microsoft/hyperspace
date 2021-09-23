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
import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.types._

import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.index.dataskipping.ArrayTestUtils

class SortedArrayLowerBoundTest
    extends HyperspaceSuite
    with ArrayTestUtils
    with ExpressionEvalHelper {
  val sortedArrayLowerBound =
    SortedArrayLowerBound(createArray(Seq(0), IntegerType), Literal.create(0, IntegerType))

  test("prettyName returns \"sorted_array_lower_bound\"") {
    assert(sortedArrayLowerBound.prettyName === "sorted_array_lower_bound")
  }

  test("dataType returns IntegerType") {
    assert(sortedArrayLowerBound.dataType === IntegerType)
  }

  test("nullable returns true") {
    assert(sortedArrayLowerBound.nullable === true)
  }

  def expr(dataType: DataType, arr: Seq[Any], value: Any): SortedArrayLowerBound = {
    SortedArrayLowerBound(createArray(arr, dataType), Literal.create(value, dataType))
  }

  test("SortedArrayLowerBound returns null if the array is empty.") {
    checkEvaluation(expr(IntegerType, Nil, 0), null)
  }

  test("SortedArrayLowerBound returns the index if the value is in the array.") {
    checkEvaluation(expr(IntegerType, Seq(1), 1), 1)
    checkEvaluation(expr(IntegerType, Seq(2), 2), 1)
    checkEvaluation(expr(IntegerType, Seq(1, 3), 1), 1)
    checkEvaluation(expr(IntegerType, Seq(1, 3), 3), 2)
    checkEvaluation(expr(IntegerType, Seq(1, 3, 5), 1), 1)
    checkEvaluation(expr(IntegerType, Seq(1, 3, 5), 3), 2)
    checkEvaluation(expr(IntegerType, Seq(1, 3, 5), 5), 3)
    checkEvaluation(expr(DoubleType, Seq(1.5, 3.0, 4.5), 1.5), 1)
    checkEvaluation(expr(DoubleType, Seq(1.5, 3.0, 4.5), 3.0), 2)
    checkEvaluation(expr(DoubleType, Seq(1.5, 3.0, 4.5), 4.5), 3)
    checkEvaluation(expr(StringType, Seq("foo"), "foo"), 1)
    checkEvaluation(expr(StringType, Seq("bar", "foo"), "bar"), 1)
    checkEvaluation(expr(StringType, Seq("bar", "foo"), "foo"), 2)
  }

  test(
    "SortedArrayLowerBound returns the index if the first value in the array " +
      "which is not less than the value.") {
    checkEvaluation(expr(IntegerType, Seq(1), 0), 1)
    checkEvaluation(expr(IntegerType, Seq(1, 3), 0), 1)
    checkEvaluation(expr(IntegerType, Seq(1, 3), 2), 2)
    checkEvaluation(expr(IntegerType, Seq(1, 3, 5), 0), 1)
    checkEvaluation(expr(IntegerType, Seq(1, 3, 5), 2), 2)
    checkEvaluation(expr(IntegerType, Seq(1, 3, 5), 4), 3)
  }

  test(
    "SortedArrayLowerBound returns null if the value is greater than " +
      "the last value in the array.") {
    checkEvaluation(expr(IntegerType, Seq(1), 2), null)
    checkEvaluation(expr(IntegerType, Seq(1, 3), 4), null)
    checkEvaluation(expr(IntegerType, Seq(1, 3, 5), 6), null)
  }

  test("SortedArrayLowerBound returns null if the value is null.") {
    checkEvaluation(expr(IntegerType, Seq(1), null), null)
    checkEvaluation(expr(IntegerType, Seq(1, 3), null), null)
    checkEvaluation(expr(IntegerType, Seq(1, 3, 5), null), null)
  }
}
