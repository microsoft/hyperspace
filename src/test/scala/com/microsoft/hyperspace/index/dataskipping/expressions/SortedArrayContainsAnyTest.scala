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
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types._

import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.index.dataskipping.ArrayTestUtils
import com.microsoft.hyperspace.index.dataskipping.util.ArrayUtils.toArray

class SortedArrayContainsAnyTest
    extends HyperspaceSuite
    with ArrayTestUtils
    with ExpressionEvalHelper {
  def test(arr1: Expression, arr2: Expression, expected: Boolean): Unit = {
    val elementType = arr2.dataType.asInstanceOf[ArrayType].elementType
    checkEvaluation(
      SortedArrayContainsAny(
        arr1,
        toArray(
          arr2.asInstanceOf[Literal].value.asInstanceOf[ArrayData].toObjectArray(elementType),
          elementType),
        elementType),
      expected)
  }

  test("SortedArrayContainsAny returns true if two arrays intersect.") {
    val array1 = createArray(Seq.range(0, 100000).map(_ * 2), IntegerType)
    val array2 = createArray(Seq(0), IntegerType)
    val array3 = createArray(Seq(2), IntegerType)
    val array4 = createArray(Seq(199998), IntegerType)
    val array5 = createArray(Seq(2, 4, 5), IntegerType)
    val array6 = createArray(Seq(1, 3, 199998), IntegerType)
    val array7 = createArray(Seq(-1, 100000), IntegerType)
    val array8 = createArray(Seq(100000, 200001), IntegerType)
    test(array1, array2, true)
    test(array1, array3, true)
    test(array1, array4, true)
    test(array1, array5, true)
    test(array1, array6, true)
    test(array1, array7, true)
    test(array1, array8, true)
    test(array3, array5, true)
    test(array4, array6, true)
    test(array7, array8, true)
  }

  test("SortedArrayContainsAny returns false if two arrays don't intersect.") {
    val array1 = createArray(Seq.range(0, 100000).map(_ * 2), IntegerType)
    val array2 = createArray(Seq(), IntegerType)
    val array3 = createArray(Seq(-1), IntegerType)
    val array4 = createArray(Seq(1), IntegerType)
    val array5 = createArray(Seq(200001), IntegerType)
    val array6 = createArray(Seq(1, 3, 199999), IntegerType)
    val array7 = createArray(Seq(-1, 100001), IntegerType)
    val array8 = createArray(Seq(49999, 100001), IntegerType)
    val array9 = createArray(Seq(-3, 1, 1), IntegerType)
    test(array1, array2, false)
    test(array1, array3, false)
    test(array1, array4, false)
    test(array1, array5, false)
    test(array1, array6, false)
    test(array1, array7, false)
    test(array1, array9, false)
    test(array2, array3, false)
    test(array3, array4, false)
    test(array5, array6, false)
    test(array6, array7, false)
  }
}
