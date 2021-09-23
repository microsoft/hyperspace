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

import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.DataType

private[dataskipping] object SortedArrayUtils {

  /**
   * Returns true and the index of the element in the array if the value is
   * contained in the slice [start, end) of the sorted array.
   *
   * If the value is not found, then false and the index of the first element
   * which is greater than the value are returned.
   *
   * Preconditions (unchecked):
   *   - The array must not contain nulls and duplicate elements.
   *   - The value to compare the elements to must not be null.
   */
  def binarySearch(
      arr: ArrayData,
      dataType: DataType,
      ordering: Ordering[Any],
      start: Int,
      end: Int,
      value: Any): (Boolean, Int) = {
    var lb = start
    var ub = end
    while (lb < ub) {
      val i = (lb + ub) / 2
      val u = arr.get(i, dataType)
      val cmp = ordering.compare(value, u)
      if (cmp == 0) {
        return (true, i)
      } else if (cmp < 0) {
        ub = i
      } else {
        lb = i + 1
      }
    }
    (false, lb)
  }

  def binarySearchCodeGen(ctx: CodegenContext, dataType: DataType): String = {
    val javaType = CodeGenerator.javaType(dataType)
    val resultType = BinarySearchResult.getClass.getCanonicalName.stripSuffix("$")
    val funcName = ctx.freshName("binarySearch")
    val funcDef =
      s"""
         |private $resultType $funcName(ArrayData arr, int start, int end, $javaType value) {
         |  int lb = start;
         |  int ub = end;
         |  while (lb < ub) {
         |    int i = (lb + ub) / 2;
         |    $javaType u = ${CodeGenerator.getValue("arr", dataType, "i")};
         |    int cmp = ${ctx.genComp(dataType, "value", "u")};
         |    if (cmp == 0) {
         |      return new $resultType(true, i);
         |    } else if (cmp < 0) {
         |      ub = i;
         |    } else {
         |      lb = i + 1;
         |    }
         |  }
         |  return new $resultType(false, lb);
         |}
       """.stripMargin
    ctx.addNewFunction(funcName, funcDef)
  }

  case class BinarySearchResult(found: Boolean, index: Int)
}
