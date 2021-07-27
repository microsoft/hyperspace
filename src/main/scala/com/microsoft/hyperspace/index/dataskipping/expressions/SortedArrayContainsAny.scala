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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Expression, Predicate, UnaryExpression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode, FalseLiteral}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, TypeUtils}
import org.apache.spark.sql.types.DataType

/**
 * Returns true if the sorted array (child) contains any of the values.
 *
 * If either array is empty, false is returned.
 *
 * Preconditions (unchecked):
 *   - Both arrays must not be null.
 *   - Elements in the arrays must be in ascending order.
 *   - The left array should not contain duplicate elements.
 *   - The arrays must not contain null elements.
 *
 * If the element type can be represented as a primitive type in Scala,
 * then the right array must be an array of the primitive type.
 */
private[dataskipping] case class SortedArrayContainsAny(
    child: Expression,
    values: Any,
    elementType: DataType)
    extends UnaryExpression
    with Predicate {

  override def prettyName: String = "sorted_array_contains_any"

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Boolean = {
    val arr1 = child.eval(input).asInstanceOf[ArrayData]
    val arr2 = values.asInstanceOf[Array[_]]
    val dt = elementType
    val n = arr1.numElements()
    val m = arr2.length
    if (n > 0 && m > 0 &&
      ordering.lteq(arr1.get(0, dt), arr2(m - 1)) &&
      ordering.lteq(arr2(0), arr1.get(n - 1, dt))) {
      var i = 0
      var j = 0
      do {
        val v = arr1.get(i, dt)
        while (j < m && ordering.lt(arr2(j), v)) j += 1
        if (j == m) return false
        val u = arr2(j)
        j += 1
        val (found, k) = SortedArrayUtils.binarySearch(arr1, dt, ordering, i, n, u)
        if (found) return true
        if (k == n) return false
        i = k
      } while (j < m)
    }
    false
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx)
    val arr1 = childGen.value
    val arr2 = ctx.freshName("values")
    val dt = elementType
    val javaType = CodeGenerator.javaType(dt)
    val arrayType = if (values.isInstanceOf[Array[Any]]) "java.lang.Object[]" else s"$javaType[]"
    val valuesRef = ctx.addReferenceObj("values", values, arrayType)
    val n = ctx.freshName("n")
    val m = ctx.freshName("m")
    val i = ctx.freshName("i")
    val j = ctx.freshName("j")
    val v = ctx.freshName("v")
    val u = ctx.freshName("u")
    val result = ctx.freshName("result")
    val binarySearchResultType =
      SortedArrayUtils.BinarySearchResult.getClass.getCanonicalName.stripSuffix("$")
    val binarySearch = SortedArrayUtils.binarySearchCodeGen(ctx, dt)
    import CodeGenerator.getValue
    val resultCode =
      s"""
         |int $n = $arr1.numElements();
         |int $m = $arr2.length;
         |if ($n > 0 && $m > 0 &&
         |    !(${ctx.genGreater(dt, getValue(arr1, dt, "0"), s"(($javaType) $arr2[$m - 1])")}) &&
         |    !(${ctx.genGreater(dt, s"(($javaType)$arr2[0])", getValue(arr1, dt, s"$n - 1"))})) {
         |  int $i = 0;
         |  int $j = 0;
         |  do {
         |    $javaType $v = ${getValue(arr1, dt, i)};
         |    while ($j < $m && ${ctx.genGreater(dt, v, s"(($javaType) $arr2[$j])")}) $j += 1;
         |    if ($j == $m) break;
         |    $javaType $u = ($javaType) $arr2[$j];
         |    $j += 1;
         |    $binarySearchResultType $result = $binarySearch($arr1, $i, $n, $u);
         |    if ($result.found()) {
         |      ${ev.value} = true;
         |      break;
         |    }
         |    if ($result.index() == $n) break;
         |    $i = $result.index();
         |  } while ($j < $m);
         |}
       """.stripMargin
    ev.copy(
      code = code"""
        ${childGen.code}
        $arrayType $arr2 = $valuesRef;
        boolean ${ev.value} = false;
        $resultCode""",
      isNull = FalseLiteral)
  }

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(elementType)
}
