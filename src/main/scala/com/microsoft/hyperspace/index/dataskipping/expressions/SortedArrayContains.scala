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
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression, Predicate}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, TypeUtils}

/**
 * Returns true if the sorted array (left) contains the value (right).
 *
 * If the value (right) is null, null is returned.
 *
 * Preconditions (unchecked):
 *   - The array must not be null.
 *   - Elements in the array must be in ascending order.
 *   - The array must not contain null elements.
 *   - The array must not contain duplicate elements.
 */
private[dataskipping] case class SortedArrayContains(left: Expression, right: Expression)
    extends BinaryExpression
    with Predicate {

  override def prettyName: String = "sorted_array_contains"

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val value = right.eval(input)
    if (value != null) {
      val arr = left.eval(input).asInstanceOf[ArrayData]
      val dt = right.dataType
      val n = arr.numElements()
      if (n > 0 &&
        ordering.lteq(arr.get(0, dt), value) &&
        ordering.lteq(value, arr.get(n - 1, dt))) {
        val (found, _) = SortedArrayUtils.binarySearch(arr, dt, ordering, 0, n, value)
        if (found) return true
      }
      return false
    }
    null
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val arr = leftGen.value
    val rightGen = right.genCode(ctx)
    val value = rightGen.value
    val dt = right.dataType
    val n = ctx.freshName("n")
    val binarySearch = SortedArrayUtils.binarySearchCodeGen(ctx, dt)
    val resultCode =
      s"""
         |if (!(${rightGen.isNull})) {
         |  ${leftGen.code}
         |  ${ev.isNull} = false;
         |  int $n = $arr.numElements();
         |  if ($n > 0 &&
         |      !(${ctx.genGreater(dt, CodeGenerator.getValue(arr, dt, "0"), value)}) &&
         |      !(${ctx.genGreater(dt, value, CodeGenerator.getValue(arr, dt, s"$n - 1"))})) {
         |    ${ev.value} = $binarySearch($arr, 0, $n, $value).found();
         |  }
         |}
       """.stripMargin
    ev.copy(code = code"""
      ${rightGen.code}
      boolean ${ev.isNull} = true;
      boolean ${ev.value} = false;
      $resultCode""")
  }

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType)
}
