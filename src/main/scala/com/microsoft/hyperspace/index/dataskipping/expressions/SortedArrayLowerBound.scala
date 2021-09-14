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
import org.apache.spark.sql.catalyst.expressions.{BinaryExpression, Expression}
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, CodeGenerator, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._
import org.apache.spark.sql.catalyst.util.{ArrayData, TypeUtils}
import org.apache.spark.sql.types.IntegerType

/**
 * Returns the index to the first element in the array (left) which is not less
 * than (greater than or equal to) the value (right), or null if there is no such
 * element.
 *
 * If the value (right) is null, null is returned.
 *
 * Preconditions (unchecked):
 *   - The array must not be null.
 *   - Elements in the array must be in ascending order.
 *   - The array must not contain null elements.
 *   - The array must not contain duplicate elements.
 */
private[dataskipping] case class SortedArrayLowerBound(left: Expression, right: Expression)
    extends BinaryExpression {

  override def prettyName: String = "sorted_array_lower_bound"

  override def dataType: IntegerType = IntegerType

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val value = right.eval(input)
    if (value != null) {
      val arr = left.eval(input).asInstanceOf[ArrayData]
      val dt = right.dataType
      val n = arr.numElements()
      if (n > 0) {
        if (ordering.lteq(value, arr.get(0, dt))) {
          return 1
        }
        if (ordering.lteq(value, arr.get(n - 1, dt))) {
          val (_, index) = SortedArrayUtils.binarySearch(arr, dt, ordering, 0, n, value)
          return index + 1
        }
      }
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
    val firstValueInArr = CodeGenerator.getValue(arr, dt, "0")
    val lastValueInArr = CodeGenerator.getValue(arr, dt, s"$n - 1")
    val binarySearch = SortedArrayUtils.binarySearchCodeGen(ctx, dt)
    val resultCode =
      s"""
         |if (!(${rightGen.isNull})) {
         |  ${leftGen.code}
         |  int $n = $arr.numElements();
         |  if ($n > 0) {
         |    if (!(${ctx.genGreater(dt, value, firstValueInArr)})) {
         |      ${ev.isNull} = false;
         |      ${ev.value} = 1;
         |    } else if (!(${ctx.genGreater(dt, value, lastValueInArr)})) {
         |      ${ev.isNull} = false;
         |      ${ev.value} = $binarySearch($arr, 0, $n, $value).index() + 1;
         |    }
         |  }
         |}
       """.stripMargin
    ev.copy(code = code"""
      ${rightGen.code}
      boolean ${ev.isNull} = true;
      int ${ev.value} = 0;
      $resultCode""")
  }

  @transient private lazy val ordering: Ordering[Any] =
    TypeUtils.getInterpretedOrdering(right.dataType)
}
