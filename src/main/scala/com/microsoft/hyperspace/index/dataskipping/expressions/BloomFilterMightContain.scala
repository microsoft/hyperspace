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
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.catalyst.expressions.codegen.Block._

/**
 * Returns true if the bloom filter (left) might contain the value (right).
 *
 * If the value (right) is null, null is returned.
 *
 * Preconditions (unchecked):
 *   - The bloom filter must not be null.
 */
private[dataskipping] case class BloomFilterMightContain(left: Expression, right: Expression)
    extends BinaryExpression
    with Predicate {

  override def prettyName: String = "bloom_filter_might_contain"

  override def nullable: Boolean = true

  override def eval(input: InternalRow): Any = {
    val value = right.eval(input)
    if (value != null) {
      val bfData = left.eval(input)
      val bf = BloomFilterEncoderProvider.defaultEncoder.decode(bfData)
      return BloomFilterUtils.mightContain(bf, value, right.dataType)
    }
    null
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val leftGen = left.genCode(ctx)
    val rightGen = right.genCode(ctx)
    val bloomFilterEncoder =
      BloomFilterEncoderProvider.defaultEncoder.getClass.getCanonicalName.stripSuffix("$")
    val bf = s"$bloomFilterEncoder.decode(${leftGen.value})"
    val result = BloomFilterUtils.mightContainCodegen(bf, rightGen.value, right.dataType)
    val resultCode =
      s"""
         |if (!(${rightGen.isNull})) {
         |  ${leftGen.code}
         |  ${ev.isNull} = false;
         |  ${ev.value} = $result;
         |}
       """.stripMargin
    ev.copy(code = code"""
      ${rightGen.code}
      boolean ${ev.isNull} = true;
      boolean ${ev.value} = false;
      $resultCode""")
  }
}
