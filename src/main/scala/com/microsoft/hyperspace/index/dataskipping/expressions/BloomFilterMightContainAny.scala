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
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.sketch.BloomFilter

/**
 * Returns true if the bloom filter (child) might contain one of the values.
 *
 * Preconditions (unchecked):
 *   - The bloom filter must not be null.
 *   - The values must be an array without nulls.
 *   - If the element type can be represented as a primitive type in Scala,
 *     then the array must be an array of the primitive type.
 */
private[dataskipping] case class BloomFilterMightContainAny(
    child: Expression,
    values: Any,
    elementType: DataType)
    extends UnaryExpression
    with Predicate {

  override def prettyName: String = "bloom_filter_might_contain_any"

  override def nullable: Boolean = false

  override def eval(input: InternalRow): Boolean = {
    val bfData = child.eval(input)
    val bf = BloomFilterEncoderProvider.defaultEncoder.decode(bfData)
    values
      .asInstanceOf[Array[_]]
      .exists(BloomFilterUtils.mightContain(bf, _, elementType))
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    val childGen = child.genCode(ctx);
    val bloomFilterEncoder =
      BloomFilterEncoderProvider.defaultEncoder.getClass.getCanonicalName.stripSuffix("$")
    val bf = ctx.freshName("bf")
    val bfType = classOf[BloomFilter].getCanonicalName
    val javaType = CodeGenerator.javaType(elementType)
    val arrayType = if (values.isInstanceOf[Array[Any]]) "java.lang.Object[]" else s"$javaType[]"
    val valuesRef = ctx.addReferenceObj("values", values, arrayType)
    val valuesArray = ctx.freshName("values")
    val i = ctx.freshName("i")
    val mightContain =
      BloomFilterUtils.mightContainCodegen(bf, s"($javaType) $valuesArray[$i]", elementType)
    val resultCode =
      s"""
         |$bfType $bf = $bloomFilterEncoder.decode(${childGen.value});
         |$arrayType $valuesArray = $valuesRef;
         |for (int $i = 0; $i < $valuesArray.length; $i++) {
         |  if ($mightContain) {
         |    ${ev.value} = true;
         |    break;
         |  }
         |}
       """.stripMargin
    ev.copy(
      code = code"""
        ${childGen.code}
        boolean ${ev.value} = false;
        $resultCode""",
      isNull = FalseLiteral)
  }
}
