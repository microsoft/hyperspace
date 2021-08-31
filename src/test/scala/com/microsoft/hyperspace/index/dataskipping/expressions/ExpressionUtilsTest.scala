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

class ExpressionUtilsTest extends HyperspaceSuite {
  import ExpressionUtils._

  test("normalize removes ExprId and qualifiers for AttributeReference.") {
    val expr = AttributeReference("A", IntegerType)(ExprId(42), Seq("t"))
    val expected = AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId, Nil)
    assert(ExpressionUtils.normalize(expr) === expected)
  }

  test("normalize removes name for GetStructField.") {
    val structType = StructType(StructField("b", IntegerType) :: Nil)
    val expr = GetStructField(Literal(null, structType), 0, Some("b"))
    val expected = GetStructField(Literal(null, structType), 0)
    assert(ExpressionUtils.normalize(expr) === expected)
  }

  test("normalize removes expressions inserted for UDF.") {
    val arg = AttributeReference("A", IntegerType)(ExprId(42), Seq("t"))
    val func = (x: Int) => x + 1
    val expr = If(
      IsNull(arg),
      Literal(null, IntegerType),
      ScalaUDF(func, IntegerType, Seq(KnownNotNull(arg)), Nil))
    val expected =
      ScalaUDF(
        func,
        IntegerType,
        Seq(arg.withExprId(ExpressionUtils.nullExprId).withQualifier(Nil)),
        Nil)
    assert(ExpressionUtils.normalize(expr) === expected)
  }

  test("ExtractIsNullDisjunction matches IsNull.") {
    val expr = IsNull(Literal(null))
    val args = expr match {
      case ExtractIsNullDisjunction(args) => args
    }
    assert(args === Seq(Literal(null)))
  }

  test("ExtractIsNullDisjunction matches Or(IsNull, IsNull).") {
    val expr = Or(IsNull(Literal(null)), IsNull(Literal(42)))
    val args = expr match {
      case ExtractIsNullDisjunction(args) => args
    }
    assert(args === Seq(Literal(null), Literal(42)))
  }

  test("ExtractIsNullDisjunction matches Or(IsNull, Or(IsNull, IsNull)).") {
    val expr = Or(IsNull(Literal(null)), Or(IsNull(Literal(42)), IsNull(Literal(23))))
    val args = expr match {
      case ExtractIsNullDisjunction(args) => args
    }
    assert(args === Seq(Literal(null), Literal(42), Literal(23)))
  }

  test("ExtractIsNullDisjunction does not match other expressions.") {
    val expr = IsNotNull(Literal(null))
    val args = expr match {
      case ExtractIsNullDisjunction(args) => args
      case _ => Nil
    }
    assert(args === Nil)
  }

  test("ExtractKnownNotNullArgs matches Seq(KnownNotNull*).") {
    val exprs = Seq(KnownNotNull(Literal(1)), KnownNotNull(Literal(42)))
    val args = exprs match {
      case ExtractKnownNotNullArgs(args) => args
    }
    assert(args === Seq(Literal(1), Literal(42)))
  }

  test("ExtractKnownNotNullArgs does not match other expressions.") {
    val exprs = Seq(KnownNotNull(Literal(1)), Literal(42))
    val args = exprs match {
      case ExtractKnownNotNullArgs(args) => args
      case _ => Nil
    }
    assert(args === Nil)
  }
}
