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

package com.microsoft.hyperspace.index.dataskipping.sketches

import org.apache.spark.sql.{Column, QueryTest}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.mockito.Mockito.mock

import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.index.dataskipping.expressions._

class MinMaxSketchTest extends QueryTest with HyperspaceSuite {
  import spark.implicits._

  val valueExtractor = AttrValueExtractor(Map.empty)

  test("indexedColumns returns the indexed column.") {
    val sketch = MinMaxSketch("A")
    assert(sketch.indexedColumns === Seq("A"))
  }

  test("referencedColumns returns the indexed column.") {
    val sketch = MinMaxSketch("A")
    assert(sketch.referencedColumns === Seq("A"))
  }

  test("aggregateFunctions returns min and max aggregation functions.") {
    val sketch = MinMaxSketch("A")
    val aggrs = sketch.aggregateFunctions.map(f => new Column(f.toAggregateExpression))
    val data = Seq(1, -1, 10, 2, 4).toDF("A")
    checkAnswer(data.select(aggrs: _*), Seq((-1, 10)).toDF)
  }

  test("toString returns a reasonable string.") {
    val sketch = MinMaxSketch("A")
    assert(sketch.toString === "MinMax(A)")
  }

  test("Two sketches are equal if their columns are equal.") {
    assert(MinMaxSketch("A") === MinMaxSketch("A"))
    assert(MinMaxSketch("A") !== MinMaxSketch("a"))
    assert(MinMaxSketch("b") !== MinMaxSketch("B"))
    assert(MinMaxSketch("B") === MinMaxSketch("B"))
  }

  test("MinMaxSketch is different from other sketches.") {
    val s1 = MinMaxSketch("A")
    val s2 = mock(classOf[Sketch])
    assert(s1 !== s2)
  }

  test("hashCode is reasonably implemented.") {
    assert(MinMaxSketch("A").hashCode === MinMaxSketch("A").hashCode)
    assert(MinMaxSketch("A").hashCode !== MinMaxSketch("a").hashCode)
  }

  test("convertPredicate converts EqualTo(<col>, <lit>).") {
    val sketch = MinMaxSketch("A")
    val predicate = EqualTo(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      And(
        LessThanOrEqual(sketchValues(0), Literal(42)),
        GreaterThanOrEqual(sketchValues(1), Literal(42))))
    assert(result === expected)
  }

  test("convertPredicate converts EqualTo(<lit>, <col>).") {
    val sketch = MinMaxSketch("A")
    val predicate = EqualTo(Literal(42), AttributeReference("A", IntegerType)(ExprId(0)))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      And(
        LessThanOrEqual(sketchValues(0), Literal(42)),
        GreaterThanOrEqual(sketchValues(1), Literal(42))))
    assert(result === expected)
  }

  test("convertPredicate converts EqualTo(<struct field access>, <lit>).") {
    val sketch = MinMaxSketch("A.C")
    val structAccess = GetStructField(
      AttributeReference("A", StructType(Seq(StructField("C", IntegerType))))(ExprId(0)),
      0)
    val predicate = EqualTo(structAccess, Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(structAccess.transformUp {
        case attr: AttributeReference => attr.withExprId(ExpressionUtils.nullExprId)
      }),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      And(
        LessThanOrEqual(sketchValues(0), Literal(42)),
        GreaterThanOrEqual(sketchValues(1), Literal(42))))
    assert(result === expected)
  }

  test("convertPredicate converts EqualTo(<nested struct field access>, <lit>).") {
    val sketch = MinMaxSketch("A.B.C")
    val structAccess = GetStructField(
      GetStructField(
        AttributeReference(
          "A",
          StructType(Seq(StructField("B", StructType(Seq(StructField("C", IntegerType)))))))(
          ExprId(0)),
        0),
      0)
    val predicate = EqualTo(structAccess, Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(structAccess.transformUp {
        case attr: AttributeReference => attr.withExprId(ExpressionUtils.nullExprId)
      }),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      And(
        LessThanOrEqual(sketchValues(0), Literal(42)),
        GreaterThanOrEqual(sketchValues(1), Literal(42))))
    assert(result === expected)
  }

  test("convertPredicate converts EqualTo(<col>, <lit>) - string type.") {
    val sketch = MinMaxSketch("A")
    val predicate =
      EqualTo(AttributeReference("A", StringType)(ExprId(0)), Literal.create("hello", StringType))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", StringType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      And(
        LessThanOrEqual(sketchValues(0), Literal.create("hello", StringType)),
        GreaterThanOrEqual(sketchValues(1), Literal.create("hello", StringType))))
    assert(result === expected)
  }

  test("convertPredicate converts EqualTo(<col>, <lit>) - double type.") {
    val sketch = MinMaxSketch("A")
    val predicate =
      EqualTo(AttributeReference("A", StringType)(ExprId(0)), Literal(3.14, DoubleType))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", StringType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      And(
        LessThanOrEqual(sketchValues(0), Literal(3.14, DoubleType)),
        GreaterThanOrEqual(sketchValues(1), Literal(3.14, DoubleType))))
    assert(result === expected)
  }

  test("convertPredicate converts LessThan.") {
    val sketch = MinMaxSketch("A")
    val predicate = LessThan(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(LessThan(sketchValues(0), Literal(42)))
    assert(result === expected)
  }

  test("convertPredicate converts LessThan - string type.") {
    val sketch = MinMaxSketch("A")
    val predicate = LessThan(
      AttributeReference("A", StringType)(ExprId(0)),
      Literal.create("hello", StringType))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", StringType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(LessThan(sketchValues(0), Literal.create("hello", StringType)))
    assert(result === expected)
  }

  test("convertPredicate converts LessThanOrEqual.") {
    val sketch = MinMaxSketch("A")
    val predicate = LessThanOrEqual(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(LessThanOrEqual(sketchValues(0), Literal(42)))
    assert(result === expected)
  }

  test("convertPredicate converts GreaterThan.") {
    val sketch = MinMaxSketch("A")
    val predicate = GreaterThan(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(GreaterThan(sketchValues(1), Literal(42)))
    assert(result === expected)
  }

  test("convertPredicate converts GreaterThanOrEqual.") {
    val sketch = MinMaxSketch("A")
    val predicate =
      GreaterThanOrEqual(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(GreaterThanOrEqual(sketchValues(1), Literal(42)))
    assert(result === expected)
  }

  test("convertPredicate converts In.") {
    val sketch = MinMaxSketch("A")
    val predicate =
      In(AttributeReference("A", IntegerType)(ExprId(0)), Seq(Literal(42), Literal(23)))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      Or(
        And(
          LessThanOrEqual(sketchValues(0), Literal(42)),
          GreaterThanOrEqual(sketchValues(1), Literal(42))),
        And(
          LessThanOrEqual(sketchValues(0), Literal(23)),
          GreaterThanOrEqual(sketchValues(1), Literal(23)))))
    assert(result === expected)
  }

  test("convertPredicate converts In - string type.") {
    val sketch = MinMaxSketch("A")
    val predicate =
      In(
        AttributeReference("A", StringType)(ExprId(0)),
        Seq(Literal.create("hello", StringType), Literal.create("world", StringType)))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", StringType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      Or(
        And(
          LessThanOrEqual(sketchValues(0), Literal.create("hello", StringType)),
          GreaterThanOrEqual(sketchValues(1), Literal.create("hello", StringType))),
        And(
          LessThanOrEqual(sketchValues(0), Literal.create("world", StringType)),
          GreaterThanOrEqual(sketchValues(1), Literal.create("world", StringType)))))
    assert(result === expected)
  }

  test("convertPredicate does not convert Not(EqualTo(<col>, <lit>)).") {
    val sketch = MinMaxSketch("A")
    val predicate = Not(EqualTo(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42)))
    val sketchValues = Seq(UnresolvedAttribute("min"), UnresolvedAttribute("max"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = None
    assert(result === expected)
  }
}
