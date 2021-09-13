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
import org.apache.spark.unsafe.types.UTF8String

import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.index.dataskipping.expressions._

class ValueListSketchTest extends QueryTest with HyperspaceSuite {
  import spark.implicits._

  val valueExtractor = AttrValueExtractor(Map.empty)

  test("indexedColumns returns the indexed column.") {
    val sketch = ValueListSketch("A")
    assert(sketch.indexedColumns === Seq("A"))
  }

  test("referencedColumns returns the indexed column.") {
    val sketch = ValueListSketch("A")
    assert(sketch.referencedColumns === Seq("A"))
  }

  test("aggregateFunctions returns an aggregation function that collects all unique values.") {
    val sketch = ValueListSketch("A")
    val aggrs = sketch.aggregateFunctions.map(new Column(_))
    val data = Seq(1, -1, 10, 2, 4, 2, 0, 10).toDF("A")
    checkAnswer(data.select(aggrs: _*), Seq(Array(-1, 0, 1, 2, 4, 10)).toDF)
  }

  test("toString returns a reasonable string.") {
    val sketch = ValueListSketch("A")
    assert(sketch.toString === "ValueList(A)")
  }

  test("Two sketches are equal if their columns are equal.") {
    assert(ValueListSketch("A") === ValueListSketch("A"))
    assert(ValueListSketch("A") !== ValueListSketch("a"))
    assert(ValueListSketch("b") !== ValueListSketch("B"))
    assert(ValueListSketch("B") === ValueListSketch("B"))
  }

  test("hashCode is reasonably implemented.") {
    assert(ValueListSketch("A").hashCode === ValueListSketch("A").hashCode)
    assert(ValueListSketch("A").hashCode !== ValueListSketch("a").hashCode)
  }

  test("covertPredicate converts EqualTo(<col>, <lit>).") {
    val sketch = ValueListSketch("A")
    val predicate = EqualTo(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(SortedArrayContains(sketchValues(0), Literal(42)))
    assert(result === expected)
  }

  test("covertPredicate converts EqualTo(<lit>, <col>).") {
    val sketch = ValueListSketch("A")
    val predicate = EqualTo(Literal(42), AttributeReference("A", IntegerType)(ExprId(0)))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(SortedArrayContains(sketchValues(0), Literal(42)))
    assert(result === expected)
  }

  test("covertPredicate converts EqualTo(<col>, <lit>) - string type.") {
    val sketch = ValueListSketch("A")
    val predicate =
      EqualTo(AttributeReference("A", StringType)(ExprId(0)), Literal.create("hello", StringType))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", StringType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(SortedArrayContains(sketchValues(0), Literal.create("hello", StringType)))
    assert(result === expected)
  }

  test("covertPredicate converts EqualTo(<col>, <lit>) - double type.") {
    val sketch = ValueListSketch("A")
    val predicate =
      EqualTo(AttributeReference("A", StringType)(ExprId(0)), Literal(3.14, DoubleType))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", StringType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(SortedArrayContains(sketchValues(0), Literal(3.14, DoubleType)))
    assert(result === expected)
  }

  test("covertPredicate converts Not(EqualTo(<col>, <lit>)).") {
    val sketch = ValueListSketch("A")
    val predicate = Not(EqualTo(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42)))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      And(
        IsNotNull(Literal(42)),
        Or(
          GreaterThan(Size(sketchValues(0)), Literal(1)),
          Not(EqualTo(ElementAt(sketchValues(0), Literal(1)), Literal(42))))))
    assert(result === expected)
  }

  test("covertPredicate converts LessThan.") {
    val sketch = ValueListSketch("A")
    val predicate = LessThan(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(LessThan(ElementAt(sketchValues(0), Literal(1)), Literal(42)))
    assert(result === expected)
  }

  test("covertPredicate converts LessThan - string type.") {
    val sketch = ValueListSketch("A")
    val predicate = LessThan(
      AttributeReference("A", StringType)(ExprId(0)),
      Literal.create("hello", StringType))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", StringType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected =
      Some(LessThan(ElementAt(sketchValues(0), Literal(1)), Literal.create("hello", StringType)))
    assert(result === expected)
  }

  test("covertPredicate converts LessThanOrEqual.") {
    val sketch = ValueListSketch("A")
    val predicate = LessThanOrEqual(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(LessThanOrEqual(ElementAt(sketchValues(0), Literal(1)), Literal(42)))
    assert(result === expected)
  }

  test("covertPredicate converts GreaterThan.") {
    val sketch = ValueListSketch("A")
    val predicate = GreaterThan(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(GreaterThan(ElementAt(sketchValues(0), Literal(-1)), Literal(42)))
    assert(result === expected)
  }

  test("covertPredicate converts GreaterThanOrEqual.") {
    val sketch = ValueListSketch("A")
    val predicate =
      GreaterThanOrEqual(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(GreaterThanOrEqual(ElementAt(sketchValues(0), Literal(-1)), Literal(42)))
    assert(result === expected)
  }

  test("covertPredicate converts In.") {
    val sketch = ValueListSketch("A")
    val predicate =
      In(AttributeReference("A", IntegerType)(ExprId(0)), Seq(Literal(42), Literal(23)))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      Or(
        SortedArrayContains(sketchValues(0), Literal(42)),
        SortedArrayContains(sketchValues(0), Literal(23))))
    assert(result === expected)
  }

  test("covertPredicate converts In - string type.") {
    val sketch = ValueListSketch("A")
    val predicate =
      In(
        AttributeReference("A", StringType)(ExprId(0)),
        Seq(Literal.create("world", StringType), Literal.create("hello", StringType)))
    val sketchValues = Seq(UnresolvedAttribute("valueList"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", StringType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      Or(
        SortedArrayContains(sketchValues(0), Literal.create("world")),
        SortedArrayContains(sketchValues(0), Literal.create("hello"))))
    assert(result === expected)
  }
}
