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
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.index.dataskipping.BloomFilterTestUtils
import com.microsoft.hyperspace.index.dataskipping.expressions._

class BloomFilterSketchTest extends QueryTest with HyperspaceSuite with BloomFilterTestUtils {
  import spark.implicits._

  val valueExtractor = AttrValueExtractor(Map.empty)

  test("indexedColumns returns the indexed column.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    assert(sketch.indexedColumns === Seq("A"))
  }

  test("referencedColumns returns the indexed column.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    assert(sketch.referencedColumns === Seq("A"))
  }

  test(
    "aggregateFunctions returns an aggregation function that collects values in a bloom filter.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    val aggrs = sketch.aggregateFunctions.map(f => new Column(f.toAggregateExpression))
    assert(aggrs.length === 1)
    val data = Seq(1, -1, 10, 2, 4, 2, 0, 10)
    val bf = BloomFilter.create(100, 0.01)
    data.foreach(bf.put)
    val bfData = data.toDF("A").select(aggrs.head).collect()(0).getAs[Any](0)
    assert(bfData === encodeExternal(bf))
  }

  test("toString returns a reasonable string.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    assert(sketch.toString === "BloomFilter(A, 0.01, 100)")
  }

  test("Two sketches are equal if their columns are equal.") {
    assert(BloomFilterSketch("A", 0.01, 100) === BloomFilterSketch("A", 0.001, 1000))
    assert(BloomFilterSketch("A", 0.01, 100) !== BloomFilterSketch("a", 0.01, 100))
    assert(BloomFilterSketch("b", 0.01, 100) !== BloomFilterSketch("B", 0.01, 100))
    assert(BloomFilterSketch("B", 0.01, 100) === BloomFilterSketch("B", 0.001, 1000))
  }

  test("hashCode is reasonably implemented.") {
    assert(
      BloomFilterSketch("A", 0.01, 100).hashCode === BloomFilterSketch("A", 0.001, 1000).hashCode)
    assert(
      BloomFilterSketch("A", 0.01, 100).hashCode !== BloomFilterSketch("a", 0.001, 1000).hashCode)
  }

  test("covertPredicate converts EqualTo.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    val predicate = EqualTo(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42))
    val sketchValues = Seq(UnresolvedAttribute("bf"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(BloomFilterMightContain(sketchValues(0), Literal(42)))
    assert(result === expected)
  }

  test("covertPredicate converts EqualTo - string type.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    val predicate =
      EqualTo(AttributeReference("A", StringType)(ExprId(0)), Literal.create("hello", StringType))
    val sketchValues = Seq(UnresolvedAttribute("bf"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", StringType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected =
      Some(BloomFilterMightContain(sketchValues(0), Literal.create("hello", StringType)))
    assert(result === expected)
  }

  test("covertPredicate converts In.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    val predicate =
      In(AttributeReference("A", IntegerType)(ExprId(0)), Seq(Literal(42), Literal(23)))
    val sketchValues = Seq(UnresolvedAttribute("bf"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      Or(
        BloomFilterMightContain(sketchValues(0), Literal(42)),
        BloomFilterMightContain(sketchValues(0), Literal(23))))
    assert(result === expected)
  }

  test("covertPredicate converts In - string type.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    val predicate =
      In(
        AttributeReference("A", StringType)(ExprId(0)),
        Seq(Literal.create("hello", StringType), Literal.create("world", StringType)))
    val sketchValues = Seq(UnresolvedAttribute("bf"))
    val nameMap = Map(ExprId(0) -> "A")
    val result = sketch.convertPredicate(
      predicate,
      Seq(AttributeReference("A", StringType)(ExpressionUtils.nullExprId)),
      sketchValues,
      nameMap,
      valueExtractor)
    val expected = Some(
      Or(
        BloomFilterMightContain(sketchValues(0), Literal.create("hello", StringType)),
        BloomFilterMightContain(sketchValues(0), Literal.create("world", StringType))))
    assert(result === expected)
  }

  test("covertPredicate does not convert Not(EqualTo(<col>, <lit>)).") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    val predicate = Not(EqualTo(AttributeReference("A", IntegerType)(ExprId(0)), Literal(42)))
    val sketchValues = Seq(UnresolvedAttribute("bf"))
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
