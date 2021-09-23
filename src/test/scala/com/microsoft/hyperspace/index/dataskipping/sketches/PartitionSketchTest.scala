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
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.types._

import com.microsoft.hyperspace.index.HyperspaceSuite
import com.microsoft.hyperspace.index.dataskipping.expressions._

class PartitionSketchTest extends QueryTest with HyperspaceSuite {
  import spark.implicits._

  test("indexedColumns returns the indexed column.") {
    val sketch = PartitionSketch(Seq(("A", None)))
    assert(sketch.indexedColumns === Seq("A"))
  }

  test("referencedColumns returns the indexed column.") {
    val sketch = PartitionSketch(Seq(("a", None), ("b", None)))
    assert(sketch.referencedColumns === Seq("a", "b"))
  }

  test("aggregateFunctions returns first aggregation function.") {
    val sketch = PartitionSketch(Seq(("A", None)))
    val aggrs = sketch.aggregateFunctions.map(f => new Column(f.toAggregateExpression))
    val data = Seq(1, 1, 1, 1, 1).toDF("A")
    checkAnswer(data.select(aggrs: _*), Seq(1).toDF)
  }

  test("toString returns a human-readable string representation.") {
    val sketch = PartitionSketch(Seq(("A", None), ("B", None)))
    assert(sketch.toString === "Partition(A, B)")
  }

  test("Two sketches are equal if their columns are equal.") {
    assert(PartitionSketch(Seq(("A", None))) === PartitionSketch(Seq(("A", None))))
    assert(
      PartitionSketch(Seq(("A", Some(LongType)))) !==
        PartitionSketch(Seq(("A", Some(IntegerType)))))
  }

  test("hashCode is reasonably implemented.") {
    assert(
      PartitionSketch(Seq(("A", None))).hashCode === PartitionSketch(Seq(("A", None))).hashCode)
    assert(
      PartitionSketch(Seq(("A", Some(LongType)))).hashCode !==
        PartitionSketch(Seq(("A", Some(IntegerType)))).hashCode)
  }

  val a = AttributeReference("A", IntegerType)(ExprId(0))
  val b = AttributeReference("B", IntegerType)(ExprId(1))
  val sketchA = UnresolvedAttribute("Partition_A")
  val sketchB = UnresolvedAttribute("Partition_B")

  {
    val sketch = PartitionSketch(Seq(("A", Some(IntegerType))))
    val resolvedExprs = Seq(ExpressionUtils.normalize(a))
    val sketchValues = Seq(sketchA)
    val valueExtractor = AttrValueExtractor(Map(a -> sketchA))

    test("convertPredicate converts EqualTo(<col>, <lit>).") {
      assert(
        sketch.convertPredicate(
          EqualTo(a, Literal(42)),
          resolvedExprs,
          sketchValues,
          Map.empty,
          valueExtractor) === Some(EqualTo(sketchA, Literal(42))))
    }

    test("convertPredicate does not convert non-deterministic expression.") {
      assert(
        sketch.convertPredicate(
          EqualTo(a, Rand(42)),
          resolvedExprs,
          sketchValues,
          Map.empty,
          valueExtractor) === None)
    }

    test("convertPredicate does not convert literal.") {
      assert(
        sketch.convertPredicate(
          Literal(42),
          resolvedExprs,
          sketchValues,
          Map.empty,
          valueExtractor) === None)
    }

    test("convertPredicate does not convert conjunction.") {
      assert(
        sketch.convertPredicate(
          And(LessThan(a, Literal(42)), GreaterThan(a, Literal(23))),
          resolvedExprs,
          sketchValues,
          Map.empty,
          valueExtractor) === None)
    }

    test("convertPredicate does not convert disjunction.") {
      assert(
        sketch.convertPredicate(
          Or(LessThan(a, Literal(42)), GreaterThan(a, Literal(23))),
          resolvedExprs,
          sketchValues,
          Map.empty,
          valueExtractor) === None)
    }

    test("convertPredicate does not convert predicate having subquery.") {
      assert(
        sketch.convertPredicate(
          InSubquery(Seq(a), ListQuery(LocalRelation(a))),
          resolvedExprs,
          sketchValues,
          Map.empty,
          valueExtractor) === None)
    }

    test("convertPredicate does not convert predicate having unknown attr.") {
      assert(
        sketch.convertPredicate(
          EqualTo(a, b),
          resolvedExprs,
          sketchValues,
          Map.empty,
          valueExtractor) === None)
    }
  }

  {
    val sketch = PartitionSketch(Seq(("A", Some(IntegerType)), ("B", Some(IntegerType))))
    val resolvedExprs = Seq(a, b).map(ExpressionUtils.normalize)
    val sketchValues = Seq(sketchA, sketchB)
    val valueExtractor = AttrValueExtractor(Map(a -> sketchA, b -> sketchB))

    test("convertPredicate converts EqualTo(<col>, <col>).") {
      assert(
        sketch.convertPredicate(
          EqualTo(a, b),
          resolvedExprs,
          sketchValues,
          Map.empty,
          valueExtractor) === Some(EqualTo(sketchA, sketchB)))
    }
  }
}
