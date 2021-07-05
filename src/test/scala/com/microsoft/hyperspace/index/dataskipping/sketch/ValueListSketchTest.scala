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

package com.microsoft.hyperspace.index.dataskipping.sketch

import org.apache.spark.sql.functions.{array_sort, collect_set}

import com.microsoft.hyperspace.index.dataskipping.DataSkippingSuite

class ValueListSketchTest extends DataSkippingSuite {
  import spark.implicits._

  test("indexedColumns returns the indexed column.") {
    val sketch = ValueListSketch("A")
    assert(sketch.indexedColumns === Seq("A"))
  }

  test("auxiliaryColumns returns an empty list.") {
    val sketch = ValueListSketch("A")
    assert(sketch.auxiliaryColumns === Nil)
  }

  test("referencedColumns returns the indexed column.") {
    val sketch = ValueListSketch("A")
    assert(sketch.referencedColumns === Seq("A"))
  }

  test("withNewColumns works correctly.") {
    val sketch = ValueListSketch("A")
    val newSketch = sketch.withNewColumns(Map("A" -> "a"))
    assert(newSketch === ValueListSketch("a"))
  }

  test("aggregateFunctions returns an aggregation function that collects all unique values.") {
    val sketch = ValueListSketch("A")
    val aggrs = sketch.aggregateFunctions
    val data = Seq(1, -1, 10, 2, 4, 2, 0, 10).toDF("A")
    checkAnswer(data.select(aggrs: _*), Seq(Array(-1, 0, 1, 2, 4, 10)).toDF)
  }

  test("numValues returns 1.") {
    val sketch = ValueListSketch("A")
    assert(sketch.numValues === 1)
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
}
