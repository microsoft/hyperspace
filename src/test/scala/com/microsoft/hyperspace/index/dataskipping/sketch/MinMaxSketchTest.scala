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

package com.microsoft.hyperspace.index.types.dataskipping.sketch

import org.apache.spark.sql.functions.{max, min}

import com.microsoft.hyperspace.index.types.dataskipping.DataSkippingSuite

class MinMaxSketchTest extends DataSkippingSuite {
  import spark.implicits._

  test("indexedColumns returns the indexed column.") {
    val sketch = MinMaxSketch("A")
    assert(sketch.indexedColumns === Seq("A"))
  }

  test("auxiliaryColumns returns an empty list.") {
    val sketch = MinMaxSketch("A")
    assert(sketch.auxiliaryColumns === Nil)
  }

  test("referencedColumns returns the indexed column.") {
    val sketch = MinMaxSketch("A")
    assert(sketch.referencedColumns === Seq("A"))
  }

  test("withNewColumns works correctly.") {
    val sketch = MinMaxSketch("A")
    val newSketch = sketch.withNewColumns(Map("A" -> "a"))
    assert(newSketch === MinMaxSketch("a"))
  }

  test("aggregateFunctions returns min and max aggregation functions.") {
    val sketch = MinMaxSketch("A")
    val aggrs = sketch.aggregateFunctions
    val data = Seq(1, -1, 10, 2, 4).toDF("A")
    checkAnswer(data.select(aggrs: _*), Seq((-1, 10)).toDF)
  }

  test("numValues returns 2.") {
    val sketch = MinMaxSketch("A")
    assert(sketch.numValues === 2)
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

  test("hashCode is reasonably implemented.") {
    assert(MinMaxSketch("A").hashCode === MinMaxSketch("A").hashCode)
    assert(MinMaxSketch("A").hashCode !== MinMaxSketch("a").hashCode)
  }
}
