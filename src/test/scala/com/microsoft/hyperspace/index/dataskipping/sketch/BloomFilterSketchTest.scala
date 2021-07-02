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

import java.io.ByteArrayOutputStream

import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.index.dataskipping.DataSkippingSuite

class BloomFilterSketchTest extends DataSkippingSuite {
  import spark.implicits._

  test("indexedColumns returns the indexed column.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    assert(sketch.indexedColumns === Seq("A"))
  }

  test("auxiliaryColumns returns an empty list.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    assert(sketch.auxiliaryColumns === Nil)
  }

  test("referencedColumns returns the indexed column.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    assert(sketch.referencedColumns === Seq("A"))
  }

  test("withNewColumns works correctly.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    val newSketch = sketch.withNewColumns(Map("A" -> "a"))
    assert(newSketch === BloomFilterSketch("a", 0.01, 100))
    assert(newSketch.fpp === 0.01)
    assert(newSketch.expectedDistinctCountPerFile === 100)
  }

  test(
    "aggregateFunctions returns an aggregation function that collects values in a bloom filter.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    val aggrs = sketch.aggregateFunctions
    val data = Seq(1, -1, 10, 2, 4, 2, 0, 10)
    val bf = BloomFilter.create(100, 0.01)
    data.foreach(bf.put(_))
    val bfStream = new ByteArrayOutputStream()
    bf.writeTo(bfStream)
    checkAnswer(data.toDF("A").select(aggrs: _*), Seq(bfStream.toByteArray).toDF)
  }

  test("numValues returns 1.") {
    val sketch = BloomFilterSketch("A", 0.01, 100)
    assert(sketch.numValues === 1)
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
}
