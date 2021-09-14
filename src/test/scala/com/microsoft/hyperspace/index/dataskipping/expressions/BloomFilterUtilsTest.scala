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

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.HyperspaceSuite

class BloomFilterUtilsTest extends HyperspaceSuite {

  def testPut(value: Any, dataType: DataType): Unit = {
    val bf = BloomFilter.create(100, 0.01)
    BloomFilterUtils.put(bf, value, dataType)
    val expected = BloomFilter.create(100, 0.01)
    expected.put(value)
    assert(bf === expected)
  }

  test("put: long") {
    testPut(10L, LongType)
  }

  test("put: int") {
    testPut(10, IntegerType)
  }

  test("put: byte") {
    testPut(10.toByte, ByteType)
  }

  test("put: short") {
    testPut(10.toShort, ShortType)
  }

  test("put: string") {
    val value = UTF8String.fromString("hello")
    val bf = BloomFilter.create(100, 0.01)
    BloomFilterUtils.put(bf, value, StringType)
    val expected = BloomFilter.create(100, 0.01)
    expected.put(value.getBytes)
    assert(bf === expected)
  }

  test("put: binary") {
    testPut(Array[Byte](1, 2, 3, 4), BinaryType)
  }

  test("put throws an exception for unsupported types.") {
    val ex = intercept[HyperspaceException](testPut(3.14, DoubleType))
    assert(ex.msg.contains("BloomFilter does not support DoubleType"))
  }

  def testMightContain(value: Any, value2: Any, dataType: DataType): Unit = {
    val bf = BloomFilter.create(100, 0.01)
    BloomFilterUtils.put(bf, value, dataType)
    assert(BloomFilterUtils.mightContain(bf, value, dataType) === bf.mightContain(value))
    assert(BloomFilterUtils.mightContain(bf, value2, dataType) === bf.mightContain(value2))
  }

  test("mightContain: int") {
    testMightContain(1, 0, IntegerType)
  }

  test("mightContain: long") {
    testMightContain(1L, 0L, LongType)
  }

  test("mightContain: byte") {
    testMightContain(1.toByte, 0.toByte, ByteType)
  }

  test("mightContain: short") {
    testMightContain(1.toShort, 0.toShort, ShortType)
  }

  test("mightContain: string") {
    val value = UTF8String.fromString("hello")
    val value2 = UTF8String.fromString("world")
    val bf = BloomFilter.create(100, 0.01)
    BloomFilterUtils.put(bf, value, StringType)
    assert(
      BloomFilterUtils.mightContain(bf, value, StringType) === bf.mightContain(value.getBytes))
    assert(
      BloomFilterUtils.mightContain(bf, value2, StringType) === bf.mightContain(value2.getBytes))
  }

  test("mightContain: binary") {
    testMightContain(Array[Byte](1, 2), Array[Byte](3, 4), BinaryType)
  }

  test("mightContain throws an exception for unsupported types.") {
    val bf = BloomFilter.create(100, 0.01)
    val ex = intercept[HyperspaceException](BloomFilterUtils.mightContain(bf, 3.14, DoubleType))
    assert(ex.msg.contains("BloomFilter does not support DoubleType"))
  }

  test("mightContainCodegen: int") {
    val code = BloomFilterUtils.mightContainCodegen("fb", "vl", IntegerType)
    assert(code === "fb.mightContainLong(vl)")
  }

  test("mightContainCodegen: long") {
    val code = BloomFilterUtils.mightContainCodegen("fb", "vl", LongType)
    assert(code === "fb.mightContainLong(vl)")
  }

  test("mightContainCodegen: byte") {
    val code = BloomFilterUtils.mightContainCodegen("fb", "vl", ByteType)
    assert(code === "fb.mightContainLong(vl)")
  }

  test("mightContainCodegen: short") {
    val code = BloomFilterUtils.mightContainCodegen("fb", "vl", ShortType)
    assert(code === "fb.mightContainLong(vl)")
  }

  test("mightContainCodegen: string") {
    val code = BloomFilterUtils.mightContainCodegen("fb", "vl", StringType)
    assert(code === "fb.mightContainBinary((vl).getBytes())")
  }

  test("mightContainCodegen: binary") {
    val code = BloomFilterUtils.mightContainCodegen("fb", "vl", BinaryType)
    assert(code === "fb.mightContainBinary(vl)")
  }

  test("mightContainCodegen throws an exception for unsupported types.") {
    val ex =
      intercept[HyperspaceException](BloomFilterUtils.mightContainCodegen("fb", "vl", DoubleType))
    assert(ex.msg.contains("BloomFilter does not support DoubleType"))
  }
}
