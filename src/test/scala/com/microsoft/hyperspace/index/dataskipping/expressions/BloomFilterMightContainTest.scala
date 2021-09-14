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
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.index.HyperspaceSuite

class BloomFilterMightContainTest extends HyperspaceSuite with ExpressionEvalHelper {
  def test(values: Seq[Any], dataType: DataType): Unit = {
    val bf = BloomFilter.create(values.length, 0.01)
    val bfData = Literal(
      BloomFilterEncoderProvider.defaultEncoder.encode(bf),
      BloomFilterEncoderProvider.defaultEncoder.dataType)
    values.foreach { v =>
      val lit = Literal.create(v, dataType)
      checkEvaluation(BloomFilterMightContain(bfData, lit), bf.mightContain(v))
    }
  }

  test("BloomFilterMightContain works correctly for an int array.") {
    test((0 until 50).map(_ * 2), IntegerType)
  }

  test("BloomFilterMightContain works correctly for a long array.") {
    test((0L until 50L).map(_ * 2), LongType)
  }

  test("BloomFilterMightContain works correctly for a byte array.") {
    test(Seq(0, 1, 3, 7, 15, 31, 63, 127).map(_.toByte), ByteType)
  }

  test("BloomFilterMightContain works correctly for a short array.") {
    test(Seq(1, 3, 5, 7, 9).map(_.toShort), ShortType)
  }

  test("BloomFilterMightContain works correctly for a string array.") {
    test(Seq("hello", "world", "foo", "bar"), StringType)
  }

  test("BloomFilterMightContain works correctly for a binary array.") {
    test(Seq(Array[Byte](1, 2), Array[Byte](3, 4)), BinaryType)
  }

  test("BloomFilterMightContain returns null if the value is null.") {
    val bf = BloomFilter.create(10, 0.01)
    val bfData = Literal(
      BloomFilterEncoderProvider.defaultEncoder.encode(bf),
      BloomFilterEncoderProvider.defaultEncoder.dataType)
    checkEvaluation(BloomFilterMightContain(bfData, Literal(null, IntegerType)), null)
  }
}
