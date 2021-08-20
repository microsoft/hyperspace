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

package com.microsoft.hyperspace.index.dataskipping.util

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import com.microsoft.hyperspace.index.HyperspaceSuite

class ArrayUtilsTest extends HyperspaceSuite {
  test("toArray returns Array[Boolean].") {
    assert(
      ArrayUtils
        .toArray(Seq(true, false), BooleanType)
        .asInstanceOf[Array[Boolean]]
        .sameElements(Array(true, false)))
  }

  test("toArray returns Array[Byte].") {
    assert(
      ArrayUtils
        .toArray(Seq(0, 1, 2, 10).map(_.toByte), ByteType)
        .asInstanceOf[Array[Byte]]
        .sameElements(Array[Byte](0, 1, 2, 10)))
  }

  test("toArray returns Array[Short].") {
    assert(
      ArrayUtils
        .toArray(Seq(0, 1, 2, 10).map(_.toShort), ShortType)
        .asInstanceOf[Array[Short]]
        .sameElements(Array[Short](0, 1, 2, 10)))
  }

  test("toArray returns Array[Int].") {
    assert(
      ArrayUtils
        .toArray(Seq(0, 1, 2, 10), IntegerType)
        .asInstanceOf[Array[Int]]
        .sameElements(Array(0, 1, 2, 10)))
  }

  test("toArray returns Array[Long].") {
    assert(
      ArrayUtils
        .toArray(Seq(0L, 1L, 2L, 10L), LongType)
        .asInstanceOf[Array[Long]]
        .sameElements(Array(0L, 1L, 2L, 10L)))
  }

  test("toArray returns Array[Float].") {
    assert(
      ArrayUtils
        .toArray(Seq(0.0f, 1.0f, 2.0f, 10.0f), FloatType)
        .asInstanceOf[Array[Float]]
        .sameElements(Array(0.0f, 1.0f, 2.0f, 10.0f)))
  }

  test("toArray returns Array[Double].") {
    assert(
      ArrayUtils
        .toArray(Seq(0.0, 1.0, 2.0, 10.0), DoubleType)
        .asInstanceOf[Array[Double]]
        .sameElements(Array(0.0, 1.0, 2.0, 10.0)))
  }

  test("toArray returns Array[Any] for non-primitive types.") {
    assert(
      ArrayUtils
        .toArray(Seq(UTF8String.fromString("foo"), UTF8String.fromString("bar")), StringType)
        .asInstanceOf[Array[Any]]
        .sameElements(Array(UTF8String.fromString("foo"), UTF8String.fromString("bar"))))
  }
}
