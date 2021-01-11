/*
 * Copyright (2020) The Hyperspace Project Authors.
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

package com.microsoft.hyperspace.util

import java.util.UUID

import org.apache.spark.SparkFunSuite

class HashingUtilsTest extends SparkFunSuite {
  test("For md5Hashing(), same input has the same output hash code.") {
    val randomUUID = UUID.randomUUID.toString
    val hashCode1 = HashingUtils.md5Hex(randomUUID)
    val hashCode2 = HashingUtils.md5Hex(randomUUID)
    assert(hashCode1.equals(hashCode2))
  }

  test("For md5Hashing(), different input different output hash code.") {
    val randomUUID1 = UUID.randomUUID.toString
    val randomUUID2 = UUID.randomUUID.toString
    val hashCode1 = HashingUtils.md5Hex(randomUUID1)
    val hashCode2 = HashingUtils.md5Hex(randomUUID2)
    assert(!hashCode1.equals(hashCode2))
  }
}
