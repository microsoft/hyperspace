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

package com.microsoft.hyperspace.util.fingerprint

import org.apache.spark.SparkFunSuite

class FingerprintTest extends SparkFunSuite {

  test("toString returns a HEX string.") {
    assert(Fingerprint(Vector[Byte](1, 2, 3, 10, 30, 66)).toString == "0102030a1e42")
  }

  test("toString returns an empty string.") {
    assert(Fingerprint(Vector[Byte]()).toString == "")
  }

  test("Bitwise XOR works for fingerprints of the same length.") {
    assert(
      (Fingerprint(Vector[Byte](19, 121)) ^ Fingerprint(Vector[Byte](31, -4))) ==
        Fingerprint(Vector[Byte]((19 ^ 31).toByte, (121 ^ -4).toByte)))
  }

  test("Bitwise XOR fails for fingerprints of different lengths.") {
    assertThrows[IllegalArgumentException] {
      Fingerprint(Vector[Byte](19, 121)) ^ Fingerprint(Vector[Byte](31, -4, 3))
    }
  }

  test("Fingerprint can be created from an empty string.") {
    assert(Fingerprint("") == Fingerprint(Vector[Byte]()))
  }

  test("Fingerprint can be created from a HEX string.") {
    assert(Fingerprint("0102fffe") == Fingerprint(Vector[Byte](1, 2, -1, -2)))
  }

  test("Fingerprint cannot be created from an invalid string.") {
    val ex = intercept[IllegalArgumentException] {
      Fingerprint("xyz")
    }
    assert(ex.getMessage.contains("hexBinary needs to be even-length"))
  }
}
