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

import org.apache.commons.codec.digest.DigestUtils
import org.apache.spark.SparkFunSuite

class FingerprintBuilderTest extends SparkFunSuite {

  test("Empty MD5 fingerprint") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.build() == Fingerprint("d41d8cd98f00b204e9800998ecf8427e"))
  }

  test("MD5 fingerprint with an integer") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(1).build() == Fingerprint("f1450306517624a57eafbbf8ed995985"))
  }

  test("MD5 fingerprint with 0") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(0).build() == Fingerprint("f1d3ff8443297732862df21dc4e57262"))
  }

  test("MD5 fingerprint with two integers") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(1).add(42).build() == Fingerprint("f26c63a4633a5deea0a644cc58050446"))
  }

  test("MD5 fingerprint with two integers in a different order") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(42).add(1).build() == Fingerprint("3331d54f38cef13e1a2423af3c6fb223"))
  }

  test("MD5 fingerprint with a long") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(1L).build() == Fingerprint("fa5ad9a8557e5a84cf23e52d3d3adf77"))
  }

  test("MD5 fingerprint with a short") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(1.toShort).build() == Fingerprint("441077cc9e57554dd476bdfb8b8b8102"))
  }

  test("MD5 fingerprint with a byte") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(1.toByte).build() == Fingerprint("55a54008ad1ba589aa210d2629c1df41"))
  }

  test("MD5 fingerprint with a float") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(2.3f).build() == Fingerprint("472d85449cd4441d8e13540a57aeff49"))
  }

  test("MD5 fingerprint with a double") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(42.3).build() == Fingerprint("4a4591ddc290c1e7db42b538ad681549"))
  }

  test("MD5 fingerprint with a boolean") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(true).build() == Fingerprint("55a54008ad1ba589aa210d2629c1df41"))
  }

  test("MD5 fingerprint with a char") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add('X').build() == Fingerprint("1e160201945908b455769e0318b1a1e9"))
  }

  test("MD5 fingerprint with a string") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add("hello").build() == Fingerprint("5d41402abc4b2a76b9719d911017c592"))
  }

  test("MD5 fingerprint with a string whose bytes are same as 1 (Int)") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add("\0\0\0\001").build() == fb.add(1).build())
  }

  test("MD5 fingerprint with a byte array") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(
      fb.add(Array[Byte](104, 101, 108, 108, 111)).build() ==
        Fingerprint("5d41402abc4b2a76b9719d911017c592"))
  }

  test("MD5 fingerprint with a fingerprint") {
    val fb = new FingerprintBuilder(DigestUtils.getMd5Digest)
    assert(fb.add(fb.build()).build() == Fingerprint("59adb24ef3cdbe0297f05b395827453f"))
  }

  test("Empty SHA1 fingerprint") {
    val fb = new FingerprintBuilder(DigestUtils.getSha1Digest)
    assert(fb.build() == Fingerprint("da39a3ee5e6b4b0d3255bfef95601890afd80709"))
  }
}
