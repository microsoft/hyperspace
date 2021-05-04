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

import javax.xml.bind.DatatypeConverter

import com.fasterxml.jackson.annotation.{JsonCreator, JsonValue}

/**
 * Represents a fingerprint, which can be used to identify and compare objects
 * without actually comparing them.
 *
 * The value of a fingerprint is typically calculated by a hash function. To
 * function as a fingerprinting function, the hash function must distribute
 * the input uniformly into hashed values, and the number of bytes of the hash
 * value should be sufficiently large. For example, MD5 produces 128-bit hash
 * values (16 bytes), whereas SHA-1 produces 160-bit hash values (20 bytes).
 *
 * All objects to be identified and compared with each other must have fingerprints
 * created from the same hash function.
 */
case class Fingerprint(value: Vector[Byte]) {

  /**
   * Returns a human-readable form of this fingerprint in HEX format.
   */
  @JsonValue
  override def toString: String = {
    value.map(_.formatted("%02x")).mkString
  }

  /**
   * Returns a new fingerprint by applying bitwise XOR to this and the other fingerprints.
   */
  def ^(other: Fingerprint): Fingerprint = {
    require(value.size == other.value.size)
    Fingerprint(value.zip(other.value).map { case (x, y) => (x ^ y).toByte })
  }
}

object Fingerprint {

  /**
   * Creates a Fingerprint from a HEX string.
   */
  @JsonCreator
  def apply(value: String): Fingerprint = {
    Fingerprint(DatatypeConverter.parseHexBinary(value).toVector)
  }
}
