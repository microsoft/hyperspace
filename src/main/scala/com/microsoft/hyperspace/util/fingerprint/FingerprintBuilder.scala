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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.security.MessageDigest

/**
 * A builder class for Fingerprint.
 * A fingerprint is built by hashing added values sequentially.
 * The order of values is significant.
 */
class FingerprintBuilder(private val md: MessageDigest) {

  def add(value: Boolean): FingerprintBuilder = {
    md.update((if (value) 1 else 0).toByte)
    this
  }

  def add(value: Byte): FingerprintBuilder = {
    md.update(value)
    this
  }

  def add(value: Short): FingerprintBuilder = {
    md.update(ByteBuffer.allocate(2).putShort(value).flip().asInstanceOf[ByteBuffer])
    this
  }

  def add(value: Int): FingerprintBuilder = {
    md.update(ByteBuffer.allocate(4).putInt(value).flip().asInstanceOf[ByteBuffer])
    this
  }

  def add(value: Long): FingerprintBuilder = {
    md.update(ByteBuffer.allocate(8).putLong(value).flip().asInstanceOf[ByteBuffer])
    this
  }

  def add(value: Float): FingerprintBuilder = {
    md.update(ByteBuffer.allocate(4).putFloat(value).flip().asInstanceOf[ByteBuffer])
    this
  }

  def add(value: Double): FingerprintBuilder = {
    md.update(ByteBuffer.allocate(8).putDouble(value).flip().asInstanceOf[ByteBuffer])
    this
  }

  def add(value: Char): FingerprintBuilder = {
    md.update(ByteBuffer.allocate(2).putChar(value).flip().asInstanceOf[ByteBuffer])
    this
  }

  def add(value: String): FingerprintBuilder = {
    md.update(value.getBytes(StandardCharsets.UTF_8))
    this
  }

  def add(value: Array[Byte]): FingerprintBuilder = {
    md.update(value)
    this
  }

  def add(value: Fingerprint): FingerprintBuilder = {
    md.update(value.value.toArray)
    this
  }

  /**
   * Builds a fingerprint by hashing added values.
   * After this call is made, values are reset and this object
   * can be used for another fingerprint.
   */
  def build(): Fingerprint = Fingerprint(md.digest().toVector)
}
