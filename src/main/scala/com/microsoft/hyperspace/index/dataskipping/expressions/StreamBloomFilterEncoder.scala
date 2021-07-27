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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import org.apache.spark.sql.types.BinaryType
import org.apache.spark.util.sketch.BloomFilter

/**
 * A [[BloomFilterEncoder]] implementation based on byte array streams.
 */
object StreamBloomFilterEncoder extends BloomFilterEncoder {
  val dataType: BinaryType = BinaryType

  def encode(bf: BloomFilter): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    bf.writeTo(out)
    out.toByteArray
  }

  def decode(value: Any): BloomFilter = {
    val in = new ByteArrayInputStream(value.asInstanceOf[Array[Byte]])
    BloomFilter.readFrom(in)
  }
}
