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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StructField, StructType}
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.index.dataskipping.util.ReflectionHelper

/**
 * A [[BloomFilterEncoder]] implementation that avoids copying arrays.
 */
object FastBloomFilterEncoder extends BloomFilterEncoder with ReflectionHelper {
  override val dataType: StructType = StructType(
    StructField("numHashFunctions", IntegerType, nullable = false) ::
      StructField("bitCount", LongType, nullable = false) ::
      StructField("data", ArrayType(LongType, containsNull = false), nullable = false) :: Nil)

  override def encode(bf: BloomFilter): InternalRow = {
    val bloomFilterImplClass = bf.getClass
    val bits = get(bloomFilterImplClass, "bits", bf)
    val bitArrayClass = bits.getClass
    InternalRow(
      getInt(bloomFilterImplClass, "numHashFunctions", bf),
      getLong(bitArrayClass, "bitCount", bits),
      ArrayData.toArrayData(get(bitArrayClass, "data", bits).asInstanceOf[Array[Long]]))
  }

  override def decode(value: Any): BloomFilter = {
    val struct = value.asInstanceOf[InternalRow]
    val numHashFunctions = struct.getInt(0)
    val bitCount = struct.getLong(1)
    val data = struct.getArray(2).toLongArray()

    val bf = BloomFilter.create(1)
    val bloomFilterImplClass = bf.getClass
    val bits = get(bloomFilterImplClass, "bits", bf)
    val bitArrayClass = bits.getClass
    setInt(bloomFilterImplClass, "numHashFunctions", bf, numHashFunctions)
    setLong(bitArrayClass, "bitCount", bits, bitCount)
    set(bitArrayClass, "data", bits, data)
    bf
  }
}
