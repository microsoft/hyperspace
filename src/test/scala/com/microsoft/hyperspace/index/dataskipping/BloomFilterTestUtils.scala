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

package com.microsoft.hyperspace.index.dataskipping

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.index.dataskipping.expressions.BloomFilterEncoderProvider
import com.microsoft.hyperspace.util.SparkTestShims

trait BloomFilterTestUtils {
  def encodeExternal(bf: BloomFilter): Any = {
    val bloomFilterEncoder = BloomFilterEncoderProvider.defaultEncoder
    val data = bloomFilterEncoder.encode(bf)
    val dataType = bloomFilterEncoder.dataType
    dataType match {
      case st: StructType =>
        SparkTestShims.fromRow(RowEncoder(st).resolveAndBind(), data.asInstanceOf[InternalRow])
      case _ =>
        val encoder = RowEncoder(StructType(StructField("x", dataType) :: Nil)).resolveAndBind()
        SparkTestShims.fromRow(encoder, InternalRow(data)).get(0)
    }
  }
}