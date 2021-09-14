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

import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.HyperspaceException

// TODO: Support more types.
// Currently we are relying on org.apache.spark.util.sketch.BloomFilter and
// supported types are restricted by the implementation. To support more types
// without changing the underlying implementation, we can convert Spark values
// to and from byte arrays.
private[dataskipping] object BloomFilterUtils {
  def put(bf: BloomFilter, value: Any, dataType: DataType): Boolean =
    dataType match {
      case LongType => bf.putLong(value.asInstanceOf[Long])
      case IntegerType => bf.putLong(value.asInstanceOf[Int])
      case ByteType => bf.putLong(value.asInstanceOf[Byte])
      case ShortType => bf.putLong(value.asInstanceOf[Short])
      case StringType => bf.putBinary(value.asInstanceOf[UTF8String].getBytes)
      case BinaryType => bf.putBinary(value.asInstanceOf[Array[Byte]])
      case _ => throw HyperspaceException(s"BloomFilter does not support ${dataType}")
    }

  def mightContain(bf: BloomFilter, value: Any, dataType: DataType): Boolean = {
    dataType match {
      case LongType => bf.mightContainLong(value.asInstanceOf[Long])
      case IntegerType => bf.mightContainLong(value.asInstanceOf[Int])
      case ByteType => bf.mightContainLong(value.asInstanceOf[Byte])
      case ShortType => bf.mightContainLong(value.asInstanceOf[Short])
      case StringType => bf.mightContainBinary(value.asInstanceOf[UTF8String].getBytes)
      case BinaryType => bf.mightContainBinary(value.asInstanceOf[Array[Byte]])
      case _ => throw HyperspaceException(s"BloomFilter does not support ${dataType}")
    }
  }

  def mightContainCodegen(bf: String, value: String, dataType: DataType): String = {
    dataType match {
      case LongType | IntegerType | ByteType | ShortType => s"$bf.mightContainLong($value)"
      case StringType => s"$bf.mightContainBinary(($value).getBytes())"
      case BinaryType => s"$bf.mightContainBinary($value)"
      case _ => throw HyperspaceException(s"BloomFilter does not support ${dataType}")
    }
  }
}
