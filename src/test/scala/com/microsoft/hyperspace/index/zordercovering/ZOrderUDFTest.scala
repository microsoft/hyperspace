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

package com.microsoft.hyperspace.index.zordercovering

import scala.collection.mutable

import org.apache.spark.sql.functions.struct
import org.apache.spark.sql.types._

import com.microsoft.hyperspace.index.HyperspaceSuite

class ZOrderUDFTest extends HyperspaceSuite {

  test("test ZOrderUDF simple") {
    val field1 = ZOrderField.build("intColName", IntegerType, 1, 9, Nil, false)
    val field2 = ZOrderField.build("longColName", LongType, 101L, 109L, Nil, false)

    val zudf = ZOrderUDF(Seq(field1, field2))
    assert(zudf.totalBitLen == 8) // 4 + 4

    def getZAddress(intVal: Int, longVal: Long): Seq[Long] = {
      import spark.implicits._
      val df = Seq((intVal, longVal)).toDF("intColName", "longColName")
      val dfWithZAddr =
        df.withColumn("zaddr", zudf.zAddressUdf(struct(df("intColName"), df("longColName"))))
      val res = dfWithZAddr.head.getValuesMap(dfWithZAddr.schema.fieldNames)
      res("zaddr").asInstanceOf[mutable.WrappedArray[Long]]
    }

    // (1, 100L) => 0b100000000, as we always set (totalBitLen)-th bit.
    // Both are minimum so other bits are 0.
    assert(getZAddress(1, 101L).equals(Seq(256L)))
    // (9, 109L) => 0b111000000 = 256 + 128 + 64 = 448
    assert(getZAddress(9, 109L).equals(Seq(448L)))
    // (1, 109L) => 0b101000000 = 256 + 64 = 320
    assert(getZAddress(1, 109L).equals(Seq(320L)))
    // (9, 101L) => 0b110000000 = 256 + 128 = 384
    assert(getZAddress(9, 101L).equals(Seq(384L)))
    // (5, 101L) => 0b100100000 = 256 + 32 = 288
    assert(getZAddress(5, 101L).equals(Seq(288L)))
    // (8, 101L) => 0b100101010 = 256 + 32 + 8 + 2 = 298
    assert(getZAddress(8, 101L).equals(Seq(298L)))
    // (1, 108L) => 0b100010101 = 256 + 16 + 4 + 1 = 277
    assert(getZAddress(1, 108L).equals(Seq(277L)))
  }
}
