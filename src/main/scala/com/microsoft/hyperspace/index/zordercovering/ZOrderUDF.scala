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

import java.util

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf

/**
 * Define UDF for generating z-address for each row based on zOrderStruct.
 *
 * @param zOrderStruct List of ZOrderField which should be constructed with
 *                     proper min/max values for each field.
 */
case class ZOrderUDF(zOrderStruct: Seq[ZOrderField]) {
  // TODO Convert UDF to spark Expression for better performance with code generation.
  //  Though it can reduce optimize time, leave it as a backlog as
  //    1) it requires a lot of code work and
  //    2) RepartitionByRange & Sorting cost outweighs z-address calculation time.

  val totalBitLen = zOrderStruct.map(_.bitLen).sum

  // The map of (column name -> bit indexes in z-address for each bit of column value)
  // For example, (colB -> [8, 6, 4, 2]) stands for
  //   1) use 4 bits of colB
  //   2) each bit will be located at [8, 6, 4, 2]-th bit in z-address.
  //
  // As the bit length to represent z-order for each column can vary,
  // use iterators for each length to implement interleaving assignment.
  private val bitIdxMapsVal = {
    var iterators = zOrderStruct.map(z => (z.name, (0 until z.bitLen).reverseIterator)).toMap
    val idxMap = zOrderStruct.map(z => (z.name, ArrayBuffer[Int]())).toMap
    // Assign bit index of z-address to each column value one by one, from highest bit.
    // In this way, all z-order columns can contribute the order.
    var idxInZAddress = totalBitLen - 1
    while (iterators.nonEmpty) {
      iterators = iterators.filter(it =>
        if (it._2.hasNext) {
          it._2.next
          idxMap(it._1).append(idxInZAddress)
          idxInZAddress = idxInZAddress - 1
          true
        } else {
          false
        })
    }
    // Explicitly calls map since mapValues uses iterator
    // which makes spark tasks not serializable.
    idxMap.map(kv => (kv._1, kv._2.reverseIterator.toArray))
  }

  // Calculate Z-address for each row.
  // For each z-order column,
  //   1) get a bit set to represent the order of the column values
  //   2) set the bits in the final zAddressBitSet using the bit index mapping
  //
  // Use Array[Long] type rather than BinaryType to optimize comparison cost.
  val zAddressUdf = udf { (row: Row) =>
    val zAddressBitSet = new util.BitSet(totalBitLen + 1)
    // Set totalBitLen location for byte array comparison.
    zAddressBitSet.set(totalBitLen)
    zOrderStruct.indices.foreach { idx =>
      // Get bitset using the column value.
      val z = zOrderStruct(idx)
      if (!row.isNullAt(idx)) {
        val zBitSet = z.getBitSet(row.getAs[Any](idx))
        // mapping from the bit index in zBitSet to the bit index in zAddress.
        val idxMap = bitIdxMapsVal(z.name)
        var fromIdx = 0
        while (fromIdx >= 0) {
          val nextIdx = zBitSet.nextSetBit(fromIdx)
          if (nextIdx >= 0) {
            zAddressBitSet.set(idxMap(nextIdx))
            fromIdx = nextIdx + 1
          } else {
            fromIdx = -1
          }
        }
      }
    }
    zAddressBitSet.toLongArray.reverse
  }
}

