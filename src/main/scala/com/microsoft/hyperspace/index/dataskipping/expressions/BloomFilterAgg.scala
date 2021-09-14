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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.TypedImperativeAggregate
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.sketch.BloomFilter

/**
 * Aggregation function that collects elements in a bloom filter.
 */
private[dataskipping] case class BloomFilterAgg(
    child: Expression,
    expectedNumItems: Long, // expected number of distinct elements
    fpp: Double, // target false positive probability
    override val mutableAggBufferOffset: Int = 0,
    override val inputAggBufferOffset: Int = 0)
    extends TypedImperativeAggregate[BloomFilter] {

  override def prettyName: String = "bloom_filter"

  override def dataType: DataType = bloomFilterEncoder.dataType

  override def nullable: Boolean = false

  override def children: Seq[Expression] = Seq(child)

  override def createAggregationBuffer(): BloomFilter = {
    BloomFilter.create(expectedNumItems, fpp)
  }

  override def update(buffer: BloomFilter, input: InternalRow): BloomFilter = {
    val value = child.eval(input)
    if (value != null) {
      BloomFilterUtils.put(buffer, value, child.dataType)
    }
    buffer
  }

  override def merge(buffer: BloomFilter, input: BloomFilter): BloomFilter = {
    buffer.mergeInPlace(input)
    buffer
  }

  override def eval(buffer: BloomFilter): Any = bloomFilterEncoder.encode(buffer)

  override def serialize(buffer: BloomFilter): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    buffer.writeTo(out)
    out.toByteArray
  }

  override def deserialize(bytes: Array[Byte]): BloomFilter = {
    val in = new ByteArrayInputStream(bytes)
    BloomFilter.readFrom(in)
  }

  override def withNewMutableAggBufferOffset(newOffset: Int): BloomFilterAgg =
    copy(mutableAggBufferOffset = newOffset)

  override def withNewInputAggBufferOffset(newOffset: Int): BloomFilterAgg =
    copy(inputAggBufferOffset = newOffset)

  private def bloomFilterEncoder = BloomFilterEncoderProvider.defaultEncoder
}
