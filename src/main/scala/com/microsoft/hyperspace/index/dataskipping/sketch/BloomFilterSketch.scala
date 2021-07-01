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

package com.microsoft.hyperspace.index.types.dataskipping.sketch

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.column

/**
 * Sketch based on a bloom filter for a given column.
 *
 * Being a probabilistic structure, it is more efficient in terms of the index
 * data size than [[ValueListSketch]] if the number of distinct values for the
 * column is large, but can be less efficient in terms of query optimization
 * than [[ValueListSketch]] due to false positives.
 *
 * Users can specify the target false positive rate and the expected number of
 * distinct values per file. These variables determine the size of the bloom
 * filters and thus the size of the index data.
 *
 * @param col Column name this sketch is based on
 * @param fpp Target false positive rate
 * @param expectedDistinctCountPerFile Expected number of distinct values per file
 */
case class BloomFilterSketch(col: String, fpp: Double, expectedDistinctCountPerFile: Long)
    extends SingleColumnSketch("BloomFilter", col) {

  override def withNewColumn(newColumn: String): Sketch = copy(col = newColumn)

  override def aggregateFunctions: Seq[Column] = {
    new Column(
      BloomFilterAgg(column(col).expr, expectedDistinctCountPerFile, fpp)
        .toAggregateExpression()) :: Nil
  }

  override def numValues: Int = 1

  override def equals(that: Any): Boolean =
    that match {
      case BloomFilterSketch(thatCol, _, _) => col == thatCol
      case _ => false
    }

  override def hashCode: Int = ("BloomFilterSketch", col).hashCode
}
