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

import org.apache.spark.sql.types.DataType
import org.apache.spark.util.sketch.BloomFilter

/**
 * Defines how [[BloomFilter]] should be represented in the Spark DataFrame.
 */
trait BloomFilterEncoder {

  /**
   * Returns the data type of the value in the DataFrame representing [[BloomFilter]].
   */
  def dataType: DataType

  /**
   * Returns a value representing the given [[BloomFilter]]
   * that can be put in the [[InternalRow]].
   */
  def encode(bf: BloomFilter): Any

  /**
   * Returns a [[BloomFilter]] from the value in the DataFrame.
   */
  def decode(value: Any): BloomFilter
}
