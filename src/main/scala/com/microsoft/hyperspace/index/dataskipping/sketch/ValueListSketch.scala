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
import org.apache.spark.sql.functions.{array_sort, collect_set}

/**
 * Sketch based on distinct values for a given column.
 *
 * This is not really a sketch, as it stores almost all data for a given
 * column. It can be useful when the number of distinct values is expected to
 * be small and each file tends to store only a subset of the values.
 */
case class ValueListSketch(col: String) extends SingleColumnSketch("ValueList", col) {
  override def withNewColumn(newColumn: String): Sketch = copy(col = newColumn)
  override def aggregateFunctions: Seq[Column] =
    array_sort(collect_set(col)).as(s"value_list($col)") :: Nil
  override def numValues: Int = 1
}
