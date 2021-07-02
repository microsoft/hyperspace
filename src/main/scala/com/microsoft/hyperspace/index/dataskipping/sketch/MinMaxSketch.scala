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

package com.microsoft.hyperspace.index.dataskipping.sketch

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{max, min}

/**
 * Sketch based on minimum and maximum values for a given column.
 */
case class MinMaxSketch(col: String) extends SingleColumnSketch("MinMax", col) {
  override def withNewColumn(newColumn: String): Sketch = copy(col = newColumn)
  override def aggregateFunctions: Seq[Column] = min(col) :: max(col) :: Nil
  override def numValues: Int = 2
}
