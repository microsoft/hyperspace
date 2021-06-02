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

/**
 * Base class for sketches which are based on and can be used for a single
 * column.
 *
 * @param sketchName Name of this sketch type to be used for toString
 * @param column Column name this sketch is based on
 */
abstract class SingleColumnSketch(sketchName: String, column: String) extends Sketch {
  final override def withNewColumns(columnMapping: Map[String, String]): Sketch = {
    assert(columnMapping.contains(column))
    withNewColumn(columnMapping(column))
  }

  /**
   * Returns a copy of this sketch with an updated column.
   */
  def withNewColumn(newColumn: String): Sketch

  final override def indexedColumns: Seq[String] = column :: Nil

  final override def auxiliaryColumns: Seq[String] = Nil

  final override def toString: String = s"$sketchName($column)"
}
