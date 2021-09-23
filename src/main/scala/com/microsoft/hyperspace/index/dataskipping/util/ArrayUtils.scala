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

package com.microsoft.hyperspace.index.dataskipping.util

import org.apache.spark.sql.types._

object ArrayUtils {

  /**
   * Converts the given sequence of values into an array of values.
   *
   * If the values can be represented as a primitive type,
   * a primitive array is returned.
   */
  def toArray(values: Seq[Any], dataType: DataType): Any = {
    dataType match {
      case BooleanType => values.map(_.asInstanceOf[Boolean]).toArray
      case ByteType => values.map(_.asInstanceOf[Byte]).toArray
      case ShortType => values.map(_.asInstanceOf[Short]).toArray
      case IntegerType => values.map(_.asInstanceOf[Int]).toArray
      case LongType => values.map(_.asInstanceOf[Long]).toArray
      case FloatType => values.map(_.asInstanceOf[Float]).toArray
      case DoubleType => values.map(_.asInstanceOf[Double]).toArray
      case _ => values.toArray
    }
  }
}
