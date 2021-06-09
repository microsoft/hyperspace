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

package com.microsoft.hyperspace.shim

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.util.StdConverter
import org.apache.spark.sql.types.{DataType, StructType}

// Shim for StructType deserialization with Jackson.
// Until Spark 2 StructType couldn't be deserialized with Jackson by default.
// To deserialize StructType, @JsonDeserialize(converter = ...) annotation
// should be used with converter set to the following class.
object StructTypeConverter {
  class T extends StdConverter[JsonNode, StructType] {
    override def convert(value: JsonNode): StructType = {
      DataType.fromJson(value.toString).asInstanceOf[StructType]
    }
  }
}
