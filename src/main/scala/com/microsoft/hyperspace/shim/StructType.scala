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

import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, SerializerProvider}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import org.apache.spark.sql.types.{DataType, StructType}

// Shim to fix StructType serialization with Jackson.
class StructTypeSerializer extends StdSerializer[StructType](classOf[StructType]) {
  override def serialize(
      value: StructType,
      gen: JsonGenerator,
      provider: SerializerProvider): Unit = {
    gen.writeRawValue(value.json)
  }
}

class StructTypeDeserializer extends StdDeserializer[StructType](classOf[StructType]) {
  override def deserialize(p: JsonParser, ctx: DeserializationContext): StructType = {
    DataType.fromJson(p.readValueAsTree().toString).asInstanceOf[StructType]
  }
}
