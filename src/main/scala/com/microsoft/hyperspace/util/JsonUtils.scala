/*
 * Copyright (2020) The Hyperspace Project Authors.
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

package com.microsoft.hyperspace.util

import com.fasterxml.jackson.annotation.JsonInclude.Include
import com.fasterxml.jackson.core.{JsonGenerator, JsonParser}
import com.fasterxml.jackson.databind.{DeserializationContext, ObjectMapper, SerializerProvider}
import com.fasterxml.jackson.databind.deser.std.StdDeserializer
import com.fasterxml.jackson.databind.module.SimpleModule
import com.fasterxml.jackson.databind.ser.std.StdSerializer
import com.fasterxml.jackson.hyperspace.module.scala.ScalaObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * Useful json functions used around the Hyperspace codebase.
 */
object JsonUtils {

  /** Used to convert between classes and JSON. */
  private val objectMapper = new ObjectMapper() with ScalaObjectMapper
  objectMapper.setSerializationInclusion(Include.ALWAYS)
  objectMapper.registerModule(DefaultScalaModule)

  // Only needed for Spark 2.4
  objectMapper.registerModule {
    new SimpleModule()
      .addSerializer(classOf[StructType], new StructTypeSerializer)
      .addDeserializer(classOf[StructType], new StructTypeDeserializer)
      .addSerializer(classOf[DataType], new DataTypeSerializer)
      .addDeserializer(classOf[DataType], new DataTypeDeserializer)
  }

  def toJson[T: Manifest](obj: T): String = {
    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
  }

  def fromJson[T: Manifest](json: String): T = {
    objectMapper.readValue[T](json)
  }

  def jsonToMap(json: String): Map[String, Any] = {
    objectMapper.readValue[Map[String, Any]](json)
  }

  private class StructTypeSerializer extends StdSerializer[StructType](classOf[StructType]) {
    override def serialize(
        value: StructType,
        gen: JsonGenerator,
        provider: SerializerProvider): Unit = {
      gen.writeRawValue(value.json)
    }
  }

  private class StructTypeDeserializer extends StdDeserializer[StructType](classOf[StructType]) {
    override def deserialize(p: JsonParser, ctx: DeserializationContext): StructType = {
      DataType.fromJson(p.readValueAsTree().toString).asInstanceOf[StructType]
    }
  }

  private class DataTypeSerializer extends StdSerializer[DataType](classOf[DataType]) {
    override def serialize(
        value: DataType,
        gen: JsonGenerator,
        provider: SerializerProvider): Unit = {
      gen.writeRawValue(value.json)
    }
  }

  private class DataTypeDeserializer extends StdDeserializer[DataType](classOf[DataType]) {
    override def deserialize(p: JsonParser, ctx: DeserializationContext): DataType = {
      DataType.fromJson(p.readValueAsTree().toString)
    }
  }
}
