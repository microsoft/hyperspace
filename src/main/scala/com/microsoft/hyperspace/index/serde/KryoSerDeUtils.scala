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

package com.microsoft.hyperspace.index.serde

import java.io.ByteArrayOutputStream

import com.esotericsoftware.kryo.io.Input
import org.apache.spark.serializer.KryoSerializer

object KryoSerDeUtils {

  /**
   * Serialize the given data payload to an array of bytes using the given Kryo serializer.
   *
   * @param kryoSerializer The Kryo serializer.
   * @param payload The data payload to be serialized.
   * @tparam T The generic type parameter.
   * @return An array of bytes that represent the serialized data.
   */
  def serialize[T](kryoSerializer: KryoSerializer, payload: T): Array[Byte] = {
    val kryo = kryoSerializer.newKryo()

    // Convert payload data to bytes.
    val baos = new ByteArrayOutputStream()
    val output = kryoSerializer.newKryoOutput()
    output.setOutputStream(baos)
    kryo.writeClassAndObject(output, payload)
    output.close()
    baos.toByteArray
  }

  /**
   * Deserialize the given byte array to the required data type using the given Kryo serializer.
   *
   * @param kryoSerializer The Kryo serializer.
   * @param byteArray The byte array that contains the serialized data.
   * @tparam T The generic type parameter
   * @return The data payload with the required data type.
   */
  def deserialize[T](kryoSerializer: KryoSerializer, byteArray: Array[Byte]): T = {
    val kryo = kryoSerializer.newKryo()
    val input = new Input()
    input.setBuffer(byteArray)

    // Read binary data objects back and covert to the required type.
    val data = kryo.readClassAndObject(input)
    data.asInstanceOf[T]
  }
}
