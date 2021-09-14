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

import java.lang.reflect.Field

trait ReflectionHelper {
  def getAccesibleDeclaredField(clazz: Class[_], name: String): Field = {
    val field = clazz.getDeclaredField(name)
    field.setAccessible(true)
    field
  }

  def get(clazz: Class[_], fieldName: String, obj: Any): Any = {
    getAccesibleDeclaredField(clazz, fieldName).get(obj)
  }

  def getInt(clazz: Class[_], fieldName: String, obj: Any): Int = {
    getAccesibleDeclaredField(clazz, fieldName).getInt(obj)
  }

  def getLong(clazz: Class[_], fieldName: String, obj: Any): Long = {
    getAccesibleDeclaredField(clazz, fieldName).getLong(obj)
  }

  def set(clazz: Class[_], fieldName: String, obj: Any, value: Any): Unit = {
    getAccesibleDeclaredField(clazz, fieldName).set(obj, value)
  }

  def setInt(clazz: Class[_], fieldName: String, obj: Any, value: Int): Unit = {
    getAccesibleDeclaredField(clazz, fieldName).setInt(obj, value)
  }

  def setLong(clazz: Class[_], fieldName: String, obj: Any, value: Long): Unit = {
    getAccesibleDeclaredField(clazz, fieldName).setLong(obj, value)
  }
}
