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

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.types.{ArrayType, StructField, StructType}

object SchemaUtils {

  val NESTED_FIELD_NEEDLE_REGEX = "\\."
  val NESTED_FIELD_REPLACEMENT = "__"

  def flatten(structFields: Seq[StructField], prefix: Option[String] = None): Seq[String] = {
    structFields.flatMap {
      case StructField(name, StructType(fields), _, _) =>
        flatten(fields, Some(prefix.map(o => s"$o.$name").getOrElse(name)))
      case StructField(name, ArrayType(StructType(fields), _), _, _) =>
        flatten(fields, Some(prefix.map(o => s"$o.$name").getOrElse(name)))
      case other =>
        Seq(prefix.map(o => s"$o.${other.name}").getOrElse(other.name))
    }
  }

  def flatten(attributes: Seq[Attribute]): Seq[String] = {
    attributes.flatMap { a =>
      a.dataType match {
        case struct: StructType =>
          flatten(struct, Some(a.name))
        case _ =>
          Seq(a.name)
      }
    }
  }

  def escapeFieldNames(fields: Seq[String]): Seq[String] = {
    fields.map(escapeFieldName)
  }

  def escapeFieldName(field: String): String = {
    field.replaceAll(NESTED_FIELD_NEEDLE_REGEX, NESTED_FIELD_REPLACEMENT)
  }

  def unescapeFieldNames(fields: Seq[String]): Seq[String] = {
    fields.map(unescapeFieldName)
  }

  def unescapeFieldName(field: String): String = {
    field.replaceAll(NESTED_FIELD_REPLACEMENT, NESTED_FIELD_NEEDLE_REGEX)
  }

  def hasNestedFields(fields: Seq[String]): Boolean = {
    fields.exists(isNestedField)
  }

  def isNestedField(field: String): Boolean = {
    NESTED_FIELD_NEEDLE_REGEX.r.findFirstIn(field).isDefined
  }
}
