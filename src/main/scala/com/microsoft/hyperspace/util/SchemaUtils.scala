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

object SchemaUtils {

  val NESTED_FIELD_PREFIX = "__hs_nested."
  val NESTED_FIELD_PREFIX_REGEX = "^__hs_nested\\."

  /**
   * The method prefixes a nested field name that hasn't already been prefixed.
   * The field name must be nested (it should contain a `.` and its type
   * should be of [[org.apache.spark.sql.types.StructType]]).
   *
   * The inverse operation is [[removePrefixNestedFieldName]].
   *
   * @param fieldName The nested field name to prefix.
   * @return A new prefixed field name.
   */
  def prefixNestedFieldName(fieldName: String): String = {
    if (fieldName.contains(".") && !fieldName.startsWith(NESTED_FIELD_PREFIX)) {
      s"$NESTED_FIELD_PREFIX$fieldName"
    } else {
      fieldName
    }
  }

  /**
   * The method prefixes the nested field names from a map where the keys are
   * the field names and the values are the nested state of that field
   * which should be the result of [[ResolverUtils.resolve]].
   * The field names that are not marked as nested will not be changed.
   *
   * See [[prefixNestedFieldName]] and [[ResolverUtils.resolve]] methods.
   *
   * @param fieldNames A sequence of tuples of field names and nested status.
   * @return A collection with prefixed nested fields.
   */
  def prefixNestedFieldNames(fieldNames: Seq[(String, Boolean)]): Seq[String] = {
    fieldNames.map {
      case (fieldName, true) =>
        prefixNestedFieldName(fieldName)
      case (fieldName, false) =>
        fieldName
    }
  }

  /**
   * The method removes the prefix from a prefixed nested field name. It returns
   * the original nested field name.
   *
   * The inverse operation is [[prefixNestedFieldName]].
   *
   * @param fieldName The prefixed nested field name from which to remove the prefix.
   * @return The original field name without prefix.
   */
  def removePrefixNestedFieldName(fieldName: String): String = {
    fieldName.replaceAll(NESTED_FIELD_PREFIX_REGEX, "")
  }

  /**
   * The method removes the prefix from a collection of prefixed nested field names.
   * It returns the original sequence of tuples of field names and nested state.
   *
   * The inverse operation is [[prefixNestedFieldNames]].
   *
   * @param fieldNames The collection of prefixed field names.
   * @return A sequence of tuples of field names and nested status.
   */
  def removePrefixNestedFieldNames(fieldNames: Seq[String]): Seq[(String, Boolean)] = {
    fieldNames.map { fieldName =>
      if (SchemaUtils.isFieldNamePrefixed(fieldName)) {
        removePrefixNestedFieldName(fieldName) -> true
      } else {
        fieldName -> false
      }
    }
  }

  /**
   * The method checks if the given field name is prefixed.
   *
   * @param fieldName The field name that to check for prefix.
   * @return True if is prefixed otherwise false.
   */
  def isFieldNamePrefixed(fieldName: String): Boolean = {
    fieldName.startsWith(NESTED_FIELD_PREFIX)
  }

  /**
   * The method checks if the collection of field names contains a nested one
   * by checking the prefix.
   *
   * @param fieldNames The collection of field names to check.
   * @return True is at leas one field name is prefixed.
   */
  def containsNestedFieldNames(fieldNames: Seq[String]): Boolean = {
    fieldNames.exists(isFieldNamePrefixed)
  }
}
