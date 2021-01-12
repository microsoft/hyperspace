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

package com.microsoft.hyperspace.actions

import java.util.Locale

import org.apache.spark.sql.types.StructType

case class IndexSchemaChange(includeColumns: StructType, excludeColumns: Seq[String]) {
  val lowerCaseExcludeColumns = excludeColumns.map(_.toLowerCase(Locale.ROOT))
  val lowerCaseExcludeColumnsSet = excludeColumns.toSet

  if (lowerCaseExcludeColumnsSet.size < lowerCaseExcludeColumns.size) {
    throw new IllegalArgumentException("Duplicate exclude column names are not allowed.")
  }

  if (includeColumns.fieldNames
        .map(_.toLowerCase(Locale.ROOT))
        .exists(lowerCaseExcludeColumnsSet.contains)) {
    throw new IllegalArgumentException(
      "Duplicate column names in include and exclude columns are not allowed.")
  }
}
