package com.microsoft.hyperspace.index
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
