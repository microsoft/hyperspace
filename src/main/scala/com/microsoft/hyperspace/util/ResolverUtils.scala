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

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, Expression, ExtractValue, GetArrayStructFields, GetMapValue, GetStructField}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.util.ResolverUtils.ResolvedColumn.NESTED_FIELD_PREFIX

/**
 * [[ResolverUtils]] provides utility functions to resolve strings based on spark's resolver.
 */
object ResolverUtils {

  /**
   * [[ResolvedColumn]] stores information when a column name is resolved against the
   * analyzed plan and its schema.
   *
   * Outside unit tests, this object should not be created directly, but via the `resolve` function,
   * or `ResolvedColumn.apply` with a normalized name.
   *
   * @param name The column name resolved from an analyzed plan.
   * @param isNested Flag to denote if this column is nested or not.
   */
  private[hyperspace] case class ResolvedColumn(name: String, isNested: Boolean) {
    assert(!isNested || (name.contains(".") && !name.startsWith(NESTED_FIELD_PREFIX)))
    // Quotes will be removed from `resolve` and nested columns with quotes (e.g., "a.`b.c`.d")
    // are not supported.
    assert(!name.contains("`"))

    // For nested fields, the column name is prefixed with `NESTED_FIELD_PREFIX`.
    lazy val normalizedName = {
      if (isNested) {
        s"$NESTED_FIELD_PREFIX$name"
      } else {
        name
      }
    }

    /**
     * Create a column using the resolved name. Top level column names are quoted, and
     * nested column names are aliased with normalized names.
     *
     * @return [[Column]] object created using the resolved name.
     */
    def toColumn: Column = {
      if (isNested) {
        // No need to quote the string for "as" even if it contains dots.
        col(name).as(normalizedName)
      } else {
        col(quote(name))
      }
    }

    /**
     * Create a column using the normalized name. Since the normalized name is already flattened
     * with "dots", it is quoted.
     *
     * @return [[Column]] object create using the normalized name.
     */
    def toNormalizedColumn: Column = col(quote(normalizedName))

    private def quote(name: String) = s"`$name`"
  }

  private[hyperspace] object ResolvedColumn {
    private val NESTED_FIELD_PREFIX = "__hs_nested."

    /**
     * Given a normalized column name, create [[ResolvedColumn]] after handling the prefix
     * for nested columns.
     *
     * @param normalizedColumnName Normalized column name.
     * @return [[ResolvedColumn]] created from the given normalized column name.
     */
    def apply(normalizedColumnName: String): ResolvedColumn = {
      if (normalizedColumnName.startsWith(NESTED_FIELD_PREFIX)) {
        ResolvedColumn(
          normalizedColumnName.substring(NESTED_FIELD_PREFIX.length),
          isNested = true)
      } else {
        ResolvedColumn(normalizedColumnName, isNested = false)
      }
    }
  }

  /**
   * Return available string if required string can be resolved with it, based on spark resolver.
   *
   * @param resolver Resolver.
   * @param requiredString The string that requires resolution.
   * @param availableString Available string to resolve from.
   * @return Optional available string if resolution is successful, else None
   */
  def resolve(
      resolver: Resolver,
      requiredString: String,
      availableString: String): Option[String] = {
    if (resolver(requiredString, availableString)) Some(availableString) else None
  }

  /**
   * Finds the first matching resolved string from the list of availableStrings, when resolving
   * for requiredString. If no matching string found, return None.
   *
   * @param spark Spark session.
   * @param requiredString The string that requires resolution.
   * @param availableStrings All available strings to resolve from.
   * @return First matching (i.e. resolved) string from availableStrings. If no match is found,
   *         return None.
   */
  def resolve(
      spark: SparkSession,
      requiredString: String,
      availableStrings: Seq[String]): Option[String] = {
    availableStrings.find(resolve(spark.sessionState.conf.resolver, requiredString, _).isDefined)
  }

  /**
   * Finds all resolved strings for requiredStrings, from the list of availableStrings. Returns
   * optional seq of resolved strings if all required strings are resolved, otherwise None.
   *
   * @param spark Spark session.
   * @param requiredStrings List of strings to resolve.
   * @param availableStrings All available strings to resolve from.
   * @return Optional Seq of resolved strings if all required strings are resolved. Else, None.
   */
  def resolve(
      spark: SparkSession,
      requiredStrings: Seq[String],
      availableStrings: Seq[String]): Option[Seq[String]] = {
    Some(requiredStrings.map(resolve(spark, _, availableStrings).getOrElse { return None }))
  }

  /**
   * Finds all resolved strings for requiredStrings, from the given logical plan. Returns
   * optional seq of resolved strings if all required strings are resolved, otherwise None.
   *
   * @param spark Spark session.
   * @param requiredStrings List of strings to resolve.
   * @param plan Logical plan to resolve against.
   * @return Optional sequence of ResolvedColumn objects if all required strings are resolved.
   *         Else, None.
   */
  def resolve(
      spark: SparkSession,
      requiredStrings: Seq[String],
      plan: LogicalPlan): Option[Seq[ResolvedColumn]] = {
    val schema = plan.schema
    val resolver = spark.sessionState.conf.resolver
    val resolved = requiredStrings.map { requiredField =>
      plan
        .resolveQuoted(requiredField, resolver)
        .map { expr =>
          val resolvedColNameParts = extractColumnName(expr)
          validateResolvedColumnName(requiredField, resolvedColNameParts)
          val origColNameParts = getColumnNameFromSchema(schema, resolvedColNameParts, resolver)
          ResolvedColumn(origColNameParts.mkString("."), origColNameParts.length > 1)
        }
        .getOrElse { return None }
    }
    Some(resolved)
  }

  // Extracts the parts of a nested field access path from an expression.
  private def extractColumnName(expr: Expression): Seq[String] = {
    expr match {
      case a: Attribute =>
        Seq(a.name)
      case GetStructField(child, _, Some(name)) =>
        extractColumnName(child) :+ name
      case _: GetArrayStructFields =>
        // TODO: Nested arrays will be supported later
        throw HyperspaceException("Array types are not supported.")
      case _: GetMapValue =>
        // TODO: Nested maps will be supported later
        throw HyperspaceException("Map types are not supported.")
      case Alias(nested: ExtractValue, _) =>
        extractColumnName(nested)
    }
  }

  // Validate the resolved column name by checking if nested columns have dots in its field names.
  private def validateResolvedColumnName(
      origCol: String,
      resolvedColNameParts: Seq[String]): Unit = {
    if (resolvedColNameParts.length > 1 && resolvedColNameParts.exists(_.contains("."))) {
      throw HyperspaceException(
        s"Hyperspace does not support the nested column whose name contains dots: $origCol")
    }
  }

  // Given resolved column name parts, return new column name parts using the name in the given
  // schema to use the original name in order to preserve the casing.
  private def getColumnNameFromSchema(
      schema: StructType,
      resolvedColNameParts: Seq[String],
      resolver: Resolver): Seq[String] =
    resolvedColNameParts match {
      case h :: tail =>
        val field = schema.find(f => resolver(f.name, h)).get
        field match {
          case StructField(name, s: StructType, _, _) =>
            name +: getColumnNameFromSchema(s, tail, resolver)
          case StructField(_, _: ArrayType, _, _) =>
            // TODO: Nested arrays will be supported later
            throw HyperspaceException("Array types are not supported.")
          case StructField(_, _: MapType, _, _) =>
            // TODO: Nested maps will be supported later
            throw HyperspaceException("Map types are not supported")
          case f => Seq(f.name)
        }
      case _ => Nil
    }
}
