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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.Resolver

/**
 * [[ResolverUtils]] provides utility functions to
 * - format the index name as needed.
 * - resolve strings based on spark's resolver.
 */
object ResolverUtils {

  /**
   * Normalize the index name by trimming space(s) at both ends, and replacing internal space(s)
   * with an underscore.
   *
   * @param indexName the name of index passed in by user.
   * @return a normalized index name.
   */
  def normalizeIndexName(indexName: String): String = {
    indexName.trim.replaceAll("\\s+", "_")
  }

  /**
   * Resolve two strings based on spark's resolver. Returns true if both are resolved.
   *
   * @param spark Spark session.
   * @param requiredString First string.
   * @param availableString Second string.
   * @return True if both are equivalent based on the current spark resolver.
   */
  def isResolved(
      spark: SparkSession,
      requiredString: String,
      availableString: String): Boolean = {
    val resolver: Resolver = spark.sessionState.conf.resolver
    resolver(requiredString, availableString)
  }

  /**
   * Resolve first string from available collection of strings based on spark's resolver. Returns
   * true if any resolved string is found in the collection.
   *
   * @param spark Spark session.
   * @param requiredString String to resolve.
   * @param availableStrings Available strings.
   * @return True if any of the available strings can be resolved with the passed string.
   */
  def isResolved(
      spark: SparkSession,
      requiredString: String,
      availableStrings: Iterable[String]): Boolean = {
    availableStrings.exists(isResolved(spark, requiredString, _))
  }

  /**
   * Resolves ALL of the requiredStrings with availableStrings based on spark's resolver. Returns
   * true if all the requiredStrings can be resolved with some string from availableStrings.
   * Returns false otherwise.
   *
   * @param spark Spark session.
   * @param requiredStrings List of strings to resolve.
   * @param availableStrings List of availble strings to resolve from.
   * @return True if every one of requiredStrings can be resolved from availableStrings. False
   *         otherwise.
   */
  def isResolved(
      spark: SparkSession,
      requiredStrings: Iterable[String],
      availableStrings: Iterable[String]): Boolean = {
    requiredStrings.forall(isResolved(spark, _, availableStrings))
  }

  /**
   * Return available string if required string can be resolved with it, based on spark resolver.
   *
   * @param spark Spark Session.
   * @param requiredString The string that requires resolution.
   * @param availableString Available list of strings to resolve from.
   * @return Optional available string if resolution is successful, else None
   */
  def resolve(
      spark: SparkSession,
      requiredString: String,
      availableString: String): Option[String] = {
    if (isResolved(spark, requiredString, availableString)) Some(availableString) else None
  }

  /**
   * Finds the first matching resolved string from the list of availableStrings, when resolving
   * for requiredString. If no matching string found, return None.
   *
   * @param spark Spark session.
   * @param requiredString The string that requires resolution.
   * @param availableStrings All available strings to find a match from.
   * @return First matching (i.e. resolved) string from availableStrings. If no match is found,
   *         return None.
   */
  def resolve(
      spark: SparkSession,
      requiredString: String,
      availableStrings: Iterable[String]): Option[String] = {
    availableStrings.find(isResolved(spark, requiredString, _))
  }

  /**
   * Finds all resolved strings for requiredStrings, from the list of availableStrings. Returns a
   * sequence of Optional values for matches, None for unmatched strings.
   *
   * @param spark Spark session.
   * @param requiredStrings List of strings to resolve.
   * @param availableStrings List of available strings to resolve from.
   * @return Sequence of optional values of resolved strings or None's.
   */
  def resolve(
      spark: SparkSession,
      requiredStrings: Iterable[String],
      availableStrings: Iterable[String]): Seq[Option[String]] = {
    requiredStrings.map(resolve(spark, _, availableStrings)).toSeq
  }
}
