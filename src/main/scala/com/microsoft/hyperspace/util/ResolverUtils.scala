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
 * [[ResolverUtils]] provides utility functions to resolve strings based on spark's resolver.
 */
object ResolverUtils {

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
}
