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
 * [[IndexNameUtils]] provides utility functions to format the index name as need.
 */
object IndexNameUtils {

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

  def resolve(spark: SparkSession, requiredString: String, availableStrings: String): Boolean = {
    val resolver: Resolver = spark.sessionState.conf.resolver
    resolver(requiredString, availableStrings)
  }

  def resolve(
      spark: SparkSession,
      requiredString: String,
      availableStrings: Iterable[String]): Boolean = {
    availableStrings.exists(resolve(spark, requiredString, _))
  }

  def resolve(
      spark: SparkSession,
      requiredStrings: Iterable[String],
      availableStrings: Iterable[String]): Boolean = {
    requiredStrings.forall(resolve(spark, _, availableStrings))
  }

  def resolvedString(
      spark: SparkSession,
      firstString: String,
      secondString: String): Option[String] = {
    if (resolve(spark, firstString, secondString)) Some(secondString) else None
  }

  def resolvedString(
      spark: SparkSession,
      requiredString: String,
      availableStrings: Iterable[String]): Option[String] = {
    availableStrings.find(resolve(spark, requiredString, _))
  }

  def resolvedString(
      spark: SparkSession,
      requiredStrings: Iterable[String],
      availableStrings: Iterable[String]): Iterable[Option[String]] = {
    requiredStrings.map(resolvedString(spark, _, availableStrings))
  }
}
