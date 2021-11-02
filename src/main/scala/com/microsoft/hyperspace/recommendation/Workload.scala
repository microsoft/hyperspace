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

package com.microsoft.hyperspace.recommendation

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Representation of a Spark SQL query.
 *
 * @param Id The id of the query
 * @param Text The text of the query
 */
case class Query(Id: String, Text: String)

/**
 * Represents a workload, which is a collection of queries.
 */
class Workload {

  // The set of (query, weight) in this workload.
  // Each query is associated with a weight that indicates its importance.
  // One simple way of assigning weights is to make them proportional to their frequencies.
  private val queries = new ListBuffer[(DataFrame, Double)]

  /**
   * Add a query and its weight into the workload.
   *
   * @param df The query in its [[DataFrame]] representation.
   * @param weight The weight of the query.
   */
  def addQuery(df: DataFrame, weight: Double = 1.0): Unit = {
    queries += ((df, weight))
  }

  /**
   * Get all queries and their associated weights.
   *
   * @return A [[Seq]] of queries with their weights.
   */
  def getQueries: Seq[(DataFrame, Double)] = {
    queries.toList
  }
}

object Workload {

  /**
   * Create a workload with the given queries.
   *
   * @param queries The queries in SQL text format.
   * @param spark The Spark session.
   * @return The workload created.
   */
  def create(queries: Seq[Query], spark: SparkSession): Workload = {
    val workload = new Workload
    queries.foreach { query =>
      val df = spark.sql(query.Text)
      workload.addQuery(df)
    }
    workload
  }
}
