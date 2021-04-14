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

package com.microsoft.hyperspace.util

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.sources.v2.DataSourceOptions

object SparkUtils {

  def toRddWithNewExecutionId(session: SparkSession, qe: QueryExecution): Unit = {
    SQLExecution.withNewExecutionId(session, qe)(qe.toRdd)
  }

  type DataSourceV2RelationOptions = Map[String, String]

  def asMap(options: DataSourceV2RelationOptions): Map[String, String] = {
    options
  }

  def getCaseInsensitive(options: DataSourceV2RelationOptions, key: String): Option[String] = {
    val value = new DataSourceOptions(options.asJava).get(key)
    if (value.isPresent) {
      Some(value.get)
    } else {
      None
    }
  }

  val JoinWithoutHint = Join
}
