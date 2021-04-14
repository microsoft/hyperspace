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
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.execution.{QueryExecution, SQLExecution}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object SparkUtils {

  def toRddWithNewExecutionId(session: SparkSession, qe: QueryExecution): Unit = {
    SQLExecution.withNewExecutionId(qe)(qe.toRdd)
  }

  type DataSourceV2RelationOptions = CaseInsensitiveStringMap

  def asMap(options: DataSourceV2RelationOptions): Map[String, String] = {
    options.asCaseSensitiveMap.asScala.toMap
  }

  def getCaseInsensitive(options: DataSourceV2RelationOptions, key: String): Option[String] = {
    Option(options.get(key))
  }

  object JoinWithoutHint {
    def apply(
        left: LogicalPlan,
        right: LogicalPlan,
        joinType: JoinType,
        condition: Option[Expression]): Join = {
      Join(left, right, joinType, condition, JoinHint.NONE)
    }

    def unapply(join: Join): Option[(LogicalPlan, LogicalPlan, JoinType, Option[Expression])] = {
      Some((join.left, join.right, join.joinType, join.condition))
        .filter(_ => join.hint == JoinHint.NONE)
    }
  }
}
