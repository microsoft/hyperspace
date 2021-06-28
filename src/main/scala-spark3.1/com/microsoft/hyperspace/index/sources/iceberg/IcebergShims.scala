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

package com.microsoft.hyperspace.index.sources.iceberg

import org.apache.iceberg.Table
import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.read.Scan
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, DataSourceV2ScanRelation}
import org.apache.spark.util.hyperspace.Utils

object IcebergShims {

  def isIcebergRelation(plan: LogicalPlan): Boolean =
    plan match {
      case DataSourceV2Relation(_: SparkTable, _, _, _, _) => true
      case DataSourceV2ScanRelation(DataSourceV2Relation(_: SparkTable, _, _, _, _), _, _) => true
      case _ => false
    }

  def loadIcebergTable(spark: SparkSession, plan: LogicalPlan): (Table, Option[Long]) =
    plan match {
      case r @ DataSourceV2Relation(table: SparkTable, _, _, _, _) =>
        (table.table, Option(r.options.get("snapshot-id")).map(_.toLong))
      case DataSourceV2ScanRelation(
            r @ DataSourceV2Relation(table: SparkTable, _, _, _, _),
            _,
            _) =>
        (table.table, Option(r.options.get("snapshot-id")).map(_.toLong))
      case _ =>
        throw new IllegalArgumentException(s"Unexpected plan type: ${plan.getClass.toString}")
    }
}
