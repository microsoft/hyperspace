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

  // In Spark 3, the V2ScanRelationPushdown rule can convert DataSourceV2Relation into
  // DataSourceV2ScanRelation.
  def isIcebergRelation(plan: LogicalPlan): Boolean =
    plan match {
      case DataSourceV2Relation(_: SparkTable, _, _, _, _) => true
      case DataSourceV2ScanRelation(_: SparkTable, _, _) => true
      case _ => false
    }

  def loadIcebergTable(spark: SparkSession, plan: LogicalPlan): (Table, Option[Long]) =
    plan match {
      case r @ DataSourceV2Relation(table: SparkTable, _, _, _, _) =>
        (table.table, Option(r.options.get("snapshot-id")).map(_.toLong))
      case r @ DataSourceV2ScanRelation(table: SparkTable, _, _) =>
        (table.table, getSnapshotId(r.scan))
      case _ =>
        throw new IllegalArgumentException(s"Unexpected plan type: ${plan.getClass.toString}")
    }

  // Temporary hack for Spark 3.0.
  // There is no way to retrieve the snapshot id from DataSourceV2ScanRelation in Spark 3.0.
  // We need to get it from the private field of SparkBatchQueryScan which is also not public.
  // This hack won't be needed for Spark 3.1, as DataSourceV2ScanRelation will include
  // DataSourceV2Relation and we can access the options.
  private lazy val snapshotIdField = {
    val f = Utils
      .classForName("org.apache.iceberg.spark.source.SparkBatchQueryScan")
      .getDeclaredField("snapshotId")
    f.setAccessible(true)
    f
  }

  private def getSnapshotId(scan: Scan): Option[Long] = {
    Option(snapshotIdField.get(scan)).map(_.asInstanceOf[Long])
  }
}
