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

import scala.collection.JavaConverters._

import org.apache.iceberg.Table
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.hive.HiveCatalogs
import org.apache.iceberg.spark.source.IcebergSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.v2.DataSourceOptions

import com.microsoft.hyperspace.util.JavaConverters._

object IcebergShims {

  def isIcebergRelation(plan: LogicalPlan): Boolean =
    plan match {
      case DataSourceV2Relation(_: IcebergSource, _, _, _, _) => true
      case _ => false
    }

  def loadIcebergTable(spark: SparkSession, plan: LogicalPlan): (Table, Option[Long]) = {
    val conf = spark.sessionState.newHadoopConf()
    val options = new DataSourceOptions(plan.asInstanceOf[DataSourceV2Relation].options.asJava)
    val path = options.get("path").asScala.getOrElse {
      throw new IllegalArgumentException("Cannot open table: path is not set")
    }
    val snapshotId = options.get("snapshot-id").asScala.map(_.toLong)
    if (path.contains("/")) {
      val tables = new HadoopTables(conf)
      (tables.load(path), snapshotId)
    } else {
      val hiveCatalog = HiveCatalogs.loadCatalog(conf)
      val tableIdentifier = TableIdentifier.parse(path)
      (hiveCatalog.loadTable(tableIdentifier), snapshotId)
    }
  }
}
