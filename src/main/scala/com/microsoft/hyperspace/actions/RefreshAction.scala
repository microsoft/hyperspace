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

package com.microsoft.hyperspace.actions

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, REFRESHING}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.telemetry.{AppInfo, HyperspaceEvent, RefreshActionEvent}

// TODO: This class depends directly on LogEntry. This should be updated such that
//   it works with IndexLogEntry only. (for example, this class can take in
//   derivedDataset specific logic for refreshing).
class RefreshAction(
    spark: SparkSession,
    final override protected val logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends CreateActionBase(dataManager)
    with Action {
  private lazy val previousLogEntry: LogEntry = {
    logManager.getLog(baseId).getOrElse {
      throw HyperspaceException("LogEntry must exist for refresh operation")
    }
  }

  private lazy val previousIndexLogEntry = previousLogEntry.asInstanceOf[IndexLogEntry]

  // Reconstruct a df from schema
  private lazy val df = {
    val rels = previousIndexLogEntry.relations
    // Currently we only support to create an index on a LogicalRelation.
    assert(rels.size == 1)
    val dataSchema = DataType.fromJson(rels.head.dataSchemaJson).asInstanceOf[StructType]
    spark.read
      .schema(dataSchema)
      .format(rels.head.fileFormat)
      .options(rels.head.options)
      .load(rels.head.rootPaths: _*)
  }

  private lazy val indexConfig: IndexConfig = {
    val ddColumns = previousIndexLogEntry.derivedDataset.properties.columns
    IndexConfig(previousIndexLogEntry.name, ddColumns.indexed, ddColumns.included)
  }

  final override lazy val logEntry: LogEntry =
    getIndexLogEntry(spark, df, indexConfig, indexDataPath)

  final override val transientState: String = REFRESHING

  final override val finalState: String = ACTIVE

  final override def validate(): Unit = {
    if (!previousIndexLogEntry.state.equalsIgnoreCase(ACTIVE)) {
      throw HyperspaceException(
        s"Refresh is only supported in $ACTIVE state. " +
          s"Current index state is ${previousIndexLogEntry.state}")
    }
  }

  final override def op(): Unit = {
    // TODO: The current implementation picks the number of buckets from session config.
    //   This should be user-configurable to allow maintain the existing bucket numbers
    //   in the index log entry.
    write(spark, df, indexConfig)
  }

  final override protected def event(appInfo: AppInfo, message: String): HyperspaceEvent = {
    RefreshActionEvent(appInfo, logEntry.asInstanceOf[IndexLogEntry], message)
  }
}
