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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, CREATING, DOESNOTEXIST}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.util.LogicalPlanUtils

class CreateAction(
    spark: SparkSession,
    df: DataFrame,
    indexConfig: IndexConfig,
    final override protected val logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends CreateActionBase(dataManager)
    with Action {
  final override lazy val logEntry: LogEntry =
  getIndexLogEntry(spark, df, indexConfig, indexDataPath, sourceFiles(df))

  final override val transientState: String = CREATING

  final override val finalState: String = ACTIVE

  final override def validate(): Unit = {
    // We currently only support createIndex() over HDFS file based scan nodes.
    if (!LogicalPlanUtils.isLogicalRelation(df.queryExecution.optimizedPlan)) {
      throw HyperspaceException(
        "Only creating index over HDFS file based scan nodes is supported.")
    }

    // schema validity checks
    if (!isValidIndexSchema(indexConfig, df.schema)) {
      throw HyperspaceException("Index config is not applicable to dataframe schema.")
    }

    // valid state check
    logManager.getLatestLog() match {
      case None => // valid
      case Some(entry) if entry.state.equals(DOESNOTEXIST) => // valid
      case _ =>
        throw HyperspaceException(
          s"Another Index with name ${indexConfig.indexName} already exists")
    }
  }

  private def isValidIndexSchema(indexConfig: IndexConfig, schema: StructType): Boolean = {
    val validColumnNames = schema.fieldNames
    val indexedColumns = indexConfig.indexedColumns
    val includedColumns = indexConfig.includedColumns
    indexedColumns.forall(validColumnNames.contains) && includedColumns.forall(
      validColumnNames.contains)
  }

  // TODO: The following should be protected, but RefreshAction is calling CreateAction.op().
  //   This needs to be refactored to mark this as protected.
  final override def op(): Unit = write(spark, df, indexConfig)
}
