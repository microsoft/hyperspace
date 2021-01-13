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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Utility functions for logical plan.
 */
object LogicalPlanUtils {

  /**
   * Check if a logical plan is supported.
   * @param logicalPlan Logical plan to check.
   * @return true if a logical plan is supported or false.
   */
  def hasSupportedLogicalRelation(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan match {
      case LogicalRelation(_: HadoopFsRelation, _, _, _) => true
      case _: DataSourceV2Relation => true
      case _ => false
    }
  }
}
