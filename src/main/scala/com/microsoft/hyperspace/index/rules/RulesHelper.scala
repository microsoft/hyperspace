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

package com.microsoft.hyperspace.index.rules

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

object RulesHelper {

  /**
   * Extract LogicalRelation node from a given logical plan.
   * The assumption is the input plan is linear and has one scan node.
   *
   * @param logicalPlan given logical plan to extract LogicalRelation
   * @return if the plan is linear, the LogicalRelation node; Otherwise None.
   */
  def getLogicalRelation(logicalPlan: LogicalPlan): Option[LogicalRelation] =
    logicalPlan match {
      case r: LogicalRelation => Some(r)
      case p: LogicalPlan =>
        if (p.children.size == 1) {
          getLogicalRelation(p.children.head)
        } else {
          None // plan is non-linear or it does not have any LogicalRelation
        }
    }
}
