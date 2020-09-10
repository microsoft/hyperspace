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

package com.microsoft.hyperspace.index

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.util.HashingUtils

/**
 * [[PlanSignatureProvider]] provides signature for a logical plan based on
 * the type of operators in it.
 * A plan needs to have at least one operator so its signature can be generated.
 */
class PlanSignatureProvider extends LogicalPlanSignatureProvider {

  /**
   * Generate the signature of logical plan.
   *
   * @param logicalPlan logical plan.
   * @return signature if there is at least one operator in the plan; Otherwise None.
   */
  def signature(logicalPlan: LogicalPlan): Option[String] = {
    var signature = ""
    logicalPlan.foreachUp(p => signature = HashingUtils.md5Hex(signature + p.nodeName))
    signature match {
      case "" => None
      case _ => Some(signature)
    }
  }
}
