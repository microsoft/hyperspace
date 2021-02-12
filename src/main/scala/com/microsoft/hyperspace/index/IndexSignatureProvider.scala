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
 * [[IndexSignatureProvider]] provides signature for a logical plan based on:
 *  1- Files and their properties in relations of the plan;
 *  2- Overall structure of the plan w.r.t its operators' types.
 *
 * [[FileBasedSignatureProvider]] is used for (1) and [[PlanSignatureProvider]] is used for (2).
 *
 * If the plan does not comply with [[FileBasedSignatureProvider]] or [[PlanSignatureProvider]]
 * requirements for signature computation, then no signature will be provided for the plan.
 */
class IndexSignatureProvider extends LogicalPlanSignatureProvider {
  private val fileBasedSignatureProvider = new FileBasedSignatureProvider
  private val planSignatureProvider = new PlanSignatureProvider

  /**
   * Generate the signature of logical plan.
   *
   * @param logicalPlan logical plan.
   * @return signature, if both [[FileBasedSignatureProvider]] and [[PlanSignatureProvider]]
   *         can generate signature for the logical plan; Otherwise None.
   */
  def signature(logicalPlan: LogicalPlan): Option[String] = {
    fileBasedSignatureProvider.signature(logicalPlan).flatMap { f =>
      planSignatureProvider.signature(logicalPlan).map { p =>
        HashingUtils.md5Hex(f + p)
      }
    }
  }
}
