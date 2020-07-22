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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexManager, LogicalPlanSignatureProvider}

object RuleUtils {

  /**
   * Get active indexes for the given logical plan by matching signatures.
   *
   * @param indexManager indexManager
   * @param plan logical plan
   * @return indexes built for this plan
   */
  def getCandidateIndexes(indexManager: IndexManager, plan: LogicalPlan): Seq[IndexLogEntry] = {
    getLogicalRelation(plan) match {
      case Some(r) =>
        // Map of a signature provider to a signature generated for the given plan.
        val signatureMap: mutable.Map[String, String] = mutable.Map()

        def signatureValid(entry: IndexLogEntry): Boolean = {
          val sourcePlanSignatures =
            entry.source.plan.properties.fingerprint.properties.signatures
          assert(sourcePlanSignatures.length == 1)
          val sourcePlanSignature = sourcePlanSignatures.head

          if (!signatureMap.contains(sourcePlanSignature.provider)) {
            val signature = LogicalPlanSignatureProvider
              .create(sourcePlanSignature.provider)
              .signature(r)
              .get // It is safe to call Option.get as signature() is
            // called on a LogicalRelation node which is
            // valid for signature computation
            signatureMap.put(sourcePlanSignature.provider, signature)
          }

          signatureMap(sourcePlanSignature.provider).equals(sourcePlanSignature.value)
        }

        // TODO: the following check only considers indexes in ACTIVE state for usage. Update
        //  the code to support indexes in transitioning states as well.
        val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))
        allIndexes.filter(index => index.created && signatureValid(index))

      case None => Seq[IndexLogEntry]()
    }
  }

  /**
   * Extract LogicalRelation node from a given logical plan.
   * The assumption is input plan is linear and has one scan node.
   *
   * @param logicalPlan given logical plan to extract LogicalRelation from.
   * @return if the plan is linear, the LogicalRelation node; Otherwise None.
   */
  def getLogicalRelation(logicalPlan: LogicalPlan): Option[LogicalRelation] = {
    val lr = logicalPlan.collect { case r: LogicalRelation => r }
    if (lr.length == 1) {
      Some(lr.head)
    } else {
      None // plan is non-linear or it does not have any LogicalRelation
    }
  }
}
