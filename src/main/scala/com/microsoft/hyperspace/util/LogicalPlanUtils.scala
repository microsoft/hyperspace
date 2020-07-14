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

import scala.collection.mutable

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{IndexLogEntry, LogicalPlanSignatureProvider}

/**
 * Utility functions for logical plan.
 */
object LogicalPlanUtils {

  /**
   * Check if a logical plan is a LogicalRelation.
   * @param logicalPlan logical plan to check.
   * @return true if a logical plan is a LogicalRelation or false.
   */
  def isLogicalRelation(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan match {
      case _: LogicalRelation => true
      case _ => false
    }
  }

  def getAllAvailableIndexes(states: Seq[String] = Seq()): Seq[IndexLogEntry] = {
    Hyperspace
      .getContext(SparkSession.getActiveSession.get)
      .indexCollectionManager
      .getIndexes(states)
  }

  def getCandidateIndexesForPlan(plan: LogicalPlan): Seq[IndexLogEntry] = {
    val signatureMap: mutable.Map[String, String] = mutable.Map()

    def signatureValid(entry: IndexLogEntry): Boolean = {
      val sourcePlanSignatures = entry.source.plan.properties.fingerprint.properties.signatures
      assert(sourcePlanSignatures.length == 1)
      val sourcePlanSignature = sourcePlanSignatures.head

      if (!signatureMap.contains(sourcePlanSignature.provider)) {
        val signature = LogicalPlanSignatureProvider
          .create(sourcePlanSignature.provider)
          .signature(plan)
        signatureMap.put(sourcePlanSignature.provider, signature)
      }
      signatureMap(sourcePlanSignature.provider).equals(sourcePlanSignature.value)
    }

    // TODO: the following check only considers indexes in ACTIVE state for usage. Update
    //  the code to support indexes in transitioning states as well.
    val allIndexes = getAllAvailableIndexes(Seq(Constants.States.ACTIVE))

    allIndexes.filter(index => index.created && signatureValid(index))
  }
}
