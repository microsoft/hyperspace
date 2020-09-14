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
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{FileInfo, IndexLogEntry, IndexManager, LogicalPlanSignatureProvider, PlanSignatureProvider}

object RuleUtils {

  /**
   * Get active indexes for the given logical plan by matching signatures.
   *
   * @param indexManager indexManager
   * @param plan logical plan
   * @param hybridScanEnabled HybridScan config.
   * @return indexes built for this plan
   */
  def getCandidateIndexes(
      indexManager: IndexManager,
      plan: LogicalPlan,
      hybridScanEnabled: Boolean = false): Seq[IndexLogEntry] = {
    // Map of a signature provider to a signature generated for the given plan.
    val signatureMap = mutable.Map[String, Option[String]]()

    def signatureValid(entry: IndexLogEntry): Boolean = {
      val sourcePlanSignatures = entry.source.plan.properties.fingerprint.properties.signatures
      assert(sourcePlanSignatures.length == 1)
      val sourcePlanSignature = sourcePlanSignatures.head
      signatureMap.getOrElseUpdate(
        sourcePlanSignature.provider,
        LogicalPlanSignatureProvider
          .create(sourcePlanSignature.provider)
          .signature(plan)) match {
        case Some(s) => s.equals(sourcePlanSignature.value)
        case None => false
      }
    }

    def isHybridScanCandidate(entry: IndexLogEntry, files: Seq[FileInfo]): Boolean = {
      // TODO Some threshold about the similarity of source data files - number of common
      //  files or total size of common files.
      //  See https://github.com/microsoft/hyperspace/issues/159
      // TODO As in [[PlanSignatureProvider]], Source plan signature comparison is required
      //  to support arbitrary source plans at index creation.
      //  See https://github.com/microsoft/hyperspace/issues/158

      // compare all the metadata of source files
      files.exists(entry.allSourceFileInfos.contains)
    }

    // TODO: the following check only considers indexes in ACTIVE state for usage. Update
    //  the code to support indexes in transitioning states as well.
    //  See https://github.com/microsoft/hyperspace/issues/65
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    if (hybridScanEnabled) {
      val files = plan
        .collect {
          case LogicalRelation(
              HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
              _,
              _,
              _) =>
            location.allFiles.map(f =>
              FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))
        }
        .flatten
      allIndexes.filter(index => index.created && isHybridScanCandidate(index, files))
    } else {
      allIndexes.filter(index => index.created && signatureValid(index))
    }
  }

  /**
   * Extract the LogicalRelation node if the given logical plan is linear.
   *
   * @param logicalPlan given logical plan to extract LogicalRelation from.
   * @return if the plan is linear, the LogicalRelation node; Otherwise None.
   */
  def getLogicalRelation(logicalPlan: LogicalPlan): Option[LogicalRelation] = {
    val lrs = logicalPlan.collect { case r: LogicalRelation => r }
    if (lrs.length == 1) {
      Some(lrs.head)
    } else {
      None // logicalPlan is non-linear or it has no LogicalRelation.
    }
  }
}
