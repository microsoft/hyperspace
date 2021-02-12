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

import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.util.HashingUtils

/**
 * [[FileBasedSignatureProvider]] provides the logical plan signature based on files in the
 * relation. File metadata, eg. size, modification time and path, of each file in the
 * relation will be used to generate the signature.
 * If the given logical plan does not have any supported relations, no signature is provided.
 */
class FileBasedSignatureProvider extends LogicalPlanSignatureProvider {

  /**
   * Generate the signature of logical plan.
   *
   * @param logicalPlan logical plan of data frame.
   * @return signature, if the logical plan has supported relations; Otherwise None.
   */
  def signature(logicalPlan: LogicalPlan): Option[String] = {
    fingerprintVisitor(logicalPlan).map(HashingUtils.md5Hex)
  }

  /**
   * Visit logical plan and collect info needed for fingerprint.
   *
   * @param logicalPlan logical plan of data frame.
   * @return fingerprint, if the logical plan has supported relations; Otherwise None.
   */
  private def fingerprintVisitor(logicalPlan: LogicalPlan): Option[String] = {
    val provider = Hyperspace.getContext.sourceProviderManager
    var fingerprint = ""
    logicalPlan.foreachUp {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        fingerprint ++= provider.getRelation(l).signature
      case _ =>
    }

    fingerprint match {
      case "" => None
      case _ => Some(fingerprint)
    }
  }
}
