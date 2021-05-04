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
import com.microsoft.hyperspace.util.fingerprint.{Fingerprint, FingerprintBuilder, FingerprintBuilderFactory}

/**
 * [[FileBasedSignatureProvider]] provides the logical plan signature based on files in the
 * relation. File metadata, eg. size, modification time and path, of each file in the
 * relation will be used to generate the signature.
 *
 * Note that while the order of files in a single relation does not affect the signature,
 * the order of relations in the plan do affect the signature calculation.
 *
 * If the given logical plan does not have any supported relations, no signature is provided.
 */
class FileBasedSignatureProvider(fbf: FingerprintBuilderFactory)
    extends LogicalPlanSignatureProvider {

  /**
   * Generate the signature of logical plan.
   *
   * @param logicalPlan logical plan of data frame.
   * @return signature, if the logical plan has supported relations; Otherwise None.
   */
  def signature(logicalPlan: LogicalPlan): Option[Fingerprint] = {
    val provider = Hyperspace.getContext.sourceProviderManager
    val fb: FingerprintBuilder = fbf.create
    var updated = false
    logicalPlan.foreachUp {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        provider.getRelation(l).signature(fb).foreach { f =>
          fb.add(f)
          updated = true
        }
      case _ =>
    }
    if (updated) Some(fb.build()) else None
  }
}
