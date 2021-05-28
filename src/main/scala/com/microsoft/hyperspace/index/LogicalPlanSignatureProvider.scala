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

import scala.util.{Success, Try}

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.util.hyperspace.Utils

import com.microsoft.hyperspace.util.fingerprint.{Fingerprint, FingerprintBuilderFactory, MD5FingerprintBuilderFactory}

/**
 * This trait contains the interface that provides the signature of logical plan.
 *
 * The implementation must have a constructor taking [[FingerprintBuilderFactory]] as an argument.
 */
trait LogicalPlanSignatureProvider {

  // The name of subclass extending trait [[LogicalPlanSignatureProvider]], which will be
  // serialized and stored inside [[Index]] object.
  def name: String = getClass.getName

  /**
   * Interface that provides the signature of logical plan.
   *
   * @param logicalPlan logical plan.
   * @return signature if it can be computed w.r.t signature provider assumptions; Otherwise None.
   */
  def signature(logicalPlan: LogicalPlan): Option[Fingerprint]
}

/**
 * Factory object for LogicalPlanSignatureProvider.
 */
object LogicalPlanSignatureProvider {
  private val fbf: FingerprintBuilderFactory = new MD5FingerprintBuilderFactory

  // Creates a default signature provider.
  def create(): LogicalPlanSignatureProvider = new IndexSignatureProvider(fbf)

  /**
   * Creates a parameterized signature provider.
   *
   * @param name fully-qualified class name of signature provider.
   * @return signature provider.
   */
  def create(name: String): LogicalPlanSignatureProvider = {
    Try(
      Utils
        .classForName(name)
        .getConstructor(classOf[FingerprintBuilderFactory])
        .newInstance(fbf)) match {
      case Success(provider: LogicalPlanSignatureProvider) => provider
      case _ =>
        throw new IllegalArgumentException(
          s"Signature provider with name $name is not supported.")
    }
  }
}
