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

package com.microsoft.hyperspace

import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule

import com.microsoft.hyperspace.index.execution.BucketUnionStrategy
import com.microsoft.hyperspace.index.rules.ApplyHyperspace

/**
 * An extension for Spark SQL to activate Hyperspace.
 *
 * Example to run a `spark-submit` with Hyperspace enabled:
 * {{{
 *   spark-submit -c spark.sql.extensions=com.microsoft.hyperspace.HyperspaceSparkSessionExtension
 * }}}
 *
 * Example to create a `SparkSession` with Hyperspace enabled:
 * {{{
 *    val spark = SparkSession
 *       .builder()
 *       .appName("...")
 *       .master("...")
 *       .config("spark.sql.extensions", "com.microsoft.hyperspace.HyperspaceSparkSessionExtension")
 *       .getOrCreate()
 * }}}
 */
class HyperspaceSparkSessionExtension extends (SparkSessionExtensions => Unit) {

  /**
   * If HyperspaceRule is injected directly to OptimizerRule with HyperspaceExtension,
   * the order of applying rule is different from without HyperspaceExtension
   * (i.e., explicitly calling enableHyperspace). To make behavior consistently,
   * current implementation of HyperspaceExtension uses a trick to call enableHyperspace
   * before rule is applied. Since the interface of injectOptimizerRule should return rule builder,
   * it returns a dummy rule that does nothing. It may increase overhead slightly
   * because enableHyperspace is called once for each evaluation of spark plan.
   */
  private case object DummyRule extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      plan
    }
  }

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { sparkSession =>
      // Enable Hyperspace to leverage indexes.
      sparkSession.addOptimizationsIfNeeded()
      // Return a dummy rule to fit in interface of injectOptimizerRule
      DummyRule
    }
  }
}
