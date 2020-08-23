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

package com.microsoft

import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.index.execution.BucketUnionStrategy
import com.microsoft.hyperspace.index.rules.{FilterIndexRule, JoinIndexRule}

package object hyperspace {
  // The order of Hyperspace index rules does matter here, because by our current design, once an
  // index rule is applied to a base table, no further index rules can be applied to the same
  // table again.
  // For instance, let's say the Join rule gets applied first, then the original data source gets
  // replaced by its index. Now we have a new logical plan with the index folder as the "new"
  // data source. If the Filter rule gets applied on this, no change will happen because
  // this "new" data source doesn't have any indexes.
  // We therefore choose to put JoinIndexRule before FilterIndexRule to give join indexes
  // higher priority, because join indexes typically result in higher performance improvement
  // compared to filter indexes.
  private val hyperspaceOptimizationRuleBatch = JoinIndexRule :: FilterIndexRule :: Nil

  /**
   * Hyperspace-specific implicit class on SparkSession.
   */
  implicit class Implicits(sparkSession: SparkSession) {

    /**
     * Plug in Hyperspace-specific rules.
     *
     * @return a spark session that contains Hyperspace-specific rules.
     */
    def enableHyperspace(): SparkSession = {
      disableHyperspace
      sparkSession.sessionState.experimentalMethods.extraOptimizations ++=
        hyperspaceOptimizationRuleBatch
      sparkSession.sessionState.experimentalMethods.extraStrategies ++=
        BucketUnionStrategy :: Nil
      sparkSession
    }

    /**
     * Plug out Hyperspace-specific rules.
     *
     * @return a spark session that does not contain Hyperspace-specific rules.
     */
    def disableHyperspace(): SparkSession = {
      val experimentalMethods = sparkSession.sessionState.experimentalMethods
      experimentalMethods.extraOptimizations =
        experimentalMethods.extraOptimizations.filterNot(hyperspaceOptimizationRuleBatch.contains)
      sparkSession
    }

    /**
     * Checks if Hyperspace is enabled or not.
     *
     * @return true if Hyperspace is enabled or false otherwise.
     */
    def isHyperspaceEnabled(): Boolean = {
      hyperspaceOptimizationRuleBatch.forall(
        sparkSession.sessionState.experimentalMethods.extraOptimizations.contains)
    }
  }
}
