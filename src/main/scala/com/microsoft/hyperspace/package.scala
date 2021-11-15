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

import com.microsoft.hyperspace.HyperspaceSparkSessionExtension
import com.microsoft.hyperspace.index.execution.BucketUnionStrategy
import com.microsoft.hyperspace.index.rules.ApplyHyperspace
import com.microsoft.hyperspace.util.HyperspaceConf

package object hyperspace {

  /**
   * Hyperspace-specific implicit class on SparkSession.
   */
  implicit class Implicits(sparkSession: SparkSession) {

    /**
     * Enable Hyperspace indexes.
     *
     * Plug in Hyperspace-specific rules and set `IndexConstants.HYPERSPACE_APPLY_ENABLED` as true.
     *
     * @return a spark session that contains Hyperspace-specific rules.
     */
    def enableHyperspace(): SparkSession = {
      HyperspaceConf.setHyperspaceApplyEnabled(sparkSession, true)
      addOptimizationsIfNeeded()
      sparkSession
    }

    /**
     * Disable Hyperspace indexes.
     *
     * Set `IndexConstants.HYPERSPACE_APPLY_ENABLED` as false
     * to stop applying Hyperspace indexes.
     *
     * @return a spark session that `IndexConstants.HYPERSPACE_APPLY_ENABLED` is set as false.
     */
    def disableHyperspace(): SparkSession = {
      HyperspaceConf.setHyperspaceApplyEnabled(sparkSession, false)
      sparkSession
    }

    /**
     * Checks if Hyperspace is enabled or not.
     *
     * Note that Hyperspace is enabled when:
     * 1) `ApplyHyperspace` exists in extraOptimization
     * 2) `BucketUnionStrate` exists in extraStrategies and
     * 3) `IndexConstants.HYPERSPACE_APPLY_ENABLED` is true.
     *
     * @return true if Hyperspace is enabled or false otherwise.
     */
    def isHyperspaceEnabled(): Boolean = {
      val experimentalMethods = sparkSession.sessionState.experimentalMethods
      experimentalMethods.extraOptimizations.contains(ApplyHyperspace) &&
      experimentalMethods.extraStrategies.contains(BucketUnionStrategy) &&
      HyperspaceConf.hyperspaceApplyEnabled(sparkSession)
    }

    /**
     * Add ApplyHyperspace and BucketUnionStrategy into extraOptimization
     * and extraStrategies, respectively, to make Spark can use Hyperspace.
     *
     * @param sparkSession Spark session that will use Hyperspace
     */
    private[hyperspace] def addOptimizationsIfNeeded(): Unit = {
      if (!sparkSession.sessionState.experimentalMethods.extraOptimizations.contains(
          ApplyHyperspace)) {
        sparkSession.sessionState.experimentalMethods.extraOptimizations ++=
          ApplyHyperspace :: Nil
      }
      if (!sparkSession.sessionState.experimentalMethods.extraStrategies.contains(
          BucketUnionStrategy)) {
        sparkSession.sessionState.experimentalMethods.extraStrategies ++=
          BucketUnionStrategy :: Nil
      }
    }
  }
}
