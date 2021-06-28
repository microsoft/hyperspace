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
import com.microsoft.hyperspace.index.rules.ApplyHyperspace

package object hyperspace {

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
        ApplyHyperspace :: Nil
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
        experimentalMethods.extraOptimizations.filterNot(ApplyHyperspace.equals)
      experimentalMethods.extraStrategies =
        experimentalMethods.extraStrategies.filterNot(BucketUnionStrategy.equals)
      sparkSession
    }

    /**
     * Checks if Hyperspace is enabled or not.
     *
     * @return true if Hyperspace is enabled or false otherwise.
     */
    def isHyperspaceEnabled(): Boolean = {
      val experimentalMethods = sparkSession.sessionState.experimentalMethods
      experimentalMethods.extraOptimizations.contains(ApplyHyperspace) &&
      experimentalMethods.extraStrategies.contains(BucketUnionStrategy)
    }
  }
}
