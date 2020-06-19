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

import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.Implicits

/**
 * Provides helper methods for python APIs.
 */
private[hyperspace] object PythonUtils {

  /**
   * Wrapper for Implicits.enableHyperspace to be called from the Python API.
   *
   * @param spark SparkSession
   * @return a spark session that contains Hyperspace-specific rules.
   */
  def enableHyperspace(spark: SparkSession): SparkSession = {
    spark.enableHyperspace()
  }

  /**
   * Wrapper for Implicits.disableHyperspace to be called from the Python API.
   *
   * @param spark SparkSession
   * @return a spark session that does not contain Hyperspace-specific rules.
   */
  def disableHyperspace(spark: SparkSession): SparkSession = {
    spark.disableHyperspace()
  }

  /**
   * Wrapper for Implicits.isHyperspaceEnabled to be called from the Python API.
   *
   * @param spark SparkSession
   * @return true if Hyperspace is enabled or false otherwise.
   */
  def isHyperspaceEnabled(spark: SparkSession): Boolean = {
    spark.isHyperspaceEnabled()
  }
}
