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

import org.apache.spark.SparkFunSuite

import com.microsoft.hyperspace.index.IndexConstants._
import com.microsoft.hyperspace.util.HyperspaceConf

class HyperspaceConfTest extends SparkFunSuite with SparkInvolvedSuite {
  test("Test configs that support legacy configs") {
    verifyLegacyAndNewConfig(
      () => HyperspaceConf.numBucketsForIndex(spark),
      INDEX_NUM_BUCKETS_LEGACY,
      10,
      INDEX_NUM_BUCKETS,
      5,
      INDEX_NUM_BUCKETS_DEFAULT)
  }

  // Verify the different behaviors when the given 'getConf' supports both legacy and new configs.
  private def verifyLegacyAndNewConfig[T](
      getConf: () => T,
      legacyConfKey: String,
      legacyConfValue: T,
      newConfKey: String,
      newConfValue: T,
      defaultConfValue: T): Unit = {
    def clear(): Unit = {
      spark.conf.unset(legacyConfKey)
      spark.conf.unset(newConfKey)
    }

    // Verify that the default is returned if no keys are set.
    clear()
    assert(getConf() === defaultConfValue)

    // Verify that the legacy config is returned if only legacy config is set.
    clear()
    spark.conf.set(legacyConfKey, legacyConfValue.toString)
    assert(getConf() === legacyConfValue)

    // Verify that the new config is returned if only new config is set.
    clear()
    spark.conf.set(newConfKey, newConfValue.toString)
    assert(getConf() === newConfValue)

    // Verify that new config is applied if both legacy and new configs are set.
    clear()
    spark.conf.set(legacyConfKey, legacyConfValue.toString)
    spark.conf.set(newConfKey, newConfValue.toString)
    assert(getConf() === newConfValue)
  }
}
