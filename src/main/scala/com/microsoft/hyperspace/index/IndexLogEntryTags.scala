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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.util.HyperspaceConf

object IndexLogEntryTags {
  // HYBRIDSCAN_REQUIRED indicates if Hybrid Scan is required for the index or not.
  // This is set in getCandidateIndexes and utilized in transformPlanToUseIndex.
  val HYBRIDSCAN_REQUIRED: IndexLogEntryTag[Boolean] =
    IndexLogEntryTag[Boolean]("hybridScanRequired")

  // IS_SIGNATURE_MATCH indicates if the plan has the same signature value with the index or not.
  val IS_SIGNATURE_MATCH: IndexLogEntryTag[Boolean] =
    IndexLogEntryTag[Boolean]("isSignatureMatch")

  // IS_HYBRIDSCAN_CANDIDATE indicates if the index can be applied to the plan using Hybrid Scan.
  val IS_HYBRIDSCAN_CANDIDATE: IndexLogEntryTag[Boolean] =
    IndexLogEntryTag[Boolean]("hybridScanCandidate")

  // IS_HYBRIDSCAN_CANDIDATE indicates if the index can be applied to the plan using Hybrid Scan.
  val HYBRIDSCAN_CONFIG_CAPTURE: IndexLogEntryTag[Seq[String]] =
    IndexLogEntryTag[Seq[String]]("hybridScanConfigCapture")

  private def getHybridScanConfigs(spark: SparkSession): Seq[String] = {
    Seq(
      HyperspaceConf.hybridScanEnabled(spark).toString,
      HyperspaceConf.hybridScanDeleteEnabled(spark).toString,
      HyperspaceConf.hybridScanDeleteMaxNumFiles(spark).toString)
  }

  private[hyperspace] def resetHybridScanTagsIfNeeded(
      spark: SparkSession,
      plan: LogicalPlan,
      indexes: Seq[IndexLogEntry]): Unit = {
    val curConfigs = getHybridScanConfigs(spark)

    indexes.foreach { index =>
      val taggedConfigs = index.getTagValue(plan, HYBRIDSCAN_CONFIG_CAPTURE)
      if (taggedConfigs.isEmpty || !taggedConfigs.get.equals(curConfigs)) {
        // Need to reset cached tags as these config changes can change the result.
        index.unsetTagValue(plan, IS_HYBRIDSCAN_CANDIDATE)
        index.setTagValue(plan, HYBRIDSCAN_CONFIG_CAPTURE, curConfigs)
      }
    }
  }

}
