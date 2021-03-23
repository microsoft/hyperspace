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

package com.microsoft.hyperspace.goldstandard

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.LogicalPlanSignatureProvider

/**
 * MockSignatureProvider is used in TPCDS_Hyperspace plan stability suite. It returns the
 * table name of the source TPCDS table on which the index is created.
 */
class MockSignatureProvider extends LogicalPlanSignatureProvider {
  override def signature(logicalPlan: LogicalPlan): Option[String] = {
    val leafPlans = logicalPlan.collectLeaves()
    if (leafPlans.size != 1) return None

    leafPlans.head match {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        Some(location.rootPaths.head.getName)
      case _ => throw HyperspaceException("Unexpected logical plan found.")
    }
  }
}
