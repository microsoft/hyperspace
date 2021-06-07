/*
 * Copyright (2021) The Hyperspace Project Authors.
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
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan}

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException}
import com.microsoft.hyperspace.index.sources.FileBasedRelation

object RelationUtils {

  /**
   * Returns a [[FileBasedRelation]] from the relation in the logical plan.
   *
   * There should be only a single supported relation in the plan.
   */
  def getRelation(spark: SparkSession, plan: LogicalPlan): FileBasedRelation = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    val relations = plan.collect {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        provider.getRelation(l)
    }
    if (relations.length != 1) {
      throw HyperspaceException("Only a single relation is supported for indexing.")
    }
    relations.head
  }
}
