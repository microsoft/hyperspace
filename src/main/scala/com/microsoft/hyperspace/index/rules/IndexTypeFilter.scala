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

package com.microsoft.hyperspace.index.rules

import scala.reflect.ClassTag

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.Index
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.PlanToIndexesMap

object IndexTypeFilter {

  /**
   * Returns a [[QueryPlanIndexFilter]] that filters out indexes which are not T.
   */
  def apply[T <: Index: ClassTag](): QueryPlanIndexFilter =
    new QueryPlanIndexFilter {
      override def apply(
          plan: LogicalPlan,
          candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
        candidateIndexes
          .map {
            case (plan, indexes) =>
              plan -> indexes.filter {
                _.derivedDataset match {
                  case _: T => true
                  case _ => false
                }
              }
          }
          .filter { case (_, indexes) => indexes.nonEmpty }
      }
    }
}
