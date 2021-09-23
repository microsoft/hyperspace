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

package com.microsoft.hyperspace.index.dataskipping.rules

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}

import com.microsoft.hyperspace.index.IndexLogEntryTags
import com.microsoft.hyperspace.index.dataskipping.DataSkippingIndex
import com.microsoft.hyperspace.index.plananalysis.FilterReasons
import com.microsoft.hyperspace.index.rules.{ExtractRelation, QueryPlanIndexFilter}
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.PlanToIndexesMap

/**
 * FilterConditionFilter filters indexes out if
 *   1) an index cannot be applied to the filter condition.
 */
object FilterConditionFilter extends QueryPlanIndexFilter {
  override def apply(plan: LogicalPlan, candidateIndexes: PlanToIndexesMap): PlanToIndexesMap = {
    if (candidateIndexes.isEmpty) {
      return Map.empty
    }
    plan match {
      case Filter(condition: Expression, ExtractRelation(relation)) =>
        val applicableIndexes = candidateIndexes(relation.plan).flatMap { indexLogEntry =>
          val indexDataPredOpt =
            indexLogEntry.withCachedTag(plan, IndexLogEntryTags.DATASKIPPING_INDEX_PREDICATE) {
              val index = indexLogEntry.derivedDataset.asInstanceOf[DataSkippingIndex]
              index.translateFilterCondition(spark, condition, relation.plan)
            }
          if (withFilterReasonTag(
              plan,
              indexLogEntry,
              FilterReasons.IneligibleFilterCondition(condition.sql))(
              indexDataPredOpt.nonEmpty)) {
            Some(indexLogEntry)
          } else {
            None
          }
        }
        if (applicableIndexes.nonEmpty) {
          Map(relation.plan -> applicableIndexes)
        } else {
          Map.empty
        }
      case _ => Map.empty
    }
  }
}
