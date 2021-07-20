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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.index.plananalysis.FilterReasons
import com.microsoft.hyperspace.util.ResolverUtils

/**
 * Check if the given source plan contains all index columns.
 */
object ColumnSchemaFilter extends SourcePlanIndexFilter {
  override def apply(plan: LogicalPlan, indexes: Seq[IndexLogEntry]): Seq[IndexLogEntry] = {
    val relationColumnNames = plan.output.map(_.name)

    indexes.filter { index =>
      withFilterReasonTag(
        plan,
        index,
        FilterReasons.ColSchemaMismatch(
          relationColumnNames.mkString(","),
          index.derivedDataset.referencedColumns.mkString(","))) {
        ResolverUtils
          .resolve(spark, index.derivedDataset.referencedColumns, relationColumnNames)
          .isDefined
      }
    }
  }
}
