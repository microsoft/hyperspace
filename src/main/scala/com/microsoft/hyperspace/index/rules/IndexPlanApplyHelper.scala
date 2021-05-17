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

import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.index.rules.FilterIndexRule.spark
import com.microsoft.hyperspace.util.ResolverUtils

object IndexPlanApplyHelper {
  def apply(indexLogEntry: IndexLogEntry): BaseRuleHelper = {
    // Detect whether the index contains nested fields.
    val indexHasNestedColumns = (indexLogEntry.indexedColumns ++ indexLogEntry.includedColumns)
        .exists(ResolverUtils.ResolvedColumn(_).isNested)
    if (indexHasNestedColumns) {
      new NestedRuleHelper(spark)
    } else {
      new BaseRuleHelper(spark)
    }
  }
}
