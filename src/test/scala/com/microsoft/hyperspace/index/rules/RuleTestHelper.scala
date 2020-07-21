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

package com.microsoft.hyperspace.index.rules

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}

import com.microsoft.hyperspace.index.LogicalPlanSignatureProvider

object RuleTestHelper {
  class TestSignatureProvider extends LogicalPlanSignatureProvider {
    def signature(plan: LogicalPlan): Option[String] =
      plan
        .collectFirst {
          case LogicalRelation(HadoopFsRelation(location, _, _, _, _, _), _, _, _) =>
            location.hashCode()
        }
        .map(_.toString)
  }
}
