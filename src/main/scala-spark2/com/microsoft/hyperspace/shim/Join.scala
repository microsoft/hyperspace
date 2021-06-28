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

package com.microsoft.hyperspace.shim

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.JoinType
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan}

object JoinUtils {
  def withNewChildren(join: Join, left: LogicalPlan, right: LogicalPlan): Join = {
    val copy =
      try {
        val copyMethod = join.getClass.getMethod(
          "copy",
          classOf[LogicalPlan],
          classOf[LogicalPlan],
          classOf[JoinType],
          classOf[Option[Expression]])
        copyMethod.invoke(join, left, right, join.joinType, join.condition)
      } catch {
        case _: java.lang.NoSuchMethodException =>
          val hint = join.getClass.getDeclaredMethod("hint").invoke(join)
          val copyMethod = join.getClass.getMethod(
            "copy",
            classOf[LogicalPlan],
            classOf[LogicalPlan],
            classOf[JoinType],
            classOf[Option[Expression]],
            hint.getClass)
          copyMethod.invoke(join, left, right, join.joinType, join.condition, hint)
      }
    copy.asInstanceOf[Join]
  }
}
