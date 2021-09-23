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

package com.microsoft.hyperspace.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.trees.TreeNode
import org.apache.spark.sql.execution.SimpleMode
import org.apache.spark.sql.execution.command.ExplainCommand

object SparkTestShims {
  object Implicits {
    implicit class TreeNodeExt(node: TreeNode[_]) {
      def simpleStringFull: String = node.simpleString(Int.MaxValue)
    }
  }

  object SimpleExplainCommand {
    def apply(logicalPlan: LogicalPlan): ExplainCommand = {
      ExplainCommand(logicalPlan, SimpleMode)
    }
  }

  def fromRow[T](encoder: ExpressionEncoder[T], row: InternalRow): T = {
    encoder.createDeserializer().apply(row)
  }
}
