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

package com.microsoft.hyperspace.index.dataskipping.sketch

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.{Max, Min}
import org.apache.spark.sql.types.DataType

/**
 * Sketch based on minimum and maximum values for a given expression.
 *
 * @param expr Expression from which min/max values are calculated
 * @param dataType Optional data type to specify the expected data type of the
 *         expression. If not specified, it is deduced automatically.
 *         If the actual data type of the expression is different from this,
 *         an error is thrown. Users are recommended to leave this parameter to
 *         None.
 */
case class MinMaxSketch(override val expr: String, override val dataType: Option[DataType] = None)
    extends SingleExprSketch[MinMaxSketch](expr, dataType) {
  override def name: String = "MinMax"

  override def withNewExpression(newExpr: (String, Option[DataType])): MinMaxSketch = {
    copy(expr = newExpr._1, dataType = newExpr._2)
  }

  override def aggregateFunctions: Seq[Expression] =
    Min(parsedExpr).toAggregateExpression() :: Max(parsedExpr).toAggregateExpression() :: Nil
}
