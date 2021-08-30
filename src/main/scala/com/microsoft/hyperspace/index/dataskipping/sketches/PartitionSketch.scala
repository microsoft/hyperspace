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

package com.microsoft.hyperspace.index.dataskipping.sketches

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.DataType

import com.microsoft.hyperspace.index.dataskipping.expressions._
import com.microsoft.hyperspace.shim.FirstNullSafe

/**
 * Internal implementation for partitioning column.
 *
 * This sketch is automatically created for each partitioning column if the
 * source data has partitioning columns. This enables data skipping indexes to
 * be usable with filter conditions having a disjunction involving partitioning
 * columns and indexed columns. For example, a filter condition like "A = 1 or
 * B = 1" will be translated into an index predicate "(Min_A <= 1 and Max_A >=
 * 1) or B = 1" where A is an indexed column of MinMaxSketch and B is a
 * partitioning column.
 */
private[dataskipping] case class PartitionSketch(
    override val expr: String,
    override val dataType: Option[DataType] = None)
    extends SingleExprSketch[PartitionSketch](expr, dataType) {
  override def name: String = "Partition"

  override def withNewExpression(newExpr: (String, Option[DataType])): PartitionSketch = {
    copy(expr = newExpr._1, dataType = newExpr._2)
  }

  override def aggregateFunctions: Seq[Expression] =
    FirstNullSafe(parsedExpr).toAggregateExpression() :: Nil

  override def convertPredicate(
      predicate: Expression,
      resolvedExprs: Seq[Expression],
      nameMap: Map[ExprId, String],
      sketchValues: Seq[Expression]): Option[Expression] = {
    val value = sketchValues.head
    val exprMatcher = NormalizedExprMatcher(resolvedExprs.head, nameMap)
    predicate match {
      case _: And => None
      case _: Or => None
      case _ =>
        if (hasMatchingExpr(predicate, exprMatcher) &&
          !SubqueryExpression.hasSubquery(predicate) &&
          predicate.deterministic) {
          Some(predicate.transform { case a: AttributeReference if exprMatcher(a) => value })
        } else {
          None
        }
    }
  }

  private def hasMatchingExpr(expr: Expression, exprMatcher: ExprMatcher): Boolean = {
    expr.find(e => e.isInstanceOf[AttributeReference] && exprMatcher(e)).isDefined
  }
}
