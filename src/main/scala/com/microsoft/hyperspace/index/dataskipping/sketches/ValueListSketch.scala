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
import org.apache.spark.sql.catalyst.expressions.aggregate.CollectSet
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.types.{ArrayType, DataType}

import com.microsoft.hyperspace.index.dataskipping.expressions._
import com.microsoft.hyperspace.index.dataskipping.util.ArrayUtils

/**
 * Sketch based on distinct values for a given expression.
 *
 * This is not really a sketch, as it stores all distinct values for a given
 * expression. It can be useful when the number of distinct values is expected to
 * be small and each file tends to store only a subset of the values.
 */
case class ValueListSketch(
    override val expr: String,
    override val dataType: Option[DataType] = None)
    extends SingleExprSketch[ValueListSketch](expr, dataType) {
  override def name: String = "ValueList"

  override def withNewExpression(newExpr: (String, Option[DataType])): ValueListSketch = {
    copy(expr = newExpr._1, dataType = newExpr._2)
  }

  override def aggregateFunctions: Seq[Expression] =
    new ArraySort(CollectSet(parsedExpr).toAggregateExpression()) :: Nil

  override def convertPredicate(
      predicate: Expression,
      resolvedExprs: Seq[Expression],
      sketchValues: Seq[Expression],
      nameMap: Map[ExprId, String],
      valueExtractor: ExpressionExtractor): Option[Expression] = {
    val valueList = sketchValues.head
    val min = ElementAt(valueList, Literal(1))
    val max = ElementAt(valueList, Literal(-1))
    // TODO: Consider shared sketches
    // HasNullSketch as described in MinMaxSketch.convertPredicate
    // can be useful for ValueListSketch too, as it can be used to
    // to optimize Not(EqualTo) as well as IsNull.
    val resolvedExpr = resolvedExprs.head
    val dataType = resolvedExpr.dataType
    val exprExtractor = NormalizedExprExtractor(resolvedExpr, nameMap)
    val ExprIsTrue = IsTrueExtractor(exprExtractor)
    val ExprIsFalse = IsFalseExtractor(exprExtractor)
    val ExprIsNotNull = IsNotNullExtractor(exprExtractor)
    val ExprEqualTo = EqualToExtractor(exprExtractor, valueExtractor)
    val ExprEqualNullSafe = EqualNullSafeExtractor(exprExtractor, valueExtractor)
    val ExprLessThan = LessThanExtractor(exprExtractor, valueExtractor)
    val ExprLessThanOrEqualTo = LessThanOrEqualExtractor(exprExtractor, valueExtractor)
    val ExprGreaterThan = GreaterThanExtractor(exprExtractor, valueExtractor)
    val ExprGreaterThanOrEqualTo = GreaterThanOrEqualExtractor(exprExtractor, valueExtractor)
    val ExprIn = InExtractor(exprExtractor, valueExtractor)
    val ExprInSet = InSetExtractor(exprExtractor)
    def Empty(arr: Expression) = EqualTo(Size(arr), Literal(0))
    Option(predicate).collect {
      case ExprIsTrue(_) => ArrayContains(valueList, Literal(true))
      case ExprIsFalse(_) => ArrayContains(valueList, Literal(false))
      case ExprIsNotNull(_) => Not(Empty(valueList))
      case ExprEqualTo(_, v) => SortedArrayContains(valueList, v)
      case ExprEqualNullSafe(_, v) => Or(IsNull(v), SortedArrayContains(valueList, v))
      case Not(ExprEqualTo(_, v)) =>
        And(
          IsNotNull(v),
          Or(
            GreaterThan(Size(valueList), Literal(1)),
            Not(EqualTo(ElementAt(valueList, Literal(1)), v))))
      case ExprLessThan(_, v) => LessThan(min, v)
      case ExprLessThanOrEqualTo(_, v) => LessThanOrEqual(min, v)
      case ExprGreaterThan(_, v) => GreaterThan(max, v)
      case ExprGreaterThanOrEqualTo(_, v) => GreaterThanOrEqual(max, v)
      case ExprIn(_, vs) =>
        vs.map(v => SortedArrayContains(valueList, v)).reduceLeft(Or)
      case ExprInSet(_, vs) =>
        SortedArrayContainsAny(
          valueList,
          ArrayUtils.toArray(
            vs.filter(_ != null).toArray.sorted(TypeUtils.getInterpretedOrdering(dataType)),
            dataType),
          dataType)
      // TODO: StartsWith, Like with constant prefix
    }
  }
}
