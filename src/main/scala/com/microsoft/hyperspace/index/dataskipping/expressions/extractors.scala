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

package com.microsoft.hyperspace.index.dataskipping.expressions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.BooleanType

case class EqualToExtractor(left: ExpressionExtractor, right: ExpressionExtractor) {
  def unapply(p: Expression): Option[(Expression, Expression)] =
    p match {
      case EqualTo(left(l), right(r)) => Some((l, r))
      case EqualTo(right(r), left(l)) => Some((l, r))
      case _ => None
    }
}

case class EqualNullSafeExtractor(left: ExpressionExtractor, right: ExpressionExtractor) {
  def unapply(p: Expression): Option[(Expression, Expression)] =
    p match {
      case EqualNullSafe(left(l), right(r)) => Some((l, r))
      case EqualNullSafe(right(r), left(l)) => Some((l, r))
      case _ => None
    }
}

case class LessThanExtractor(left: ExpressionExtractor, right: ExpressionExtractor) {
  def unapply(p: Expression): Option[(Expression, Expression)] =
    p match {
      case LessThan(left(l), right(r)) => Some((l, r))
      case GreaterThan(right(r), left(l)) => Some((l, r))
      case _ => None
    }
}

case class LessThanOrEqualExtractor(left: ExpressionExtractor, right: ExpressionExtractor) {
  def unapply(p: Expression): Option[(Expression, Expression)] =
    p match {
      case LessThanOrEqual(left(l), right(r)) => Some((l, r))
      case GreaterThanOrEqual(right(r), left(l)) => Some((l, r))
      case _ => None
    }
}

case class IsNullExtractor(expr: ExpressionExtractor) {
  def unapply(p: Expression): Option[Expression] =
    p match {
      case IsNull(expr(e)) => Some(e)
      case EqualNullSafe(expr(e), v: Literal) if v.value == null => Some(e)
      case EqualNullSafe(v: Literal, expr(e)) if v.value == null => Some(e)
      case _ => None
    }
}

case class IsNotNullExtractor(expr: ExpressionExtractor) {
  def unapply(p: Expression): Option[Expression] =
    p match {
      case IsNotNull(expr(e)) => Some(e)
      // Spark 2.4 lacks a rule transforming Not(IsNull) to IsNotNull
      case Not(IsNull(expr(e))) => Some(e)
      case Not(EqualNullSafe(expr(e), v: Literal)) if v.value == null => Some(e)
      case Not(EqualNullSafe(v: Literal, expr(e))) if v.value == null => Some(e)
      case _ => None
    }
}

case class IsTrueExtractor(expr: ExpressionExtractor) {
  def unapply(p: Expression): Option[Expression] =
    p.dataType match {
      case BooleanType =>
        p match {
          case expr(e) => Some(e)
          case EqualTo(expr(e), Literal(true, BooleanType)) => Some(e)
          case EqualTo(Literal(true, BooleanType), expr(e)) => Some(e)
          case _ => None
        }
      case _ => None
    }
}

case class IsFalseExtractor(expr: ExpressionExtractor) {
  def unapply(p: Expression): Option[Expression] =
    p.dataType match {
      case BooleanType =>
        p match {
          case Not(expr(e)) => Some(e)
          case EqualTo(expr(e), Literal(false, BooleanType)) => Some(e)
          case EqualTo(Literal(false, BooleanType), expr(e)) => Some(e)
          case _ => None
        }
      case _ => None
    }
}

case class InExtractor(arg: ExpressionExtractor, element: ExpressionExtractor) {
  def unapply(p: Expression): Option[(Expression, Seq[Expression])] =
    p match {
      case In(arg(a), vs) =>
        Some((a, vs.map(element.unapply(_).getOrElse { return None })))
      case _ => None
    }
}

case class InSetExtractor(arg: ExpressionExtractor) {
  def unapply(p: Expression): Option[(Expression, Set[Any])] =
    p match {
      case InSet(arg(a), vs) => Some((a, vs))
      case _ => None
    }
}
