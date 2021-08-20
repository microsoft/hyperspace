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

package com.microsoft.hyperspace.index.dataskipping.util

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{BooleanType, DataType}

// Value extractors returning Option[Literal] check if the value is not null
// because we're only interested in non-null values.
//
// Also note that we don't go overboard to match every pattern because
// we assume that Catalyst optimizer will give us an optimized predicate in NNF.
// It means in general we don't have to deal with Not, or worry about
// foldable expressions because they will be optimized to literals.
//
// There are some differences between Spark versions and that's why we include
// some patterns that are no longer needed in newer Spark versions.

/**
 * Extracts the non-null literal value in the predicate if it's equivalent to
 * <expr> = <literal>.
 *
 * For the purpose of data skipping, we don't extract the value if it's null.
 * If the literal is not null, then the only way to make the predicate
 * <expr> = <literal> or <expr> <=> <literal> is when the expression is not null
 * and its value is equal to the literal value.
 */
case class EqualToExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[Literal] =
    p match {
      case EqualTo(e, v: Literal) if v.value != null && matcher(e) => Some(v)
      case EqualTo(v: Literal, e) if v.value != null && matcher(e) => Some(v)
      case EqualNullSafe(e, v: Literal) if v.value != null && matcher(e) => Some(v)
      case EqualNullSafe(v: Literal, e) if v.value != null && matcher(e) => Some(v)
      case _ => None
    }
}

/**
 * Extracts the non-null literal value in the predicate if it's equivalent to
 * <expr> < <literal>.
 */
case class LessThanExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[Literal] =
    p match {
      case LessThan(e, v: Literal) if v.value != null && matcher(e) => Some(v)
      case GreaterThan(v: Literal, e) if v.value != null && matcher(e) => Some(v)
      case _ => None
    }
}

/**
 * Extracts the non-null literal value in the predicate if it's equivalent to
 * <expr> <= <literal>.
 */
case class LessThanOrEqualToExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[Literal] =
    p match {
      case LessThanOrEqual(e, v: Literal) if v.value != null && matcher(e) => Some(v)
      case GreaterThanOrEqual(v: Literal, e) if v.value != null && matcher(e) => Some(v)
      case _ => None
    }
}

/**
 * Extracts the non-null literal value in the predicate if it's equivalent to
 * <expr> > <literal>.
 */
case class GreaterThanExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[Literal] =
    p match {
      case GreaterThan(e, v: Literal) if v.value != null && matcher(e) => Some(v)
      case LessThan(v: Literal, e) if v.value != null && matcher(e) => Some(v)
      case _ => None
    }
}

/**
 * Extracts the non-null literal value in the predicate if it's equivalent to
 * <expr> >= <literal>.
 */
case class GreaterThanOrEqualToExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[Literal] =
    p match {
      case GreaterThanOrEqual(e, v: Literal) if v.value != null && matcher(e) => Some(v)
      case LessThanOrEqual(v: Literal, e) if v.value != null && matcher(e) => Some(v)
      case _ => None
    }
}

/**
 * Matches the predicate if it's equivalent to <expr> IS NULL.
 */
case class IsNullExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Boolean =
    p match {
      case IsNull(e) if matcher(e) => true
      case EqualNullSafe(e, v: Literal) if v.value == null && matcher(e) => true
      case EqualNullSafe(v: Literal, e) if v.value == null && matcher(e) => true
      case _ => false
    }
}

/**
 * Matches the predicate if it's equivalent to <expr> IS NOT NULL.
 *
 * Not(IsNull(<expr>)) is also matched because it can be in the predicate in
 * Spark 2.4. Since Spark 3.0, this is optimized to IsNotNull(<expr>).
 */
case class IsNotNullExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Boolean =
    p match {
      case IsNotNull(e) if matcher(e) => true
      case Not(IsNull(e)) if matcher(e) => true // for Spark 2.4
      case Not(EqualNullSafe(e, v: Literal)) if v.value == null && matcher(e) => true
      case Not(EqualNullSafe(v: Literal, e)) if v.value == null && matcher(e) => true
      case _ => false
    }
}

/**
 * Matches the predicate if it's equivalent to <expr> = true.
 *
 * Note that boolean expressions can be a predicate on their own, not needing
 * EqualTo with true. To avoid false matches, we check that the type of the
 * expression is BooleanType, although it's not strictly necessary because our
 * predicate conversion does not go down the predicate tree unless it's And/Or
 * and Spark has already checked the expression is Boolean if it's a direct
 * child of And/Or.
 */
case class IsTrueExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Boolean = {
    val EqualTo = EqualToExtractor(matcher)
    p.dataType == BooleanType && (p match {
      case EqualTo(Literal(true, BooleanType)) => true
      case e if matcher(e) => true
      case _ => false
    })
  }
}

/**
 * Matches the predicate if it's equivalent to <expr> = false.
 */
case class IsFalseExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Boolean = {
    val EqualTo = EqualToExtractor(matcher)
    p.dataType == BooleanType && (p match {
      case EqualTo(Literal(false, BooleanType)) => true
      case Not(e) if matcher(e) => true
      case _ => false
    })
  }
}

/**
 * Extracts non-null values in the predicate if it's equivalent to
 * <expr> IN (<lit>*).
 *
 * In Spark, In is created if the number of values in the list does not exceed
 * spark.sql.optimizer.inSetConversionThreshold.
 */
case class InExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[(Seq[Literal])] =
    p match {
      case In(e, vs) if vs.nonEmpty && vs.forall(v => v.isInstanceOf[Literal]) && matcher(e) =>
        Some(vs.map(_.asInstanceOf[Literal]).filter(_.value != null))
      case _ => None
    }
}

/**
 * Extracts non-null values in the predicate if it's equivalent to
 * <expr> IN (<lit>*).
 *
 * InSet is created instead of In if the list size is larger than
 * spark.sql.optimizer.inSetConversionThreshold.
 */
case class InSetExtractor(matcher: ExprMatcher) {
  def unapply(p: Expression): Option[(Set[Any])] =
    p match {
      case InSet(e, vs) if matcher(e) => Some(vs)
      case _ => None
    }
}
