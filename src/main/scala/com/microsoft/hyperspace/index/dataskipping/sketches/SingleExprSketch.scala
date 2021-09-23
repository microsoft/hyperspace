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

import scala.reflect.ClassTag

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.DataType

/**
 * Base class for sketches which are based on and can be used for a single
 * expression.
 *
 * @param expr Expression this sketch is based on
 */
abstract class SingleExprSketch[T <: SingleExprSketch[T]](
    val expr: String,
    val dataType: Option[DataType])(implicit tag: ClassTag[T])
    extends Sketch {

  /**
   * Returns the name of this sketch.
   */
  def name: String

  /**
   * Parsed, unresolved expression of the expression string.
   *
   * Use this to build aggregate functions.
   */
  @transient
  protected final lazy val parsedExpr: Expression = {
    SparkSession.getActiveSession.get.sessionState.sqlParser.parseExpression(expr)
  }

  /**
   * Returns a copy of the sketch with an updated expression.
   */
  def withNewExpression(newExpr: (String, Option[DataType])): T

  final override def withNewExpressions(newExprs: Seq[(String, Option[DataType])]): T = {
    assert(newExprs.length == 1)
    withNewExpression(newExprs.head)
  }

  final override def expressions: Seq[(String, Option[DataType])] = (expr, dataType) :: Nil

  final override def indexedColumns: Seq[String] =
    parsedExpr.collect {
      case attr: UnresolvedAttribute => attr.name
    }

  final override def referencedColumns: Seq[String] = indexedColumns

  override def toString: String = s"$name($expr)"

  final override def equals(that: Any): Boolean =
    that match {
      case other: T => expr == other.expr && dataType == other.dataType
      case _ => false
    }

  final override def hashCode: Int = (tag.toString(), expr).hashCode

  // Workaround for issue https://github.com/FasterXML/jackson-module-scala/issues/218
  @JsonProperty("expr") private def _expr: String = expr
  @JsonProperty("dataType") private def _dataType: Option[DataType] = dataType
}
