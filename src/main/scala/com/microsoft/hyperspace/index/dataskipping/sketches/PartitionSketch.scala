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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction
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
    override val expressions: Seq[(String, Option[DataType])])
    extends Sketch {

  override def indexedColumns: Seq[String] = exprStrings

  override def referencedColumns: Seq[String] = exprStrings

  override def withNewExpressions(
      newExpressions: Seq[(String, Option[DataType])]): PartitionSketch = {
    copy(expressions = newExpressions)
  }

  override def aggregateFunctions: Seq[AggregateFunction] = {
    val parser = SparkSession.getActiveSession.get.sessionState.sqlParser
    exprStrings.map { e =>
      FirstNullSafe(parser.parseExpression(e))
    }
  }

  override def toString: String = s"Partition(${exprStrings.mkString(", ")})"

  override def convertPredicate(
      predicate: Expression,
      resolvedExprs: Seq[Expression],
      sketchValues: Seq[Expression],
      nameMap: Map[ExprId, String],
      valueExtractor: ExpressionExtractor): Option[Expression] = {
    predicate match {
      case And(_, _) | Or(_, _) => None
      case valueExtractor(v) if (predicate.references.nonEmpty) => Some(v)
      case _ => None
    }
  }

  private def exprStrings: Seq[String] = expressions.map(_._1)
}
