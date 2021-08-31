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

import java.util.UUID

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, LogicalPlan, Project, Window}
import org.apache.spark.sql.types.{BooleanType, DataType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.IndexUtils
import com.microsoft.hyperspace.index.dataskipping.sketches.Sketch
import com.microsoft.hyperspace.index.rules.ApplyHyperspace.withHyperspaceRuleDisabled

object ExpressionUtils {

  /**
   * Returns copies of the given sketches with the indexed columns replaced by
   * resolved column names and data types.
   */
  def resolve(spark: SparkSession, sketches: Seq[Sketch], sourceData: DataFrame): Seq[Sketch] = {
    sketches.map { s =>
      val dataTypes = checkExprs(s.expressions, sourceData)
      val oldColumns = s.referencedColumns
      val newColumns = IndexUtils.resolveColumns(sourceData, oldColumns).map(_.name)
      val columnMapping = oldColumns.zip(newColumns).toMap
      val newExprs = s.expressions.map {
        case (expr, _) =>
          spark.sessionState.sqlParser
            .parseExpression(expr)
            .transformUp {
              case attr: UnresolvedAttribute => QuotedAttribute(columnMapping(attr.name))
            }
            .sql
      }
      s.withNewExpressions(newExprs.zip(dataTypes.map(Some(_))))
    }
  }

  private def checkExprs(
      exprWithExpectedDataTypes: Seq[(String, Option[DataType])],
      sourceData: DataFrame): Seq[DataType] = {
    val exprs = exprWithExpectedDataTypes.map(_._1)
    def throwNotSupportedIf(cond: Boolean, msg: => String): Unit = {
      if (cond) {
        throw HyperspaceException(s"DataSkippingIndex does not support indexing $msg")
      }
    }
    val plan = sourceData.selectExpr(exprs: _*).queryExecution.analyzed
    throwNotSupportedIf(
      plan.isInstanceOf[Aggregate],
      "aggregate functions: " + exprs.mkString(", "))
    throwNotSupportedIf(
      plan.find(_.isInstanceOf[Window]).nonEmpty,
      "window functions: " + exprs.mkString(", "))
    val analyzedExprs = plan.asInstanceOf[Project].projectList
    exprWithExpectedDataTypes.zip(analyzedExprs).map {
      case ((expr, expectedDataType), analyzedExpr) =>
        val e = analyzedExpr match {
          case Alias(child, _) => child
          case e => e
        }
        throwNotSupportedIf(!e.deterministic, s"an expression which is non-deterministic: $expr")
        throwNotSupportedIf(e.foldable, s"an expression which is evaluated to a constant: $expr")
        throwNotSupportedIf(
          e.find(_.isInstanceOf[SubqueryExpression]).nonEmpty,
          s"an expression which has a subquery: $expr")
        throwNotSupportedIf(
          e.find(_.isInstanceOf[Attribute]).isEmpty,
          s"an expression which does not reference source columns: $expr")
        if (expectedDataType.nonEmpty && expectedDataType.get != analyzedExpr.dataType) {
          throw HyperspaceException(
            "Specified and analyzed data types differ: " +
              s"expr=$expr, specified=${expectedDataType.get}, analyzed=${analyzedExpr.dataType}")
        }
        analyzedExpr.dataType
    }
  }

  /**
   * Used to workaround the issue where UnresolvedAttribute.sql() doesn't work as expected.
   */
  private case class QuotedAttribute(name: String) extends LeafExpression with Unevaluable {
    override def sql: String = name

    // $COVERAGE-OFF$ code never used
    override def nullable: Boolean = throw new NotImplementedError
    override def dataType: DataType = throw new NotImplementedError
    // $COVERAGE-ON$
  }

  /**
   * Returns a normalized expression so that the indexed expression and an
   * expression in the filter condition can be matched. For example,
   * expressions in the filter condition can have different ExprIds for every
   * execution, whereas the indexed expression is fixed.
   */
  def normalize(expr: Expression): Expression = {
    expr.transformUp {
      case a: Attribute => a.withExprId(nullExprId).withQualifier(Nil)
      case g @ GetStructField(child, ordinal, _) => g.copy(child, ordinal, None)
      // Undo HandleNullInputsForUDF and ReplaceNullWithFalseInPredicate so
      // that we can match scala UDF invocations. HandleNullInputsForUDF rule
      // transforms ScalaUDF(args...) into If(Or(IsNull(args)...), null,
      // ScalaUDF(KnownNotNull(args)...)), and ReplaceNullWithFalseInPredicate
      // rule transforms null into false. ReplaceNullWithFalseInPredicate is
      // sensitive to the tree shape.
      // This is a safe operation because we ignore null values when computing
      // sketch values. For example, MinMaxSketch("f(a)") will only collect
      // min/max values of non-null results of "f(a)". Then we can use those
      // sketch values to translate filter conditions like "f(a) = 1". Here,
      // we are only interested in whether those "f(a)" refers to the same
      // thing, not how they should be evaluated. Normalized expressions are
      // only meant to be compared, not evaluated.
      case If(
            ExtractIsNullDisjunction(args1),
            Literal(null | false, dataType1),
            udf @ ExtractScalaUDF(dataType2, ExtractKnownNotNullArgs(args2)))
          if args1 == args2 && dataType1 == dataType2 =>
        udf.copy(children = args2)
    }
  }

  // Exposed for test
  private[dataskipping] val nullExprId: ExprId = ExprId(0, new UUID(0, 0))

  private[dataskipping] object ExtractScalaUDF {
    def unapply(e: ScalaUDF): Option[(DataType, Seq[Expression])] = {
      Some((e.dataType, e.children))
    }
  }

  private[dataskipping] object ExtractIsNullDisjunction {
    def unapply(pred: Expression): Option[Seq[Expression]] =
      pred match {
        case IsNull(arg) => Some(Seq(arg))
        case Or(IsNull(arg), ExtractIsNullDisjunction(args)) => Some(arg +: args)
        case _ => None
      }
  }

  private[dataskipping] object ExtractKnownNotNullArgs {
    def unapply(args: Seq[Expression]): Option[Seq[Expression]] = {
      if (args.forall(_.isInstanceOf[KnownNotNull])) {
        Some(args.map(_.asInstanceOf[KnownNotNull].child))
      } else {
        None
      }
    }
  }

  /**
   * Returns sketch expressions that can be used to match indexed expressions
   * and expressions in the filter condition. For example, when a user creates
   * an index with MinMaxSketch("A"), we create an expression corresponding to
   * "A" here, and later we try to match expression nodes in a filter condition,
   * say, EqualTo(AttributeReference("A"), Literal(1)), to the expression for
   * "A".
   *
   * We need this step as the filter/join conditions are given to us as a tree
   * of expressions in the Spark's optimizer, whereas the indexed expressions
   * are provided and stored as strings.
   */
  def getResolvedExprs(
      spark: SparkSession,
      sketches: Seq[Sketch],
      source: LogicalPlan): Option[Map[Sketch, Seq[Expression]]] = {
    val resolvedExprs = sketches.map { s =>
      s -> s.expressions.map {
        case (expr, dataTypeOpt) =>
          val parsedExpr = spark.sessionState.sqlParser.parseExpression(expr)
          val dataType = dataTypeOpt.get
          val filter = dataType match {
            case BooleanType => Filter(parsedExpr, source)
            case _ => Filter(PredicateWrapper(parsedExpr), source)
          }
          val optimizedFilter = withHyperspaceRuleDisabled {
            spark.sessionState.optimizer
              .execute(spark.sessionState.analyzer.execute(filter))
              .asInstanceOf[Filter]
          }
          val resolvedExpr = dataType match {
            case BooleanType => optimizedFilter.condition
            case _ => optimizedFilter.condition.asInstanceOf[PredicateWrapper].child
          }
          if (resolvedExpr.dataType != dataType) {
            return None
          }
          normalize(resolvedExpr)
      }
    }.toMap
    Some(resolvedExprs)
  }

  // Used to preserve sketch expressions during optimization
  private case class PredicateWrapper(override val child: Expression)
      extends UnaryExpression
      with Unevaluable
      with Predicate
}
