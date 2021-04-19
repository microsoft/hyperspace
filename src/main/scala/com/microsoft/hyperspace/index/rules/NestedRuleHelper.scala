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

package com.microsoft.hyperspace.index.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BinaryOperator, ExprId, GetStructField, IsNotNull, NamedExpression}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project}
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.index.rules.PlanUtils._
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.util.ResolverUtils

class NestedRuleHelper(spark: SparkSession) extends BaseRuleHelper(spark) {

  /**
   * @inheritdoc
   */
  override protected[rules] def getUpdatedOutput(
      relation: FileBasedRelation,
      schema: StructType): Seq[AttributeReference] = {
    schema.flatMap { s =>
      relation.plan.output
        .find(a => ResolverUtils.ResolvedColumn(s.name).name.startsWith(a.name))
        .map { a =>
          AttributeReference(s.name, s.dataType, a.nullable, a.metadata)(
            NamedExpression.newExprId,
            a.qualifier)
        }
    }
  }

  /**
   * @inheritdoc
   */
  override protected[rules] def getFsRelationSchema(
      relation: FileBasedRelation,
      index: IndexLogEntry): StructType = {
    StructType(
      index.schema.filter(i => relation.plan.schema.exists(j => i.name.contains(j.name))))
  }

  /**
   * @inheritdoc
   */
  override protected[rules] def transformPlanToUseIndexOnlyScan(
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    plan transformUp {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        transformRelation(index, provider, l, useBucketSpec)

      // Given that the index may have top level field for a nested one
      // it is needed to transform the projection to use that index field
      case p: Project if hasNestedColumns(p, index) =>
        transformProject(p)

      // Given that the index may have top level field for a nested one
      // it is needed to transform the filter to use that index field
      case f: Filter if hasNestedColumns(f, index) =>
        transformFilter(f)
    }
  }

  /**
   * The method transforms the filter part of a plan to support indexes on
   * nested fields. The process is to go through all expression nodes and
   * do the following things:
   *  - Replace retrieval of nested values with index ones.
   *  - In some specific cases remove the `isnotnull` check because that
   *    is used on the root of the nested field (ie: `isnotnull(nested#102)`
   *    does not makes any sense when using the index field).
   *
   * For example, given the following query:
   * {{{
   *   df
   *     .filter("nested.leaf.cnt > 10 and nested.leaf.id == 'leaf_id9'")
   *     .select("Date", "nested.leaf.cnt")
   * }}}
   *
   * Having this simple filter:
   * {{{
   *   Filter (isnotnull(nested#102) && (nested#102.leaf.cnt > 10) &&
   *           (nested#102.leaf.id = leaf_id9))
   * }}}
   *
   * The filter part should become:
   * {{{
   *   Filter ((__hs_nested.nested.leaf.cnt#335 > 10) &&
   *           (__hs_nested.nested#.leaf.id#336 = leaf_id9))
   * }}}
   *
   * @param filter The filter that needs to be transformed.
   * @return The transformed filter with support for nested indexed fields.
   */
  protected[rules] def transformFilter(filter: Filter): Filter = {
    val names = extractNamesFromExpression(filter.condition)
    val transformedCondition = filter.condition.transformDown {
      case bo @ BinaryOperator(IsNotNull(AttributeReference(name, _, _, _)), other) =>
        if (names.toDiscard.contains(name)) {
          other
        } else {
          bo
        }
      case bo @ BinaryOperator(other, IsNotNull(AttributeReference(name, _, _, _))) =>
        if (names.toDiscard.contains(name)) {
          other
        } else {
          bo
        }
      case g: GetStructField =>
        val n = getChildNameFromStruct(g)
        if (names.toKeep.contains(n)) {
          val escapedFieldName = PlanUtils.prefixNestedField(n)
          getExprId(filter, escapedFieldName) match {
            case Some(exprId) =>
              val fieldType = extractTypeFromExpression(g, n)
              val attr = extractAttributeRef(g, n)
              attr.copy(escapedFieldName, fieldType, attr.nullable, attr.metadata)(
                exprId,
                attr.qualifier)
            case _ =>
              g
          }
        } else {
          g
        }
      case o =>
        o
    }
    filter.copy(condition = transformedCondition)
  }

  /**
   * The method transforms the project part of a plan to support indexes on
   * nested fields.
   *
   * For example, given the following query:
   * {{{
   *   df
   *     .filter("nested.leaf.cnt > 10 and nested.leaf.id == 'leaf_id9'")
   *     .select("Date", "nested.leaf.cnt")
   * }}}
   *
   * Having this simple projection:
   * {{{
   *   Project [Date#100, nested#102.leaf.cnt]
   * }}}
   *
   * The projection part should become:
   * {{{
   *   Project [Date#330, __hs_nested.nested.leaf.cnt#335]
   * }}}
   *
   * @param project The project that needs to be transformed.
   * @return The transformed project with support for nested indexed fields.
   */
  protected[rules] def transformProject(project: Project): Project = {
    val projectedFields = project.projectList.map { exp =>
      val fieldName = extractNamesFromExpression(exp).toKeep.head
      val escapedFieldName = PlanUtils.prefixNestedField(fieldName)
      val resolvedField = ResolverUtils.ResolvedColumn(escapedFieldName)
      val attr = extractAttributeRef(exp, fieldName)
      val fieldType = extractTypeFromExpression(exp, fieldName)
      // Try to find it in the project transformed child.
      getExprId(project.child, escapedFieldName) match {
        case Some(exprId) =>
          val newAttr = attr.copy(escapedFieldName, fieldType, attr.nullable, attr.metadata)(
            exprId,
            attr.qualifier)
          resolvedField.projectName match {
            case Some(projectName) =>
              Alias(newAttr, projectName)()
            case None =>
              newAttr
          }
        case _ =>
          attr
      }
    }
    project.copy(projectList = projectedFields)
  }

  /**
   * Returns true if the given project is a supported project. If all of the registered
   * providers return None, this returns false.
   *
   * @param project Project to check if it's supported.
   * @return True if the given project is a supported relation.
   */
  protected[rules] def hasNestedColumns(project: Project, index: IndexLogEntry): Boolean = {
    val indexCols =
      (index.indexedColumns ++ index.includedColumns).map(i => ResolverUtils.ResolvedColumn(i))
    val hasNestedCols = indexCols.exists(_.isNested)
    if (hasNestedCols) {
      val projectListFields = project.projectList.flatMap(extractNamesFromExpression(_).toKeep)
      val containsNestedFields =
        projectListFields.exists(i => indexCols.exists(j => j.isNested && j.name == i))
      var containsNestedChildren = false
      project.child.foreach {
        case f: Filter =>
          val filterSupported = hasNestedColumns(f, index)
          containsNestedChildren = containsNestedChildren || filterSupported
        case _ =>
      }
      containsNestedFields || containsNestedChildren
    } else {
      false
    }
  }

  /**
   * Returns true if the given filter has nested columns.
   *
   * @param filter Filter to check if it's supported.
   * @return True if the given project is a supported relation.
   */
  protected[rules] def hasNestedColumns(filter: Filter, index: IndexLogEntry): Boolean = {
    val indexCols =
      (index.indexedColumns ++ index.includedColumns).map(i => ResolverUtils.ResolvedColumn(i))
    val hasNestedCols = indexCols.exists(_.isNested)
    if (hasNestedCols) {
      val filterFields = extractNamesFromExpression(filter.condition).toKeep.toSeq
      val resolvedFilterFields = filterFields.map(ResolverUtils.ResolvedColumn(_))
      resolvedFilterFields.exists(i => indexCols.exists(j => j == i || j.name == i.name))
    } else {
      false
    }
  }

  /**
   * The method retrieves the expression id for a given field name.
   *
   * This method should be mainly used when transforming plans and the
   * leaves are already transformed.
   *
   * @param plan The logical plan from which to get the expression id.
   * @param fieldName The name of the field to search for.
   * @return An [[ExprId]] if that could be found in the plan otherwise [[None]].
   */
  private def getExprId(plan: LogicalPlan, fieldName: String): Option[ExprId] = {
    plan.output.find(a => a.name.equalsIgnoreCase(fieldName)).map(_.exprId)
  }
}
