/*
 * Copyright (2020) The Hyperspace Project Authors.
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

import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, AttributeReference, Expression, GetStructField}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.util.SchemaUtils

object PlanUtils {

  /**
   * Returns true if the given project is a supported project. If all of the registered
   * providers return None, this returns false.
   *
   * @param project Project to check if it's supported.
   * @return True if the given project is a supported relation.
   */
  def isSupportedProject(project: Project): Boolean = {
    val containsNestedFields =
      SchemaUtils.hasNestedFields(project.projectList.flatMap(extractNamesFromExpression))
    var containsNestedChildren = false
    project.child.foreach {
      case f: Filter =>
        containsNestedChildren = containsNestedChildren || {
          SchemaUtils.hasNestedFields(
            SchemaUtils.unescapeFieldNames(extractNamesFromExpression(f.condition).toSeq))
        }
      case _ =>
    }
    containsNestedFields || containsNestedChildren
  }

  /**
   * Returns true if the given filter is a supported filter. If all of the registered
   * providers return None, this returns false.
   *
   * @param filter Filter to check if it's supported.
   * @return True if the given project is a supported relation.
   */
  def isSupportedFilter(filter: Filter): Boolean = {
    val containsNestedFields =
      SchemaUtils.hasNestedFields(extractNamesFromExpression(filter.condition).toSeq)
    containsNestedFields
  }

  /**
   * Given an expression it extracts all the field names from it.
   *
   * @param exp Expression to extract field names from
   * @return A set of distinct strings representing the field names
   *         (ie: `Set(nested.field.id, nested.field.other)`)
   */
  def extractNamesFromExpression(exp: Expression): Set[String] = {
    exp match {
      case AttributeReference(name, _, _, _) =>
        Set(s"$name")
      case otherExp =>
        otherExp.containsChild.flatMap {
          case g: GetStructField =>
            Set(s"${getChildNameFromStruct(g)}")
          case e: Expression =>
            extractNamesFromExpression(e).filter(_.nonEmpty)
          case _ => Set.empty[String]
        }
    }
  }

  /**
   * Given a nested field this method extracts the full name out of it.
   *
   * @param field The field from which to get the name from
   * @return The name of the field (ie: `nested.field.id`)
   */
  def getChildNameFromStruct(field: GetStructField): String = {
    field.child match {
      case f: GetStructField =>
        s"${getChildNameFromStruct(f)}.${field.name.get}"
      case a: AttributeReference =>
        s"${a.name}.${field.name.get}"
      case _ =>
        s"${field.name.get}"
    }
  }

  /**
   * Given an expression it extracts the attribute reference by field name.
   *
   * @param exp The expression where to look for the attribute reference
   * @param name The name of the field to look for
   * @return The attribute reference for that field name
   */
  def extractAttributeRef(exp: Expression, name: String): AttributeReference = {
    val splits = name.split(SchemaUtils.NESTED_FIELD_NEEDLE_REGEX)
    val elem = exp.find {
      case a: AttributeReference if splits.contains(a.name) => true
      case _ => false
    }
    elem.get.asInstanceOf[AttributeReference]
  }

  /**
   * Given and expression it extracts the type of the field by field name.
   *
   * @param exp The expression from where to extract the type from
   * @param name The name of the field to look for
   * @return The type of the field as [[DataType]]
   */
  def extractTypeFromExpression(exp: Expression, name: String): DataType = {
    val splits = name.split(SchemaUtils.NESTED_FIELD_NEEDLE_REGEX)
    val elem = exp.flatMap {
      case attrRef: AttributeReference =>
        if (splits.forall(s => attrRef.name == s)) {
          Some((name, attrRef.dataType))
        } else {
          Try({
            val h :: t = splits.toList
            if (attrRef.name == h && attrRef.dataType.isInstanceOf[StructType]) {
              val currentDataType = attrRef.dataType.asInstanceOf[StructType]
              var localDT = currentDataType
              val foldedFields = t.foldLeft(Seq.empty[(String, DataType)]) { (acc, i) =>
                val collected = localDT.collect {
                  case dt if dt.name == i =>
                    dt.dataType match {
                      case st: StructType =>
                        localDT = st
                      case _ =>
                    }
                    (i, dt.dataType)
                }
                acc ++ collected
              }
              Some(foldedFields.last)
            } else {
              None
            }
          }).getOrElse(None)
        }
      case f: GetStructField if splits.forall(s => f.toString().contains(s)) =>
        Some((name, f.dataType))
      case _ => None
    }
    elem.find(e => e._1 == name || e._1 == splits.last).get._2
  }

  /**
   * Given a logical plan the method collects all aliases in the plan.
   * For example, given this projection
   * `Project [nested#548.leaf.cnt AS cnt#659, Date#543, nested#548.leaf.id AS id#660]`
   * the result will be:
   * {{{
   * Seq(
   *   ("cnt", cnt#659, nested#548.leaf.cnt),
   *   ("id", id#660, nested#548.leaf.id)
   * )
   * }}}
   *
   * @param plan The plan from which to collect the aliases
   * @return A collection of:
   *         - a string representing the alias name
   *         - the attribute the alias transforms to
   *         - the expressions from which this alias comes from
   */
  def collectAliases(plan: LogicalPlan): Seq[(String, Attribute, Expression)] = {
    plan.collect {
      case Project(projectList, _) =>
        projectList.collect {
          case a @ Alias(child, name) =>
            (name, a.toAttribute, child)
        }
    }.flatten
  }
}
