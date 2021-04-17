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

import scala.util.Try

import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BinaryExpression, Expression, GetStructField, IsNotNull, UnaryExpression}
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.util.ResolverUtils

object PlanUtils {

  /**
   * The method extract field names from a Spark Catalyst [[Expression]].
   *
   * @param exp The Spark Catalyst expression from which to extract names.
   * @return A set of distinct field names.
   */
  def extractNamesFromExpression(exp: Expression): ExtractedNames = {

    def extractNames(
        e: Expression,
        prevExpStrTypes: Seq[String] = Seq.empty): Set[(String, Seq[String])] = {
      e match {
        case g: GetStructField =>
          Set((s"${getChildNameFromStruct(g)}", prevExpStrTypes :+ "getStructField"))
        case AttributeReference(name, _, _, _) =>
          Set((s"$name", prevExpStrTypes :+ "attrRef"))
        case Alias(child, _) =>
          extractNames(child, prevExpStrTypes :+ "alias")
        case b: BinaryExpression =>
          val leftFields = extractNames(b.left, prevExpStrTypes :+ "binaryLeft")
          val rightFields = extractNames(b.right, prevExpStrTypes :+ "binaryRight")
          leftFields ++ rightFields
        case u: IsNotNull =>
          extractNames(u.child, prevExpStrTypes :+ "isNotNull")
        case u: UnaryExpression =>
          extractNames(u.child, prevExpStrTypes :+ "unary")
        case _ =>
          Set.empty[(String, Seq[String])]
      }
    }

    var toRemove = Seq.empty[String]
    val toKeep = extractNames(exp).toSeq
      .sortBy(-_._1.length)
      .foldLeft(Seq.empty[String]) { (acc, e) =>
        val (fieldName, expStrType) = e
        if (expStrType.contains("isNotNull") && acc.exists(i => i.startsWith(fieldName))) {
          toRemove :+= fieldName
          acc
        } else {
          acc :+ fieldName
        }
      }
    ExtractedNames(toKeep.toSet, toRemove.toSet)
  }

  /**
   * Given a [[GetStructField]] expression for a nested field (aka a struct)
   * the method will extract the full field `.` (dot) separated name.
   *
   * @param field The [[GetStructField]] field from which we want to extract
   *              the name.
   * @return A field name `.` (dot) separated if nested.
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
   * Given an Spark Catalyst [[Expression]] and a field name the method extracts
   * the parent search expression and the expression that contains the field name
   *
   * @param exp The Spark Catalyst [[Expression]] to extract from.
   * @param name The field name to search for.
   * @return A tuple with the parent expression and the leaf expression that
   *         contains the given name.
   */
  def extractSearchQuery(exp: Expression, name: String): (Expression, Expression) = {
    val splits = name.split("\\.")
    val expFound = exp.find {
      case a: AttributeReference if splits.forall(s => a.name.contains(s)) => true
      case f: GetStructField if splits.forall(s => f.toString().contains(s)) => true
      case _ => false
    }.get
    val parent = exp.find {
      case e: Expression if e.containsChild.contains(expFound) => true
      case _ => false
    }.get
    (parent, expFound)
  }

  /**
   * Given an Spark Catalyst [[Expression]], a needle [[Expression]] and a replace
   * [[Expression]] the method will replace the needle with the replacement into
   * the parent expression.
   *
   * @param parent The parent Spark Catalyst [[Expression]] into which to replace.
   * @param needle The Spark Catalyst [[Expression]] needle to search for.
   * @param repl The replacement Spark Catalyst [[Expression]].
   * @return A new Spark Catalyst [[Expression]].
   */
  def replaceInSearchQuery(
      parent: Expression,
      needle: Expression,
      repl: Expression): Expression = {
    parent.mapChildren { c =>
      if (c == needle) {
        repl
      } else {
        c
      }
    }
  }

  /**
   * Given an Spark Catalyst [[Expression]] and a field name the method
   * extracts the [[AttributeReference]] for that field name.
   *
   * @param exp The Spark Catalyst [[Expression]] to extract from.
   * @param name The field name for which to extract the attribute reference.
   * @return A Spark Catalyst [[AttributeReference]] pointing to the field name.
   */
  def extractAttributeRef(exp: Expression, name: String): AttributeReference = {
    val splits = name.split("\\.")
    val elem = exp.find {
      case a: AttributeReference if splits.contains(a.name) => true
      case _ => false
    }
    elem.get.asInstanceOf[AttributeReference]
  }

  /**
   * Given a Spark Catalyst [[Expression]] and a field name the method
   * extracts the type of the field as a Spark SQL [[DataType]].
   *
   * @param exp The Spark Catalyst [[Expression]] from which to extract the type.
   * @param name The field name for which we need to get the type.
   * @return A Spark SQL [[DataType]] of the given field name.
   */
  def extractTypeFromExpression(exp: Expression, name: String): DataType = {
    val splits = name.split("\\.")
    val elem = exp.flatMap {
      case a: AttributeReference =>
        if (splits.forall(s => a.name == s)) {
          Some((name, a.dataType))
        } else {
          Try({
            val h :: t = splits.toList
            if (a.name == h && a.dataType.isInstanceOf[StructType]) {
              val itFields = t.flatMap { i =>
                a.dataType
                  .asInstanceOf[StructType]
                  .find(_.name.equalsIgnoreCase(i))
                  .map(j => (i, j.dataType))
              }
              Some(itFields.last)
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

  def prefixNestedField(fieldName: String): String = {
    ResolverUtils.ResolvedColumn(fieldName, fieldName.contains(".")).normalizedName
  }

  private[hyperspace] case class ExtractedNames(toKeep: Set[String], toDiscard: Set[String])
}
