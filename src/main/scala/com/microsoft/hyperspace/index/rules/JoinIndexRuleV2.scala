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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.LogicalRelation

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.util.HyperspaceConf

/**
 * Join Index Rule V2. This rule tries to optimizes both sides of a shuffle based join
 * independently. The optimization works by replacing data sources with bucketed indexes which
 * match the join predicate partitioning.
 *
 * Algorithm:
 * 1. Identify whether this join node can be optimized:
 *    a. We support only equi-joins in CNF forms. Also make sure the join columns are directly
 *       picked from scan nodes.
 *    b. This join is not a broadcast hash join. To check this, we independently check the left
 *       and right sides of the join and make sure their size is less than
 *       "spark.sql.autoBroadcastJoinThreshold".
 * 2. Independently check left and right sides of the join for available indexes. If an index
 *    is picked, the shuffle on that side will be eliminated.
 */
object JoinIndexRuleV2 extends Rule[LogicalPlan] with Logging with ActiveSparkSession {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (HyperspaceConf.joinV2RuleEnabled(spark)) {
      plan transformUp {
        case join @ Join(l, r, _, Some(condition)) if eligible(l, r, condition) =>
          updatePlan(join)
      }
    } else {
      plan
    }
  }

  private def contains(
      attributes: Set[AttributeReference],
      attribute: AttributeReference): Boolean = {
    attributes.exists(_.semanticEquals(attribute))
  }

  /**
   * Return a logical plan with source data replaced by index data. The index should cover the
   * relevant join columns as indexed columns. The index should also cover only the relevant
   * columns from the list of allReferencedCols from this side of the plan.
   *
   * @param relation Source logical relation.
   * @param joinCols All columns used in join predicate. This is a superset of columns from the
   *                 relation as well as other join columns from other relations.
   * @param allReferencedCols All columns referenced in the logical plan on any given side of Join
   *                          node. This list of columns includes the output columns from the
   *                          plan as well as other referenced columns from intermediate nodes.
   *
   * @return Logical plan with eligible indexes if found, otherwise return original relation.
   */
  private def setIndexes(
      relation: LogicalRelation,
      joinCols: Set[AttributeReference],
      allReferencedCols: Seq[Attribute]): LogicalPlan = {
    val indexManager = Hyperspace
      .getContext(spark)
      .indexCollectionManager

    // TODO: the following check only considers indexes in ACTIVE state for usage. Update
    //  the code to support indexes in transitioning states as well.
    //  See https://github.com/microsoft/hyperspace/issues/65.
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    val allCols = (joinCols ++ allReferencedCols).map(_.asInstanceOf[AttributeReference])

    val allReqdCols =
      relation.outputSet
        .filter(c => contains(allCols, c.asInstanceOf[AttributeReference]))
        .map(_.name)
    val joinColAttrs = joinCols.map((col: Attribute) => col.asInstanceOf[AttributeReference])
    val reqdIdxCols = relation.outputSet
      .filter(c => contains(joinColAttrs, c.asInstanceOf[AttributeReference]))
      .map(_.name)
      .toSet

    val usableIndexes = allIndexes
      .filter { index =>
        val indexConfigCols = index.config.indexedColumns ++ index.config.includedColumns

        index.config.indexedColumns.toSet.equals(reqdIdxCols) &&
        allReqdCols.forall(indexConfigCols.contains(_))
      }

    usableIndexes match {
      case Nil => relation
      case _ =>
        RuleUtils.getCandidateIndexes(spark, usableIndexes, relation) match {
          case Nil => relation
          case index +: _ => RuleUtils.transformPlanToUseIndex(spark, index, relation, true)
        }
    }
  }

  private def updatePlan(join: Join): Join = {
    join.copy(
      left = updateIfSupported(join.left, join.condition.get),
      right = updateIfSupported(join.right, join.condition.get))
  }

  private def updateIfSupported(plan: LogicalPlan, condition: Expression): LogicalPlan = {
    // Either left or right or both sides should contain half of references.
    // All references for any side, either left or right, should all come from the same leaf node.
    //
    // E.g. if join condition refers to columns A, B, C, D, two columns should come from left, 2
    // from right. This would be by default true. Nothing to do in this part. Let's say A,B
    // come from left. C,D come from right. The requirement is both A and B should come from the
    // same leaf node on left. Same for C and D. Both should come from same leaf node on right.

    val joinCols = condition.references.map(_.asInstanceOf[AttributeReference]).toSet
    val eligibleBaseRelations = plan.collectLeaves().filter {
      case relation: LogicalRelation => relation.output.exists(contains(joinCols, _))
      case _ => false
    }

    eligibleBaseRelations match {
      case Seq(r: LogicalRelation)
          if r.output.toSet.count(c => contains(joinCols, c)) * 2 == joinCols.size =>
        updateIndex(plan, joinCols, plan.output)
      case _ => plan
    }
  }

  /**
   * Update the plan by replacing source data with indexes. To make sure the index covers all
   * required columns, traverse down the plan and collect all referenced columns from all nodes
   * starting from top of the plan to the bottom.
   *
   * Please note: we can't use plan.transform(...) here because we have to collect referenced
   * columns from every node we traverse till the end.
   * @param plan Original plan.
   * @param joinCols All referenced join columns from join condition.
   * @param requiredCols Superset of all columns which are referenced in the plan. If an index
   *                     is chosen to replace a base relation `r`, it should contain all the
   *                     columns from this `requiredCols`, which directly belong to `r`.
   * @return Logical plan with indexes if applicable indexes are found, else the original plan.
   */
  private def updateIndex(
      plan: LogicalPlan,
      joinCols: Set[AttributeReference],
      requiredCols: Seq[Attribute]): LogicalPlan = {
    plan match {
      case p: LogicalRelation => setIndexes(p, joinCols, requiredCols)
      case p: LeafNode => p
      case p: LogicalPlan =>
        p.withNewChildren {
          p.children.map { child =>
            updateIndex(child, joinCols, p.references.toSeq ++ requiredCols)
          }
        }
    }
  }

  private def eligible(l: LogicalPlan, r: LogicalPlan, condition: Expression): Boolean = {
    !isBroadcastJoin(l, r) && isJoinConditionSupported(condition)
  }

  private def isBroadcastJoin(l: LogicalPlan, r: LogicalPlan): Boolean = {
    // This logic is picked from `JoinSelection.canBroadcast(logicalPlan)`. Please refer:
    // https://github.com/apache/spark/blob/branch-2.4/sql/core/src/main/scala/org/apache/spark/
    // sql/execution/SparkStrategies.scala#L150-L155.
    val broadcastThreshold =
      SparkSession.getActiveSession.get.conf
        .get("spark.sql.autoBroadcastJoinThreshold")
        .toLong
    l.stats.sizeInBytes <= broadcastThreshold || r.stats.sizeInBytes <= broadcastThreshold
  }

  /**
   * Check for supported Join Conditions. Equi-Joins in simple CNF form are supported.
   *
   * Predicates should be of the form (A = B and C = D and E = F and...). OR based conditions
   * are not supported. E.g. (A = B OR C = D) is not supported.
   *
   * @param condition The join condition
   * @return True if the condition is supported. False otherwise.
   */
  private def isJoinConditionSupported(condition: Expression): Boolean = {
    condition match {
      case EqualTo(_: AttributeReference, _: AttributeReference) => true
      case And(left, right) => isJoinConditionSupported(left) && isJoinConditionSupported(right)
      case _ => false
    }
  }
}
