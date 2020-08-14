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

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, AttributeReference, AttributeSet, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LeafNode, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.IndexLogEntry
import com.microsoft.hyperspace.index.rules.JoinIndexRule.spark

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
 *       "spark.sql.autoBroadcastJoinThreshold"
 * 2. Independently check left and right sides of the join for available indexes. If an index
 *    is picked, the shuffle on that side will be eliminated.
 */
object JoinIndexRuleV2 extends Rule[LogicalPlan] with Logging {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformUp {
    case join @ Join(l, r, _, Some(condition)) if eligible(l, r, condition) =>
      updatePlan(join)
  }

  private def contains(
      attributes: Set[AttributeReference],
      attribute: AttributeReference): Boolean = {
    attributes.exists(_.semanticEquals(attribute))
  }

  private def setIndexes(
      relation: LogicalRelation,
      joinCols: AttributeSet,
      requiredCols: Seq[Attribute]): LogicalPlan = {
    val indexManager = Hyperspace
      .getContext(spark)
      .indexCollectionManager

    val availableIndexes = RuleUtils.getCandidateIndexes(indexManager, relation)
    if (availableIndexes.isEmpty) {
      return relation
    }

    val tempVar = (joinCols.toSet ++ requiredCols.toSet)
      .map(_.asInstanceOf[AttributeReference])

    val allReqdCols =
      relation.outputSet
        .filter(c => contains(tempVar, c.asInstanceOf[AttributeReference]))
        .map(_.name)
    val reqdIdxCols = relation.outputSet
      .filter(
        c =>
          contains(
            joinCols.toSet.map((col: Attribute) => col.asInstanceOf[AttributeReference]),
            c.asInstanceOf[AttributeReference]))
      .map(_.name)

    val usableIndexes = availableIndexes
      .filter { index =>
        index.config.indexedColumns.toSet.equals(reqdIdxCols.toSet) &&
        allReqdCols.forall(
          c =>
            (index.config.indexedColumns ++ index.config.includedColumns)
              .contains(c))
      }

    usableIndexes.headOption match {
      case None => relation
      case Some(index) => replaceRelationWithIndex(relation, index)
    }
  }

  private def updatePlan(join: Join): Join = {
    val newLeft: LogicalPlan = updateIfSupported(join.left, join.condition.get)
    val newRight: LogicalPlan =
      updateIfSupported(join.right, join.condition.get)
    join.copy(left = newLeft, right = newRight)
  }

  private def updateIfSupported(plan: LogicalPlan, condition: Expression): LogicalPlan = {
    // Either left or right or both sides should contain half of references.
    // All references for any side, either left or right, should all come from the same leaf node.
    //
    // e.g. if join condition refers to columns A, B, C, D, two columns should come from left, 2
    // from right. This would be by default true. Nothing to do in this part. Let's say A,B
    // come from left. C,D come from right. The requirement is both A and B should come from the
    // same leaf node on left. Same for C and D. Both should come from same leaf node on right.

    val joinCols =
      condition.references.map(_.asInstanceOf[AttributeReference]).toSet
    val eligibleBaseRelations = plan.collectLeaves().filter {
      case relation: LogicalRelation =>
        relation.output.toSet.exists(col => contains(joinCols, col))
      case _ => false
    }
    if (eligibleBaseRelations.length != 1) {
      return plan
    }
    val eligibleRelation =
      eligibleBaseRelations.head.asInstanceOf[LogicalRelation]
    if (eligibleRelation.output.toSet.count(c => contains(joinCols, c)) * 2 != joinCols.size) {
      return plan
    }

    updateIndex(plan, condition.references, plan.output)
  }

  private def updateIndex(
      plan: LogicalPlan,
      joinCols: AttributeSet,
      requiredCols: Seq[Attribute]): LogicalPlan =
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

  private def eligible(l: LogicalPlan, r: LogicalPlan, condition: Expression): Boolean = {
    !isBroadcastJoin(l, r) && isJoinConditionSupported(condition)
  }

  private def isBroadcastJoin(l: LogicalPlan, r: LogicalPlan): Boolean = {
    val broadcastThreshold: Long =
      SparkSession.getActiveSession.get.conf
        .get("spark.sql.autoBroadcastJoinThreshold")
        .toLong
    l.stats.sizeInBytes <= broadcastThreshold || r.stats.sizeInBytes <= broadcastThreshold
  }

  /**
   * Check for supported Join Conditions. Equi-Joins in simple CNF form are supported.
   *
   * Predicates should be of the form (A = B and C = D and E = F and...). OR based conditions
   * are not supported. E.g. (A = B OR C = D) is not supported
   *
   * TODO (500053): Investigate whether OR condition can use bucketing info for optimization
   *
   * @param condition the join condition
   * @return true if the condition is supported. False otherwise.
   */
  private def isJoinConditionSupported(condition: Expression): Boolean = {
    condition match {
      case EqualTo(_: AttributeReference, _: AttributeReference) => true
      case And(left, right) => isJoinConditionSupported(left) && isJoinConditionSupported(right)
      case _ => false
    }
  }

  private def replaceRelationWithIndex(
    relation: LogicalRelation,
    index: IndexLogEntry): LogicalRelation = {
    val bucketSpec = BucketSpec(
      numBuckets = index.numBuckets,
      bucketColumnNames = index.config.indexedColumns,
      sortColumnNames = index.config.indexedColumns)

    val spark = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }

    val location = new InMemoryFileIndex(spark, Seq(new Path(index.content.root)), Map(), None)
    val newRelation = HadoopFsRelation(
      location,
      new StructType(),
      index.schema,
      Some(bucketSpec),
      new ParquetFileFormat,
      Map())(spark)

    val newOutput =
      relation.output.filter(attr => newRelation.schema.fieldNames.contains(attr.name))
    relation.copy(relation = newRelation, output = newOutput)
  }
}
