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

import scala.collection.mutable

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, Expression}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexManager, LogicalPlanSignatureProvider}

object RuleUtils {

  /**
   * Get active indexes for the given logical plan by matching signatures.
   *
   * @param indexManager indexManager
   * @param plan logical plan
   * @return indexes built for this plan
   */
  def getCandidateIndexes(indexManager: IndexManager, plan: LogicalPlan): Seq[IndexLogEntry] = {
    // Map of a signature provider to a signature generated for the given plan.
    val signatureMap = mutable.Map[String, Option[String]]()

    def signatureValid(entry: IndexLogEntry): Boolean = {
      val sourcePlanSignatures = entry.source.plan.properties.fingerprint.properties.signatures
      assert(sourcePlanSignatures.length == 1)
      val sourcePlanSignature = sourcePlanSignatures.head
      signatureMap.getOrElseUpdate(
        sourcePlanSignature.provider,
        LogicalPlanSignatureProvider
          .create(sourcePlanSignature.provider)
          .signature(plan)) match {
        case Some(s) => s.equals(sourcePlanSignature.value)
        case None => false
      }
    }

    // TODO: the following check only considers indexes in ACTIVE state for usage. Update
    //  the code to support indexes in transitioning states as well.
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    allIndexes.filter(index => index.created && signatureValid(index))
  }

  /**
   * Extract the LogicalRelation node if the given logical plan is linear.
   *
   * @param logicalPlan given logical plan to extract LogicalRelation from.
   * @return if the plan is linear, the LogicalRelation node; Otherwise None.
   */
  def getLogicalRelation(logicalPlan: LogicalPlan): Option[LogicalRelation] = {
    val lrs = logicalPlan.collect { case r: LogicalRelation => r }
    if (lrs.length == 1) {
      Some(lrs.head)
    } else {
      None // logicalPlan is non-linear or it has no LogicalRelation.
    }
  }
}

object JoinRuleUtils {

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
  def isJoinConditionSupported(condition: Expression): Boolean = {
    condition match {
      case EqualTo(_: AttributeReference, _: AttributeReference) => true
      case And(left, right) => isJoinConditionSupported(left) && isJoinConditionSupported(right)
      case _ => false
    }
  }

  /**
   * Return an updated scan node (LogicalRelation) containing index information.
   * @param spark SparkSession.
   * @param relation Original source relation to update.
   * @param index Index Log Entry to use in place of original relation.
   * @return
   */
  def updateLogicalRelationWithIndex(
      spark: SparkSession,
      relation: LogicalRelation,
      index: IndexLogEntry): LogicalRelation = {
    val bucketSpec = BucketSpec(
      numBuckets = index.numBuckets,
      bucketColumnNames = index.config.indexedColumns,
      sortColumnNames = index.config.indexedColumns)

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
