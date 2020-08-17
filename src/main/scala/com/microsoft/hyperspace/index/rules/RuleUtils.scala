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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, RepartitionByExpression}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.BucketUnionLogicalPlan
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{IndexConstants, IndexLogEntry, IndexManager, LogicalPlanSignatureProvider}

object RuleUtils {

  /**
   * Get active indexes for the given logical plan by matching signatures.
   *
   * @param indexManager indexManager
   * @param plan logical plan
   * @param spark Spark session
   * @return indexes built for this plan
   */
  def getCandidateIndexes(
      indexManager: IndexManager,
      plan: LogicalPlan,
      spark: SparkSession): Seq[IndexLogEntry] = {
    // Map of a signature provider to a signature generated for the given plan.
    val signatureMap = mutable.Map[String, Option[String]]()

    def signatureValid(entry: IndexLogEntry): Boolean = {
      val sourcePlanSignatures = entry.source.plan.properties.fingerprint.properties.signatures
      assert(sourcePlanSignatures.length == 1)
      val sourcePlanSignature = sourcePlanSignatures.head
      val hybridScanEnabled = spark.sessionState.conf
        .getConfString(
          IndexConstants.INDEX_HYBRID_SCAN_ENABLED,
          IndexConstants.INDEX_HYBRID_SCAN_ENABLED_DEFAULT.toString)
        .toBoolean

      signatureMap.getOrElseUpdate(
        sourcePlanSignature.provider,
        LogicalPlanSignatureProvider
          .create(sourcePlanSignature.provider)
          .signature(plan, if (hybridScanEnabled) entry.allSourceFileSet else Set())) match {
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

  /**
   * Get replacement plan for current plan. The replacement plan reads data from indexes
   *
   * Pre-requisites
   * - We know for sure the index which can be used to replace the plan.
   *
   * NOTE: This method currently only supports replacement of Scan Nodes i.e. Logical relations
   *
   * @param spark Spark session
   * @param index index used in replacement plan
   * @param plan current plan
   * @return replacement plan
   */
  def getReplacementPlan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan): LogicalPlan = {
    val location = new InMemoryFileIndex(spark, Seq(new Path(index.content.root)), Map(), None)
    val relation = HadoopFsRelation(
      location,
      new StructType(),
      index.schema,
      Some(index.bucketSpec),
      new ParquetFileFormat,
      Map())(spark)

    // Here we can't replace the plan completely with the index. This will create problems.
    // For e.g. if Project(A,B) -> Filter(C = 10) -> Scan (A,B,C,D,E)
    // if we replace this plan with index Scan (A,B,C), we lose the Filter(C=10) and it will
    // lead to wrong results. So we only replace the base relation.

    val replacedPlan = plan transformUp {
      case baseRelation @ LogicalRelation(_: HadoopFsRelation, baseOutput, _, _) =>
        val updatedOutput =
          baseOutput.filter(attr => relation.schema.fieldNames.contains(attr.name))
        baseRelation.copy(relation = relation, output = updatedOutput)
    }

    val hybridScanEnabled = spark.sessionState.conf
      .getConfString(
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED,
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED_DEFAULT.toString)
      .toBoolean

    if (hybridScanEnabled) {
      getComplementIndexPlan(spark, index, plan, replacedPlan)
    } else {
      replacedPlan
    }
  }

  private def getComplementIndexPlan(
      spark: SparkSession,
      index: IndexLogEntry,
      originalPlan: LogicalPlan,
      indexPlan: LogicalPlan): LogicalPlan = {
    val complementIndexPlan = originalPlan transformUp {
      case baseRelation @ LogicalRelation(fsRelation: HadoopFsRelation, baseOutput, _, _) =>
        val updatedOutput =
          baseOutput.filter(attr => index.schema.fieldNames.contains(attr.name))

        val filesNotCovered =
          fsRelation.location.inputFiles
            .map(new Path(_))
            .filter(!index.allSourceFileSet.contains(_))

        if (filesNotCovered.nonEmpty) {
          val newLocation =
            new InMemoryFileIndex(spark, filesNotCovered, Map(), None)
          val newRelation =
            fsRelation.copy(location = newLocation, dataSchema = index.schema)(spark)
          baseRelation.copy(relation = newRelation, output = updatedOutput)
        } else {
          baseRelation
        }
    }

    if (!originalPlan.equals(complementIndexPlan)) {
      val attrs = complementIndexPlan.output.attrs.filter { attr =>
        index.indexedColumns contains attr.name
      }
      // Perform on-the-fly shuffle with the same partition structure of index
      // so that we could avoid incurring shuffle whole index data at merge stage
      val shuffled = RepartitionByExpression(attrs, complementIndexPlan, index.numBuckets)

      // removing sort order because original data isn't sorted by the same columns as the index
      val bucketSpec = index.bucketSpec.copy(sortColumnNames = Seq())

      // Merge index data & newly shuffled data by using bucket-aware union.
      // Currently, BucketUnion does not keep the sort order within a bucket.
      BucketUnionLogicalPlan(Seq(indexPlan, shuffled), bucketSpec)
    } else {
      indexPlan
    }
  }
}
