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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, RepartitionByExpression}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{FileInfo, IndexLogEntry, IndexManager, LogicalPlanSignatureProvider}
import com.microsoft.hyperspace.index.plans.logical.BucketUnion
import com.microsoft.hyperspace.util.HyperspaceConf

object RuleUtils {

  /**
   * Get active indexes for the given logical plan by matching signatures.
   *
   * @param indexManager Index Manager.
   * @param plan Logical plan.
   * @param hybridScanEnabled HybridScan config.
   * @return Indexes built for this plan.
   */
  def getCandidateIndexes(
      indexManager: IndexManager,
      plan: LogicalPlan,
      hybridScanEnabled: Boolean): Seq[IndexLogEntry] = {
    // Map of a signature provider to a signature generated for the given plan.
    val signatureMap = mutable.Map[(String, Option[Set[Path]]), Option[String]]()

    def signatureValid(entry: IndexLogEntry): Boolean = {
      val sourcePlanSignatures = entry.source.plan.properties.fingerprint.properties.signatures
      assert(sourcePlanSignatures.length == 1)
      val sourceFileSet = if (hybridScanEnabled) {
        assert(entry.relations.length == 1)
        Some(entry.allSourceFiles)
      } else None
      val sourcePlanSignature = sourcePlanSignatures.head
      signatureMap.getOrElseUpdate(
        (sourcePlanSignature.provider, sourceFileSet),
        LogicalPlanSignatureProvider
          .create(sourcePlanSignature.provider)
          .signature(plan, sourceFileSet)) match {
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
   * Get replacement plan for current plan. The replacement plan reads data from indexes.
   * If HybridScan is enabled, additional logical plans for the appended data would be
   * generated and merged with index data plan. Refer [[getComplementIndexPlan]].
   *
   * Pre-requisites
   * - We know for sure the index which can be used to replace the plan.
   *
   * NOTE: This method currently only supports replacement of Scan Nodes i.e. Logical relations
   *
   * @param spark Spark session.
   * @param index Index used in replacement plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @return Replacement plan.
   */
  def getReplacementPlan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    if (HyperspaceConf.hybridScanEnabled(spark)) {
      getHybridScanIndexPlan(spark, index, plan, useBucketSpec)
    } else {
      getIndexPlan(spark, index, plan, useBucketSpec)
    }
  }

  /**
   * Get alternative logical plan of the current plan using the given index.
   *
   * @param spark Spark session.
   * @param index Index used in replacement plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @return Alternative logical plan.
   */
  private def getIndexPlan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    // Here we can't replace the plan completely with the index. This will create problems.
    // For e.g. if Project(A,B) -> Filter(C = 10) -> Scan (A,B,C,D,E)
    // if we replace this plan with index Scan (A,B,C), we lose the Filter(C=10) and it will
    // lead to wrong results. So we only replace the base relation.
    plan transformDown {
      case baseRelation @ LogicalRelation(_: HadoopFsRelation, baseOutput, _, _) =>
        val location =
          new InMemoryFileIndex(spark, index.content.files, Map(), None)
        val relation = HadoopFsRelation(
          location,
          new StructType(),
          StructType(index.schema.filter(baseRelation.schema.contains(_))),
          if (useBucketSpec) Some(index.bucketSpec) else None,
          new ParquetFileFormat,
          Map())(spark)

        val updatedOutput =
          baseOutput.filter(attr => relation.schema.fieldNames.contains(attr.name))
        baseRelation.copy(relation = relation, output = updatedOutput)
    }
  }

  /**
   * Get alternative logical plan of the current plan using the given index.
   * With HybridScan, indexes with newly appended files to its source relation are also
   * eligible and we reconstruct new plans for the appended files so as to merge into
   * the index data.
   *
   * @param spark Spark session.
   * @param index Index used in replacement plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @return Alternative logical plan.
   */
  private def getHybridScanIndexPlan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    // Other than "parquet" format, we cannot read source files along with index data
    // files in parquet format with 1 FileScan node. In this case, we need to read the
    // appended source files by another FileScan node and merge into the index data.
    // Though BucketUnion (using BucketSpec and on-the-fly Shuffle) is used to merge
    // them for now, we will try to optimize these plans with Union operator.
    val useBucketUnion = useBucketSpec || !index.relations.head.fileFormat.equals("parquet")
    val replacedPlan = plan transformDown {
      case baseRelation @ LogicalRelation(
            _ @HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
            baseOutput,
            _,
            _) =>
        val curFileSet = location.allFiles
          .map(f => FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))
          .toSet
        // if BucketSpec of index data isn't used, we could read appended data from
        // source files directly.
        val readPaths = {
          if (useBucketUnion) {
            index.content.files
          } else {
            val filesAppended =
              (curFileSet -- index.allSourceFileInfos).map(f => new Path(f.name)).toSeq
            index.content.files ++ filesAppended
          }
        }

        val newLocation = new InMemoryFileIndex(spark, readPaths, Map(), None)
        val relation = HadoopFsRelation(
          newLocation,
          new StructType(),
          StructType(index.schema.filter(baseRelation.schema.contains(_))),
          if (useBucketUnion) Some(index.bucketSpec) else None,
          new ParquetFileFormat,
          Map())(spark)

        val updatedOutput =
          baseOutput.filter(attr => relation.schema.fieldNames.contains(attr.name))
        baseRelation.copy(relation = relation, output = updatedOutput)
    }
    if (useBucketUnion) {
      // if BucketSpec of the index is used to read the index data, we need to shuffle
      // the appended data in the same way to avoid additional shuffle of index data.
      getComplementIndexPlan(spark, index, plan, replacedPlan)
    } else {
      replacedPlan
    }
  }

  /**
   * Get complement plan for an index with appended data.
   *
   * This method consists of the following steps
   * 1) Get a plan from originalPlan by replacing data location with appended data
   * 2) On-the-fly shuffle for the appended data, using indexedColumns & numBuckets.
   *   - Shuffle is located before Project to utilize Push-down Filters
   *     - Shuffle => Project => Filter => Relation
   *   - if Project filters indexedColumns, then Shuffle should be located after the node
   *     - Project => Shuffle => Filter => Relation
   * 3) Bucket-aware union both indexPlan and complementPlan to avoid repartitioning index data
   *
   * NOTE: This method currently only supports replacement of Scan Nodes i.e. Logical relations
   *
   * @param spark Spark session
   * @param index index used in replacement plan
   * @param originalPlan original plan
   * @param indexPlan replaced plan with index
   * @return complementIndexPlan integrated plan of indexPlan and complementPlan
   */
  private def getComplementIndexPlan(
      spark: SparkSession,
      index: IndexLogEntry,
      originalPlan: LogicalPlan,
      indexPlan: LogicalPlan): LogicalPlan = {
    // 1) Replace the location of LogicalRelation with appended files
    val complementIndexPlan = originalPlan transformDown {
      case baseRelation @ LogicalRelation(
            fsRelation @ HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
            baseOutput,
            _,
            _) =>
        // Set the same output schema with the index plan to merge them using BucketUnion
        val updatedOutput =
          baseOutput.filter(attr => index.schema.fieldNames.contains(attr.name))

        val filesAppended = (location.allFiles
          .map(f => FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))
          .toSet -- index.allSourceFileInfos).toSeq.map(f => new Path(f.name))

        if (filesAppended.nonEmpty) {
          val newLocation =
            new InMemoryFileIndex(spark, filesAppended, Map(), None)
          val newRelation =
            fsRelation.copy(
              location = newLocation,
              dataSchema = StructType(index.schema.filter(baseRelation.schema.contains(_))))(
              spark)
          baseRelation.copy(relation = newRelation, output = updatedOutput)
        } else {
          baseRelation
        }
    }

    if (!originalPlan.equals(complementIndexPlan)) {
      // Remove sort order because we cannot guarantee the ordering of source files
      val bucketSpec = index.bucketSpec.copy(sortColumnNames = Seq())

      object ExtractTopLevelPlanForShuffle {
        type returnType = (LogicalPlan, Seq[Option[Attribute]], Boolean)
        def unapply(plan: LogicalPlan): Option[returnType] = plan match {
          case project @ Project(
                _,
                Filter(_, LogicalRelation(HadoopFsRelation(_, _, _, _, _, _), _, _, _))) =>
            val indexedAttrs = getIndexedAttrs(project, index.indexedColumns)
            Some(project, indexedAttrs, true)
          case project @ Project(
                _,
                LogicalRelation(HadoopFsRelation(_, _, _, _, _, _), _, _, _)) =>
            val indexedAttrs = getIndexedAttrs(project, index.indexedColumns)
            Some(project, indexedAttrs, true)
          case filter @ Filter(_, LogicalRelation(HadoopFsRelation(_, _, _, _, _, _), _, _, _)) =>
            val indexedAttrs = getIndexedAttrs(filter, index.indexedColumns)
            Some(filter, indexedAttrs, false)
          case relation @ LogicalRelation(HadoopFsRelation(_, _, _, _, _, _), _, _, _) =>
            val indexedAttrs = getIndexedAttrs(relation, index.indexedColumns)
            Some(relation, indexedAttrs, false)
        }
        def getIndexedAttrs(
            plan: LogicalPlan,
            indexedColumns: Seq[String]): Seq[Option[Attribute]] = {
          val attrMap = plan.output.attrs.map(attr => (attr.name, attr)).toMap
          indexedColumns.map(colName => attrMap.get(colName))
        }
      }

      // 2) Perform on-the-fly Shuffle with the same partition structure of index
      // so that we could avoid incurring Shuffle of whole index data at merge stage.
      // In order to utilize push-down filters, we would locate Shuffle node after
      // Project or Filter node. (Case 1)
      // However, if Project node excludes any of indexedColumns, Shuffle will be
      // converted to RoundRobinPartitioning which can cause wrong result issues.
      // So Shuffle should be located before Project node in that case. (Case 2)
      // Case 1) Shuffle => Project => Filter => Relation (Project&Filter will be pushed down)
      // Case 2) Project => Shuffle => Filter => Relation (Filter will be pushed down)
      var shuffleInjected = false
      val shuffled = complementIndexPlan transformDown {
        case p if shuffleInjected => p
        case ExtractTopLevelPlanForShuffle(plan, indexedAttr, isProject)
            if !isProject || indexedAttr.forall(_.isDefined) =>
          shuffleInjected = true
          RepartitionByExpression(indexedAttr.flatten, plan, index.numBuckets)
      }
      // 3) Merge index plan & newly shuffled plan by using bucket-aware union.
      // Currently, BucketUnion does not keep the sort order within a bucket.
      BucketUnion(Seq(indexPlan, shuffled), bucketSpec)
    } else {
      indexPlan
    }
  }
}
