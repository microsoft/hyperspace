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
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, RepartitionByExpression, Union}
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
   * @param indexManager IndexManager.
   * @param plan Logical plan.
   * @param hybridScanEnabled Flag that checks if hybrid scan is enabled or disabled.
   * @return Indexes built for this plan.
   */
  def getCandidateIndexes(
      indexManager: IndexManager,
      plan: LogicalPlan,
      hybridScanEnabled: Boolean): Seq[IndexLogEntry] = {
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

    def isHybridScanCandidate(entry: IndexLogEntry, filesByRelations: Seq[FileInfo]): Boolean = {
      // TODO: Some threshold about the similarity of source data files - number of common files or
      //  total size of common files.
      //  See https://github.com/microsoft/hyperspace/issues/159
      // TODO: As in [[PlanSignatureProvider]], Source plan signature comparison is required to
      //  support arbitrary source plans at index creation.
      //  See https://github.com/microsoft/hyperspace/issues/158

      // Find a common file between the input relation & index source files.
      // Without the threshold described above, we can utilize exists & contain functions here.
      val commonCnt = filesByRelations.count(entry.allSourceFileInfos.contains)
      val deletedCnt = entry.allSourceFileInfos.size - commonCnt

      // Currently, Hybrid Scan only support for append-only dataset.
      deletedCnt == 0 && commonCnt > 0
    }

    // TODO: the following check only considers indexes in ACTIVE state for usage. Update
    //  the code to support indexes in transitioning states as well.
    //  See https://github.com/microsoft/hyperspace/issues/65
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    if (hybridScanEnabled) {
      // TODO: Duplicate listing files for the given relation as in
      //  [[transformPlanToUseHybridIndexDataScan]]
      //  See https://github.com/microsoft/hyperspace/issues/160
      val filesByRelations = plan
        .collect {
          case LogicalRelation(
              HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
              _,
              _,
              _) =>
            location.allFiles.map(f =>
              FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))
        }
      assert(filesByRelations.length == 1)
      allIndexes.filter(index =>
        index.created && isHybridScanCandidate(index, filesByRelations.flatten))
    } else {
      allIndexes.filter(index => index.created && signatureValid(index))
    }
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
   * Transform the current plan to utilize the given index.
   *
   * The transformed plan reads the given index data rather than original source files.
   * If HybridScan is enabled, additional logical plans for the appended source files would be
   * constructed and merged with the transformed plan with the index.
   *
   * Pre-requisites
   * - We know for sure the index which can be used to replace the plan.
   * - The plan should be linear and include 1 LogicalRelation.
   *
   * @param spark Spark session.
   * @param index Index used in replacement plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @return Replacement plan.
   */
  def transformPlanToUseIndex(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    val transformed = if (HyperspaceConf.hybridScanEnabled(spark)) {
      transformPlanToUseHybridIndexDataScan(spark, index, plan, useBucketSpec)
    } else {
      transformPlanToUsePureIndexScan(spark, index, plan, useBucketSpec)
    }
    assert(!transformed.equals(plan))
    transformed
  }

  /**
   * Transform the current plan to utilize index
   * The transformed plan reads data from indexes instead of the source relations.
   * Bucketing information of the index is retained if useBucketSpec is true.
   *
   * NOTE: This method currently only supports replacement of Scan Nodes i.e. Logical relations
   *
   * @param spark Spark session.
   * @param index Index used in replacement plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @return Transformed logical plan that leverages an index.
   */
  private def transformPlanToUsePureIndexScan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    // Note that we replace *only* the base relation and not other portions of the plan
    // (e.g., filters). For instance, given the following input plan:
    //        Project(A,B) -> Filter(C = 10) -> Scan (A,B,C,D,E)
    // in the presence of a suitable index, the getIndexPlan() method will emit:
    //        Project(A,B) -> Filter(C = 10) -> Index Scan (A,B,C)
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
   * Transform the current plan to utilize the given index along with newly appended source files.
   *
   * With HybridScan, indexes with newly appended files to its source relation are also
   * eligible and we reconstruct new plans for the appended files so as to merge with
   * bucketed index data correctly.
   *
   * @param spark Spark session.
   * @param index Index used in replacement plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @return Transformed logical plan that leverages an index and merges appended data.
   */
  private def transformPlanToUseHybridIndexDataScan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    val isParquetSourceFormat = index.relations.head.fileFormat.equals("parquet")
    var filesAppended: Seq[Path] = Nil

    // Get replaced plan with index data and appended files if applicable.
    val indexPlan = plan transformDown {
      case baseRelation @ LogicalRelation(
            _ @HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
            baseOutput,
            _,
            _) =>
        val curFileSet = location.allFiles
          .map(f => FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))
        filesAppended =
          curFileSet.filterNot(index.allSourceFileInfos.contains).map(f => new Path(f.name))
        // TODO: Hybrid Scan delete support.

        // If BucketSpec of index data isn't used (e.g., in the case of FilterIndex currently),
        // or the source format is not parquet. We could read appended data from source files
        // along with the index data.
        val readPaths = {
          if (useBucketSpec || !isParquetSourceFormat) {
            // FilterIndexRule: since the index data is in "parquet" format, we cannot read
            // source files in formats other than "parquet" using 1 FileScan node as the
            // operator requires files in one homogenous format. To address this, we need
            // to read the appended source files using another FileScan node injected into
            // the plan and subsequently merge the data into the index data. Please refer below
            // [[Union]] operation.
            index.content.files
          } else {
            val files = index.content.files ++ filesAppended
            // Reset filesAppended as it's handled with index data.
            filesAppended = Nil
            files
          }
        }

        val newLocation = new InMemoryFileIndex(spark, readPaths, Map(), None)
        val relation = HadoopFsRelation(
          newLocation,
          new StructType(),
          StructType(index.schema.filter(baseRelation.schema.contains(_))),
          if (useBucketSpec) Some(index.bucketSpec) else None,
          new ParquetFileFormat,
          Map())(spark)

        val updatedOutput =
          baseOutput.filter(attr => relation.schema.fieldNames.contains(attr.name))
        baseRelation.copy(relation = relation, output = updatedOutput)
    }

    if (filesAppended.nonEmpty) {
      // If there are unhandled appended files, we need to create additional plans
      // by the following steps:
      // Step 1) Generate a plan(planForAppended) from the current plan to read
      //   the appended files similar to indexPlan.
      // Step 2) If Shuffle is required, perform shuffle for the plan.
      // Step 3) Merge both indexPlan and planForAppended by using [[BucketUnion]] or [[Union]].
      // For more details, see https://github.com/microsoft/hyperspace/issues/150.

      val planForAppended =
        transformPlanToReadFromAppendedFiles(spark, index.schema, plan, filesAppended)
      if (useBucketSpec) {
        // If Bucketing information of the index is used to read the index data, we need to
        // shuffle the appended data in the same way to correctly merge with bucketed index data.

        // Clear sortColumnNames as BucketUnion does not keep the sort order within a bucket.
        val bucketSpec = index.bucketSpec.copy(sortColumnNames = Seq())

        // Merge index plan & newly shuffled plan by using bucket-aware union.
        BucketUnion(
          Seq(indexPlan, transformPlanToShuffleUsingIndexSpec(bucketSpec, planForAppended)),
          bucketSpec)
      } else {
        // If bucketing is not necessary (e.g. FilterIndexRule), we use [[Union]] to merge
        // the appended data without additional shuffle.
        Union(indexPlan, planForAppended)
      }
    } else {
      indexPlan
    }
  }

  /**
   * Transform the current plan to read the given appended source files.
   *
   * The result will be merged with the plan which is reading index data
   * by using [[BucketUnion]] or [[Union]].
   *
   * @param spark Spark session.
   * @param indexSchema Index schema used for the output.
   * @param originalPlan Original plan.
   * @param filesAppended Appended files to the source relation.
   * @return Transformed linear logical plan for appended files.
   */
  private def transformPlanToReadFromAppendedFiles(
      spark: SparkSession,
      indexSchema: StructType,
      originalPlan: LogicalPlan,
      filesAppended: Seq[Path]): LogicalPlan = {
    // Replace the location of LogicalRelation with appended files.
    val planForAppended = originalPlan transformDown {
      case baseRelation @ LogicalRelation(fsRelation: HadoopFsRelation, baseOutput, _, _) =>
        // Set the same output schema with the index plan to merge them using BucketUnion.
        val updatedOutput =
          baseOutput.filter(attr => indexSchema.fieldNames.contains(attr.name))
        val newLocation =
          new InMemoryFileIndex(spark, filesAppended, Map(), None)
        val newRelation =
          fsRelation.copy(
            location = newLocation,
            dataSchema = StructType(indexSchema.filter(baseRelation.schema.contains(_))))(spark)
        baseRelation.copy(relation = newRelation, output = updatedOutput)
    }
    assert(!originalPlan.equals(planForAppended))
    planForAppended
  }

  /**
   * Transform the plan to perform on-the-fly Shuffle the data based on bucketSpec.
   *
   * Pre-requisites
   * - The plan should be linear and include 1 LogicalRelation.
   *
   * @param bucketSpec Bucket specification used for Shuffle.
   * @param plan Plan to be shuffled.
   * @return Transformed plan by injecting on-the-fly shuffle with given bucket specification.
   */
  private[rules] def transformPlanToShuffleUsingIndexSpec(
      bucketSpec: BucketSpec,
      plan: LogicalPlan): LogicalPlan = {
    // Extract top level plan including all required columns for shuffle in its output.
    // This is located inside this function because of bucketSpec.bucketColumnNames
    object ExtractTopLevelPlanForShuffle {
      type returnType = (LogicalPlan, Seq[Option[Attribute]], Boolean)
      def unapply(plan: LogicalPlan): Option[returnType] = plan match {
        case project @ Project(_, Filter(_, LogicalRelation(_: HadoopFsRelation, _, _, _))) =>
          val indexedAttrs = getIndexedAttrs(project, bucketSpec.bucketColumnNames)
          Some(project, indexedAttrs, true)
        case project @ Project(_, LogicalRelation(_: HadoopFsRelation, _, _, _)) =>
          val indexedAttrs = getIndexedAttrs(project, bucketSpec.bucketColumnNames)
          Some(project, indexedAttrs, true)
        case filter @ Filter(_, LogicalRelation(_: HadoopFsRelation, _, _, _)) =>
          val indexedAttrs = getIndexedAttrs(filter, bucketSpec.bucketColumnNames)
          Some(filter, indexedAttrs, false)
        case relation @ LogicalRelation(_: HadoopFsRelation, _, _, _) =>
          val indexedAttrs = getIndexedAttrs(relation, bucketSpec.bucketColumnNames)
          Some(relation, indexedAttrs, false)
      }
      def getIndexedAttrs(
          plan: LogicalPlan,
          indexedColumns: Seq[String]): Seq[Option[Attribute]] = {
        val attrMap = plan.output.attrs.map(attr => (attr.name, attr)).toMap
        indexedColumns.map(colName => attrMap.get(colName))
      }
    }

    // During merge of the index data with the newly appended data, perform
    // on-the-fly shuffle of the appended-data with the same partition structure
    // of index so that we could avoid incurring shuffle of the index data
    // (which is assumed to be the larger sized dataset).
    //
    // To do this optimally, we would have to push-down filters/projects before
    // shuffling the newly appended data to reduce the amount of data that gets
    // shuffled. However, if project excludes any of the [[indexedColumns]], Spark
    // handles the shuffle through [[RoundRobinPartitioning]], which can cause
    // wrong results.
    //
    // Therefore, we do the following:
    // if Project node excludes any of the [[indexedColumns]]:
    //    Case 1: Shuffle should be located **before** Project but **after** Filter
    //                Plan: Project => Shuffle => Filter => Relation
    // else:
    //    Case 2: Shuffle node should come **after** Project and/or Filter node
    //                Plan: Shuffle => Project => Filter => Relation
    //
    // Currently, we only perform on-the-fly shuffle when applying JoinIndexRule.
    // Therefore, it's always guaranteed that the children nodes have all indexed columns
    // in their output; Case 1 won't be shown in use cases. The implementation is kept
    // for future use cases.

    var shuffleInjected = false
    // Remove sort order because we cannot guarantee the ordering of source files.
    val shuffled = plan transformDown {
      case p if shuffleInjected => p
      case ExtractTopLevelPlanForShuffle(p, indexedAttr, isProject)
          if !isProject || indexedAttr.forall(_.isDefined) =>
        shuffleInjected = true
        RepartitionByExpression(indexedAttr.flatten, p, bucketSpec.numBuckets)
    }
    assert(shuffleInjected)
    shuffled
  }
}
