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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, In, Literal, NamedExpression, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, RepartitionByExpression, Union}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{StringType, StructType}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{FileInfo, IndexConstants, IndexLogEntry, IndexManager, LogicalPlanSignatureProvider}
import com.microsoft.hyperspace.index.plans.logical.BucketUnion
import com.microsoft.hyperspace.util.HyperspaceConf

object RuleUtils {

  /**
   * Get active indexes for the given logical plan by matching signatures.
   *
   * @param spark Spark Session.
   * @param indexManager IndexManager.
   * @param plan Logical plan.
   * @return Indexes built for this plan.
   */
  def getCandidateIndexes(
      spark: SparkSession,
      indexManager: IndexManager,
      plan: LogicalPlan): Seq[IndexLogEntry] = {
    // Map of a signature provider to a signature generated for the given plan.
    val signatureMap = mutable.Map[String, Option[String]]()

    val hybridScanEnabled = HyperspaceConf.hybridScanEnabled(spark)
    val hybridScanDeleteEnabled = HyperspaceConf.hybridScanDeleteEnabled(spark)

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

    def isHybridScanCandidate(entry: IndexLogEntry, inputSourceFiles: Seq[FileInfo]): Boolean = {
      // TODO: Some threshold about the similarity of source data files - number of common files or
      //  total size of common files.
      //  See https://github.com/microsoft/hyperspace/issues/159
      // TODO: As in [[PlanSignatureProvider]], Source plan signature comparison is required to
      //  support arbitrary source plans at index creation.
      //  See https://github.com/microsoft/hyperspace/issues/158

      // Find the number of common files and deleted files between the source relations
      // & index source files.
      val commonCnt = inputSourceFiles.count(entry.allSourceFileInfos.contains)
      val deletedCnt = entry.allSourceFileInfos.size - commonCnt

      if (hybridScanDeleteEnabled && entry.hasLineageColumn(spark)) {
        commonCnt > 0
      } else {
        // For append-only dataset.
        deletedCnt == 0 && commonCnt > 0
      }
    }

    // TODO: the following check only considers indexes in ACTIVE state for usage. Update
    //  the code to support indexes in transitioning states as well.
    //  See https://github.com/microsoft/hyperspace/issues/65
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    if (hybridScanEnabled) {
      // TODO: Duplicate listing files for the given relation as in
      //  [[transformPlanToUseHybridScan]]
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
   * - We know for sure the index which can be used to transform the plan.
   * - The plan should be linear and include 1 LogicalRelation.
   *
   * @param spark Spark session.
   * @param index Index used in transformation of plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @return Transformed plan.
   */
  def transformPlanToUseIndex(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    val transformed = if (HyperspaceConf.hybridScanEnabled(spark)) {
      transformPlanToUseHybridScan(spark, index, plan, useBucketSpec)
    } else {
      transformPlanToUseIndexOnlyScan(spark, index, plan, useBucketSpec)
    }
    assert(!transformed.equals(plan))
    transformed
  }

  /**
   * Transform the current plan to utilize index.
   * The transformed plan reads data from indexes instead of the source relations.
   * Bucketing information of the index is retained if useBucketSpec is true.
   *
   * NOTE: This method currently only supports transformation of Scan Nodes i.e. Logical relations.
   *
   * @param spark Spark session.
   * @param index Index used in transformation of plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @return Transformed logical plan that leverages an index.
   */
  private def transformPlanToUseIndexOnlyScan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    // Note that we transform *only* the base relation and not other portions of the plan
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
   * @param index Index used in transformation of plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @return Transformed logical plan that leverages an index and merges appended data.
   */
  private def transformPlanToUseHybridScan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    val isParquetSourceFormat = index.relations.head.fileFormat.equals("parquet")
    var unhandledAppendedFiles: Seq[Path] = Nil

    // Get transformed plan with index data and appended files if applicable.
    val indexPlan = plan transformUp {
      // Use transformUp here since only 1 Logical Relation is pre-requisite condition.
      // Otherwise, we need additional handling of injected Filter-Not-In condition for
      // Hybrid Scan Delete support.
      case baseRelation @ LogicalRelation(
            _ @HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
            baseOutput,
            _,
            _) =>
        val curFileSet = location.allFiles
          .map(f => FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))

        val (filesDeleted, filesAppended) =
          if (HyperspaceConf.hybridScanDeleteEnabled(spark) && index.hasLineageColumn(spark)) {
            val (exist, nonExist) = curFileSet.partition(index.allSourceFileInfos.contains)
            val filesAppended = nonExist.map(f => new Path(f.name))
            if (exist.length < index.allSourceFileInfos.size) {
              (index.allSourceFileInfos -- exist, filesAppended)
            } else {
              (Nil, filesAppended)
            }
          } else {
            // Keep append-only implementation for efficiency.
            (
              Nil,
              curFileSet.filterNot(index.allSourceFileInfos.contains).map(f => new Path(f.name)))
          }

        val filesToRead = {
          if (useBucketSpec || !isParquetSourceFormat || filesDeleted.nonEmpty) {
            // Since the index data is in "parquet" format, we cannot read source files
            // in formats other than "parquet" using 1 FileScan node as the operator requires
            // files in one homogenous format. To address this, we need to read the appended
            // source files using another FileScan node injected into the plan and subsequently
            // merge the data into the index data. Please refer below [[Union]] operation.
            unhandledAppendedFiles = filesAppended
            index.content.files
          } else {
            // If BucketSpec of index data isn't used (e.g., in the case of FilterIndex currently)
            // and the source format is parquet, we could read the appended files along
            // with the index data.
            val files = index.content.files ++ filesAppended
            files
          }
        }

        val readSchema = if (filesDeleted.isEmpty) {
          StructType(index.schema.filter(baseRelation.schema.contains(_)))
        } else {
          StructType(index.schema)
        }

        val newLocation = new InMemoryFileIndex(spark, filesToRead, Map(), None)
        val relation = HadoopFsRelation(
          newLocation,
          new StructType(),
          readSchema,
          if (useBucketSpec) Some(index.bucketSpec) else None,
          new ParquetFileFormat,
          Map())(spark)

        val updatedOutput =
          baseOutput.filter(attr => relation.schema.fieldNames.contains(attr.name))

        if (filesDeleted.isEmpty) {
          baseRelation.copy(relation = relation, output = updatedOutput)
        } else {
          val lAttr = AttributeReference(IndexConstants.DATA_FILE_NAME_COLUMN, StringType)(
            NamedExpression.newExprId)
          val deletedFileNames = filesDeleted.map(f => Literal(f.name)).toArray
          val rel = baseRelation.copy(relation = relation, output = updatedOutput ++ Seq(lAttr))
          Project(updatedOutput, Filter(Not(In(lAttr, deletedFileNames)), rel))
        }
    }

    if (unhandledAppendedFiles.nonEmpty) {
      // If there are unhandled appended files, we need to create additional plans
      // by the following steps:
      // Step 1) Generate a plan (planForAppended) from the current plan to read
      //   the appended files similar to indexPlan.
      // Step 2) If Shuffle is required, perform shuffle for the plan.
      // Step 3) Merge both indexPlan and planForAppended by using [[BucketUnion]] or [[Union]].
      // For more details, see https://github.com/microsoft/hyperspace/issues/150.

      val planForAppended =
        transformPlanToReadAppendedFiles(spark, index.schema, plan, unhandledAppendedFiles)
      if (useBucketSpec) {
        // If Bucketing information of the index is used to read the index data, we need to
        // shuffle the appended data in the same way to correctly merge with bucketed index data.

        // Although only numBuckets of BucketSpec is used in BucketUnion*, bucketColumnNames
        // and sortColumnNames are shown in plan string. So remove sortColumnNames to avoid
        // misunderstanding.
        val bucketSpec = index.bucketSpec.copy(sortColumnNames = Nil)

        // Merge index plan & newly shuffled plan by using bucket-aware union.
        BucketUnion(
          Seq(indexPlan, transformPlanToShuffleUsingBucketSpec(bucketSpec, planForAppended)),
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
  private def transformPlanToReadAppendedFiles(
      spark: SparkSession,
      indexSchema: StructType,
      originalPlan: LogicalPlan,
      filesAppended: Seq[Path]): LogicalPlan = {
    // Transform the location of LogicalRelation with appended files.
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
   * Pre-requisite
   * - The plan should be linear and include 1 LogicalRelation.
   *
   * @param bucketSpec Bucket specification used for Shuffle.
   * @param plan Plan to be shuffled.
   * @return Transformed plan by injecting on-the-fly shuffle with given bucket specification.
   */
  private[rules] def transformPlanToShuffleUsingBucketSpec(
      bucketSpec: BucketSpec,
      plan: LogicalPlan): LogicalPlan = {
    // Extract top level plan including all required columns for shuffle in its output.
    object ExtractTopLevelPlanForShuffle {
      type returnType = (LogicalPlan, Seq[Option[Attribute]], Boolean)
      def unapply(plan: LogicalPlan): Option[returnType] = plan match {
        case p @ Project(_, Filter(_, LogicalRelation(_: HadoopFsRelation, _, _, _))) =>
          Some(p, getIndexedAttrs(p, bucketSpec.bucketColumnNames), true)
        case p @ Project(_, LogicalRelation(_: HadoopFsRelation, _, _, _)) =>
          Some(p, getIndexedAttrs(p, bucketSpec.bucketColumnNames), true)
        case f @ Filter(_, LogicalRelation(_: HadoopFsRelation, _, _, _)) =>
          Some(f, getIndexedAttrs(f, bucketSpec.bucketColumnNames), false)
        case r @ LogicalRelation(_: HadoopFsRelation, _, _, _) =>
          Some(r, getIndexedAttrs(r, bucketSpec.bucketColumnNames), false)
      }

      private def getIndexedAttrs(
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
