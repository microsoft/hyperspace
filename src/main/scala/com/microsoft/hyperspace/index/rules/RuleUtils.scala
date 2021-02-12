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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, In, Literal, Not}
import org.apache.spark.sql.catalyst.optimizer.OptimizeIn
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{LongType, StructType}

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.IndexLogEntryTags.{HYBRIDSCAN_RELATED_CONFIGS, IS_HYBRIDSCAN_CANDIDATE}
import com.microsoft.hyperspace.index.plans.logical.{BucketUnion, IndexHadoopFsRelation}
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.util.HyperspaceConf

object RuleUtils {

  /**
   * Filter the given candidate indexes by matching signatures and index status.
   * If Hybrid Scan is enabled, it compares the file metadata directly, and does not
   * match signatures. By doing that, we could perform file-level comparison between
   * index source files and the input files of the given plan. If there are some common
   * files, the index is considered as a candidate.
   *
   * @param spark Spark Session.
   * @param indexes List of available indexes.
   * @param relation Relation with which given indexes are compared.
   * @return Active indexes built for this plan.
   */
  def getCandidateIndexes(
      spark: SparkSession,
      indexes: Seq[IndexLogEntry],
      relation: FileBasedRelation): Seq[IndexLogEntry] = {
    // Map of a signature provider to a signature generated for the given plan.
    val signatureMap = mutable.Map[String, Option[String]]()

    val hybridScanEnabled = HyperspaceConf.hybridScanEnabled(spark)
    val hybridScanDeleteEnabled = HyperspaceConf.hybridScanDeleteEnabled(spark)

    def signatureValid(entry: IndexLogEntry): Boolean = {
      entry.withCachedTag(relation.plan, IndexLogEntryTags.SIGNATURE_MATCHED) {
        val sourcePlanSignatures = entry.source.plan.properties.fingerprint.properties.signatures
        assert(sourcePlanSignatures.length == 1)
        val sourcePlanSignature = sourcePlanSignatures.head

        signatureMap.getOrElseUpdate(
          sourcePlanSignature.provider,
          LogicalPlanSignatureProvider
            .create(sourcePlanSignature.provider)
            .signature(relation.plan)) match {
          case Some(s) => s.equals(sourcePlanSignature.value)
          case None => false
        }
      }
    }

    def isHybridScanCandidate(
        entry: IndexLogEntry,
        inputSourceFiles: Seq[FileInfo],
        inputSourceFilesSizeInBytes: Long): Boolean = {
      // TODO: Some threshold about the similarity of source data files - number of common files or
      //  total size of common files.
      //  See https://github.com/microsoft/hyperspace/issues/159
      // TODO: As in [[PlanSignatureProvider]], Source plan signature comparison is required to
      //  support arbitrary source plans at index creation.
      //  See https://github.com/microsoft/hyperspace/issues/158

      entry.withCachedTag(relation.plan, IndexLogEntryTags.IS_HYBRIDSCAN_CANDIDATE) {
        // Find the number of common files between the source relation and index source files.
        // The total size of common files are collected and tagged for candidate.
        val (commonCnt, commonBytes) = inputSourceFiles.foldLeft(0L, 0L) { (res, f) =>
          if (entry.sourceFileInfoSet.contains(f)) {
            (res._1 + 1, res._2 + f.size) // count, total bytes
          } else {
            res
          }
        }

        val appendedBytesRatio = 1 - commonBytes / inputSourceFilesSizeInBytes.toFloat
        val deletedBytesRatio = 1 - commonBytes / entry.sourceFilesSizeInBytes.toFloat

        val deletedCnt = entry.sourceFileInfoSet.size - commonCnt
        val isAppendAndDeleteCandidate = hybridScanDeleteEnabled && entry.hasLineageColumn &&
          commonCnt > 0 &&
          appendedBytesRatio < HyperspaceConf.hybridScanAppendedRatioThreshold(spark) &&
          deletedBytesRatio < HyperspaceConf.hybridScanDeletedRatioThreshold(spark)

        // For append-only Hybrid Scan, deleted files are not allowed.
        lazy val isAppendOnlyCandidate = deletedCnt == 0 && commonCnt > 0 &&
          appendedBytesRatio < HyperspaceConf.hybridScanAppendedRatioThreshold(spark)

        val isCandidate = isAppendAndDeleteCandidate || isAppendOnlyCandidate
        if (isCandidate) {
          entry.setTagValue(
            relation.plan,
            IndexLogEntryTags.COMMON_SOURCE_SIZE_IN_BYTES,
            commonBytes)

          // If there is no change in source dataset, the index will be applied by
          // transformPlanToUseIndexOnlyScan.
          entry.setTagValue(
            relation.plan,
            IndexLogEntryTags.HYBRIDSCAN_REQUIRED,
            !(commonCnt == entry.sourceFileInfoSet.size && commonCnt == inputSourceFiles.size))
        }
        isCandidate
      }
    }

    def prepareHybridScanCandidateSelection(
        spark: SparkSession,
        plan: LogicalPlan,
        indexes: Seq[IndexLogEntry]): Unit = {
      assert(HyperspaceConf.hybridScanEnabled(spark))
      val curConfigs = Seq(
        HyperspaceConf.hybridScanAppendedRatioThreshold(spark).toString,
        HyperspaceConf.hybridScanDeletedRatioThreshold(spark).toString)

      indexes.foreach { index =>
        val taggedConfigs = index.getTagValue(plan, HYBRIDSCAN_RELATED_CONFIGS)
        if (taggedConfigs.isEmpty || !taggedConfigs.get.equals(curConfigs)) {
          // Need to reset cached tags as these config changes can change the result.
          index.unsetTagValue(plan, IS_HYBRIDSCAN_CANDIDATE)
          index.setTagValue(plan, HYBRIDSCAN_RELATED_CONFIGS, curConfigs)
        }
      }
    }

    if (hybridScanEnabled) {
      // TODO: Duplicate listing files for the given relation as in
      //  [[transformPlanToUseHybridScan]]
      //  See https://github.com/microsoft/hyperspace/issues/160
      val inputSourceFiles = relation.allFiles.map { f =>
        // For a given file, file id is only meaningful in the context of a given
        // index. At this point, we do not know which index, if any, would be picked.
        // Therefore, we simply set the file id to UNKNOWN_FILE_ID.
        FileInfo(
          f.getPath.toString,
          f.getLen,
          f.getModificationTime,
          IndexConstants.UNKNOWN_FILE_ID)
      }
      prepareHybridScanCandidateSelection(spark, relation.plan, indexes)
      val totalSizeInBytes = inputSourceFiles.map(_.size).sum
      indexes.filter(index =>
        index.created && isHybridScanCandidate(index, inputSourceFiles, totalSizeInBytes))
    } else {
      indexes.filter(index => index.created && signatureValid(index))
    }
  }

  /**
   * Check if an index was applied the given relation or not.
   * This can be determined by an identifier in [[FileBasedRelation]]'s options.
   *
   * @param relation FileBasedRelation to check if an index is already applied.
   * @return true if an index is applied to the given relation. Otherwise false.
   */
  def isIndexApplied(relation: FileBasedRelation): Boolean = {
    relation.options.exists(_.equals(IndexConstants.INDEX_RELATION_IDENTIFIER))
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
   * - The plan should be linear and include one supported relation.
   *
   * @param spark Spark session.
   * @param index Index used in transformation of plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @param useBucketUnionForAppended Option whether to use BucketUnion to merge appended data.
   * @return Transformed plan.
   */
  def transformPlanToUseIndex(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean,
      useBucketUnionForAppended: Boolean): LogicalPlan = {
    // Check pre-requisite.
    val relation = getRelation(spark, plan)
    assert(relation.isDefined)

    // If there is no change in source data files, the index can be applied by
    // transformPlanToUseIndexOnlyScan regardless of Hybrid Scan config.
    // This tag should always exist if Hybrid Scan is enabled.
    val hybridScanRequired = HyperspaceConf.hybridScanEnabled(spark) &&
      index.getTagValue(relation.get.plan, IndexLogEntryTags.HYBRIDSCAN_REQUIRED).get

    // If the index has appended files and/or deleted files, which means the current index data
    // is outdated, Hybrid Scan should be used to handle the newly updated source files.
    // Added `lazy` to avoid constructing sets for appended/deleted files unless necessary.
    lazy val isSourceUpdated = index.hasSourceUpdate

    val transformed = if (hybridScanRequired || isSourceUpdated) {
      transformPlanToUseHybridScan(spark, index, plan, useBucketSpec, useBucketUnionForAppended)
    } else {
      transformPlanToUseIndexOnlyScan(spark, index, plan, useBucketSpec)
    }
    assert(!transformed.equals(plan))
    transformed
  }

  /**
   * Extract the relation node if the given logical plan is linear.
   *
   * @param plan Logical plan to extract a relation node from.
   * @return If the plan is linear and the relation node is supported, the [[FileBasedRelation]]
   *         object that wraps the relation node. Otherwise None.
   */
  def getRelation(spark: SparkSession, plan: LogicalPlan): Option[FileBasedRelation] = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    val leaves = plan.collectLeaves()
    if (leaves.size == 1 && provider.isSupportedRelation(leaves.head)) {
      Some(provider.getRelation(leaves.head))
    } else {
      None
    }
  }

  /**
   * Transform the current plan to utilize index.
   * The transformed plan reads data from indexes instead of the source relations.
   * Bucketing information of the index is retained if useBucketSpec is true.
   *
   * NOTE: This method currently only supports transformation of nodes with supported relations.
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
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    // Note that we transform *only* the base relation and not other portions of the plan
    // (e.g., filters). For instance, given the following input plan:
    //        Project(A,B) -> Filter(C = 10) -> Scan (A,B,C,D,E)
    // in the presence of a suitable index, the getIndexPlan() method will emit:
    //        Project(A,B) -> Filter(C = 10) -> Index Scan (A,B,C)
    plan transformDown {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        val relation = provider.getRelation(l)
        val location = index.withCachedTag(IndexLogEntryTags.INMEMORYFILEINDEX_INDEX_ONLY) {
          new InMemoryFileIndex(spark, index.content.files, Map(), None)
        }

        val indexFsRelation = new IndexHadoopFsRelation(
          location,
          new StructType(),
          StructType(index.schema.filter(relation.plan.schema.contains(_))),
          if (useBucketSpec) Some(index.bucketSpec) else None,
          new ParquetFileFormat,
          Map(IndexConstants.INDEX_RELATION_IDENTIFIER))(spark, index)

        val updatedOutput = relation.plan.output
          .filter(attr => indexFsRelation.schema.fieldNames.contains(attr.name))
          .map(_.asInstanceOf[AttributeReference])
        relation.createLogicalRelation(indexFsRelation, updatedOutput)
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
   * @param useBucketUnionForAppended Option whether to use BucketUnion to merge appended data.
   * @return Transformed logical plan that leverages an index and merges appended data.
   */
  private def transformPlanToUseHybridScan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean,
      useBucketUnionForAppended: Boolean): LogicalPlan = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    var unhandledAppendedFiles: Seq[Path] = Nil
    // Get transformed plan with index data and appended files if applicable.
    val indexPlan = plan transformUp {
      // Use transformUp here as currently one relation is allowed (pre-requisite).
      // The transformed plan will have LogicalRelation as a child; for example, LogicalRelation
      // can be transformed to 'Project -> Filter -> LogicalRelation'. Thus, with transformDown,
      // it will be matched again and transformed recursively which causes stack overflow exception.
      case l: LeafNode if provider.isSupportedRelation(l) =>
        val relation = provider.getRelation(l)
        val (filesDeleted, filesAppended) =
          if (!HyperspaceConf.hybridScanEnabled(spark) && index.hasSourceUpdate) {
            // If the index contains the source update info, it means the index was validated
            // with the latest signature including appended files and deleted files, but
            // index data is not updated with those files. Therefore, we need to handle
            // appendedFiles and deletedFiles in IndexLogEntry.
            (index.deletedFiles, index.appendedFiles.map(f => new Path(f.name)).toSeq)
          } else {
            val curFiles = relation.allFiles.map(f =>
              FileInfo(f, index.fileIdTracker.addFile(f), asFullPath = true))
            if (HyperspaceConf.hybridScanDeleteEnabled(spark) && index.hasLineageColumn) {
              val (exist, nonExist) = curFiles.partition(index.sourceFileInfoSet.contains)
              val filesAppended = nonExist.map(f => new Path(f.name))
              if (exist.length < index.sourceFileInfoSet.size) {
                (index.sourceFileInfoSet -- exist, filesAppended)
              } else {
                (Nil, filesAppended)
              }
            } else {
              // Append-only implementation of getting appended files for efficiency.
              // It is guaranteed that there is no deleted files via the condition
              // 'deletedCnt == 0 && commonCnt > 0' in isHybridScanCandidate function.
              (
                Nil,
                curFiles.filterNot(index.sourceFileInfoSet.contains).map(f => new Path(f.name)))
            }
          }

        val filesToRead = {
          if (useBucketSpec || !index.hasParquetAsSourceFormat || filesDeleted.nonEmpty ||
              relation.partitionSchema.nonEmpty) {
            // Since the index data is in "parquet" format, we cannot read source files
            // in formats other than "parquet" using one FileScan node as the operator requires
            // files in one homogenous format. To address this, we need to read the appended
            // source files using another FileScan node injected into the plan and subsequently
            // merge the data into the index data. Please refer below [[Union]] operation.
            // In case there are both deleted and appended files, we cannot handle the appended
            // files along with deleted files as source files do not have the lineage column which
            // is required for excluding the index data from deleted files.
            // If the source relation is partitioned, we cannot read the appended files with the
            // index data as the schema of partitioned files are not equivalent to the index data.
            unhandledAppendedFiles = filesAppended
            index.content.files
          } else {
            // If BucketSpec of index data isn't used (e.g., in the case of FilterIndex currently)
            // and the source format is parquet, we could read the appended files along
            // with the index data.
            index.content.files ++ filesAppended
          }
        }

        // In order to handle deleted files, read index data with the lineage column so that
        // we could inject Filter-Not-In conditions on the lineage column to exclude the indexed
        // rows from the deleted files.
        val newSchema = StructType(
          index.schema.filter(s =>
            relation.plan.schema.contains(s) || (filesDeleted.nonEmpty && s.name.equals(
              IndexConstants.DATA_FILE_NAME_ID))))

        def fileIndex: InMemoryFileIndex = {
          new InMemoryFileIndex(spark, filesToRead, Map(), None)
        }

        val newLocation = if (filesToRead.length == index.content.files.size) {
          index.withCachedTag(IndexLogEntryTags.INMEMORYFILEINDEX_INDEX_ONLY)(fileIndex)
        } else {
          index.withCachedTag(plan, IndexLogEntryTags.INMEMORYFILEINDEX_HYBRID_SCAN)(fileIndex)
        }

        val indexFsRelation = new IndexHadoopFsRelation(
          newLocation,
          new StructType(),
          newSchema,
          if (useBucketSpec) Some(index.bucketSpec) else None,
          new ParquetFileFormat,
          Map(IndexConstants.INDEX_RELATION_IDENTIFIER))(spark, index)

        val updatedOutput = relation.plan.output
          .filter(attr => indexFsRelation.schema.fieldNames.contains(attr.name))
          .map(_.asInstanceOf[AttributeReference])

        if (filesDeleted.isEmpty) {
          relation.createLogicalRelation(indexFsRelation, updatedOutput)
        } else {
          val lineageAttr = AttributeReference(IndexConstants.DATA_FILE_NAME_ID, LongType)()
          val deletedFileIds = filesDeleted.map(f => Literal(f.id)).toArray
          val rel =
            relation.createLogicalRelation(indexFsRelation, updatedOutput ++ Seq(lineageAttr))
          val filterForDeleted = Filter(Not(In(lineageAttr, deletedFileIds)), rel)
          Project(updatedOutput, OptimizeIn(filterForDeleted))
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
        transformPlanToReadAppendedFiles(spark, index, plan, unhandledAppendedFiles)
      if (useBucketUnionForAppended && useBucketSpec) {
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
   * @param index Index used in transformation of plan.
   * @param originalPlan Original plan.
   * @param filesAppended Appended files to the source relation.
   * @return Transformed linear logical plan for appended files.
   */
  private def transformPlanToReadAppendedFiles(
      spark: SparkSession,
      index: IndexLogEntry,
      originalPlan: LogicalPlan,
      filesAppended: Seq[Path]): LogicalPlan = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    // Transform the relation node to include appended files.
    val planForAppended = originalPlan transformDown {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        val relation = provider.getRelation(l)
        val options = relation.partitionBasePath
          .map { basePath =>
            // Set "basePath" so that partitioned columns are also included in the output schema.
            Map("basePath" -> basePath)
          }
          .getOrElse(Map())

        val newLocation = index.withCachedTag(
          originalPlan,
          IndexLogEntryTags.INMEMORYFILEINDEX_HYBRID_SCAN_APPENDED) {
          new InMemoryFileIndex(spark, filesAppended, options, None)
        }
        // Set the same output schema with the index plan to merge them using BucketUnion.
        // Include partition columns for data loading.
        val partitionColumns = relation.partitionSchema.map(_.name)
        val updatedSchema = StructType(relation.plan.schema.filter(col =>
          index.schema.contains(col) || relation.partitionSchema.contains(col)))
        val updatedOutput = relation.plan.output
          .filter(attr =>
            index.schema.fieldNames.contains(attr.name) || partitionColumns.contains(attr.name))
          .map(_.asInstanceOf[AttributeReference])
        val newRelation = relation.createHadoopFsRelation(
          newLocation,
          updatedSchema,
          relation.options + IndexConstants.INDEX_RELATION_IDENTIFIER)
        relation.createLogicalRelation(newRelation, updatedOutput)
    }
    assert(!originalPlan.equals(planForAppended))
    planForAppended
  }

  /**
   * Transform the plan to perform on-the-fly Shuffle the data based on bucketSpec.
   * Note that this extractor depends on [[LogicalRelation]] with [[HadoopFsRelation]] because
   * the given plan is the result of [[transformPlanToReadAppendedFiles]], which converts
   * the original relation to [[LogicalRelation]] with [[HadoopFsRelation]].
   *
   * Pre-requisite
   * - The plan should be linear and include one LogicalRelation.
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
