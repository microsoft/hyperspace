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
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, In, Literal, NamedExpression, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, RepartitionByExpression}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{Metadata, StringType, StructType}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{IndexConstants, IndexLogEntry, IndexManager, LogicalPlanSignatureProvider}
import com.microsoft.hyperspace.index.Content.Directory.FileInfo
import com.microsoft.hyperspace.index.plans.logical.BucketUnion

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
      spark: SparkSession,
      indexManager: IndexManager,
      plan: LogicalPlan): Seq[IndexLogEntry] = {
    // Map of a signature provider to a signature generated for the given plan.
    val signatureMap = mutable.Map[(String, Option[Set[Path]]), Option[String]]()
    val hybridScanEnabled = spark.sessionState.conf
      .getConfString(
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED,
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED_DEFAULT.toString)
      .toBoolean

    def signatureValid(entry: IndexLogEntry, sourceFileSet: Option[Set[Path]]): Boolean = {
      val sourcePlanSignatures = entry.source.plan.properties.fingerprint.properties.signatures
      assert(sourcePlanSignatures.length == 1)
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

    def isCandidate(entry: IndexLogEntry): Boolean = {
      if (hybridScanEnabled) {
        assert(entry.relations.length == 1)
        // entry.allSourceFiles
        var files = scala.collection.mutable.Seq[FileInfo]()
        plan.foreach {
          case LogicalRelation(
              HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
              _,
              _,
              _) =>
            files ++= location.allFiles.map(f =>
              FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))
        }
        val commonFiles = files.toSet.intersect(entry.allSourceFiles)
        commonFiles.nonEmpty
      } else {
        signatureValid(entry, None)
      }
    }

    // TODO: the following check only considers indexes in ACTIVE state for usage. Update
    //  the code to support indexes in transitioning states as well.
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    allIndexes.filter(index => index.created && isCandidate(index))
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
   * @param spark Spark session
   * @param index index used in replacement plan
   * @param plan current plan
   * @return replacement plan
   */
  def getReplacementPlan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    // Here we can't replace the plan completely with the index. This will create problems.
    // For e.g. if Project(A,B) -> Filter(C = 10) -> Scan (A,B,C,D,E)
    // if we replace this plan with index Scan (A,B,C), we lose the Filter(C=10) and it will
    // lead to wrong results. So we only replace the base relation.
    val hybridScanEnabled = spark.sessionState.conf
      .getConfString(
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED,
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED_DEFAULT.toString)
      .toBoolean

    if (hybridScanEnabled) {
      getHybridScanIndexPlan(spark, index, plan, useBucketSpec)
    } else {
      getIndexPlan(spark, index, plan, useBucketSpec)
    }
  }

  private def getIndexPlan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    plan transformUp {
      case baseRelation @ LogicalRelation(_: HadoopFsRelation, baseOutput, _, _) =>
        val location =
          new InMemoryFileIndex(spark, Seq(new Path(index.content.root)), Map(), None)
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

  private def getHybridScanIndexPlan(
      spark: SparkSession,
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    var useBucketUnion = useBucketSpec
    val replacedPlan = plan transformUp {
      case baseRelation @ LogicalRelation(
            fsRelation @ HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
            baseOutput,
            _,
            _) =>
        val curFileSet = location.allFiles
          .map(f => FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))
          .toSet
        // if BucketSpec of index data isn't used, we could read appended data from
        // source files directly.
        val filesAppended = if (!useBucketUnion) {
          (curFileSet -- index.allSourceFiles).map(f => new Path(f.name)).toSeq
        } else Seq()
        val filesDeleted =
          (index.allSourceFiles -- curFileSet).toSeq.map(f =>
            f.name.replace("file:/C:", "file:///C:")) // TODO fix

        if (filesAppended.nonEmpty && filesDeleted.nonEmpty) {
          // Because appended source files do not have the lineage column in its schema,
          // we cannot read with the files with the lineage column. Therefore we cannot
          // handle both appended data & deleted data by adding appended file paths to
          // the location & injecting the lineage filter for deleted files concurrently.
          useBucketUnion = true
        }

        val readSchema = if (filesDeleted.isEmpty) {
          StructType(index.schema.filter(baseRelation.schema.contains(_)))
        } else {
          StructType(index.schema)
        }
        val readPaths = if (useBucketUnion || filesAppended.isEmpty) {
          Seq(new Path(index.content.root))
        } else {
          Seq(new Path(index.content.root)) ++ filesAppended
        }

        val newLocation = new InMemoryFileIndex(spark, readPaths, Map(), None)
        val relation = HadoopFsRelation(
          newLocation,
          new StructType(),
          readSchema,
          if (useBucketUnion) Some(index.bucketSpec) else None,
          new ParquetFileFormat,
          Map())(spark)

        if (filesDeleted.isEmpty) {
          val updatedOutput =
            baseOutput.filter(attr => relation.schema.fieldNames.contains(attr.name))
          baseRelation.copy(relation = relation, output = updatedOutput)
        } else {
          val lAttr = AttributeReference(IndexConstants.DATA_FILE_NAME_COLUMN, StringType)(
            NamedExpression.newExprId)
          val updatedOutput =
            baseOutput.filter(attr => relation.schema.fieldNames.contains(attr.name))
          val deletedFileNames = filesDeleted.map(Literal(_)).toArray
          val rel = baseRelation.copy(relation = relation, output = updatedOutput ++ Seq(lAttr))
          Project(updatedOutput, Filter(Not(In(lAttr, deletedFileNames)), rel))
        }
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
    val complementIndexPlan = originalPlan transformUp {
      case baseRelation @ LogicalRelation(
            fsRelation @ HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
            baseOutput,
            _,
            _) =>
        val updatedOutput =
          baseOutput.filter(attr => index.schema.fieldNames.contains(attr.name))

        val filesNotCovered = (location.allFiles
          .map(f => FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))
          .toSet -- index.allSourceFiles).toSeq.map(f => new Path(f.name))

        if (filesNotCovered.nonEmpty) {
          val newLocation =
            new InMemoryFileIndex(spark, filesNotCovered, Map(), None)
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
      // Remove sort order because original data isn't sorted by the same columns as the index.
      val bucketSpec = index.bucketSpec.copy(sortColumnNames = Seq())
      val attrs = complementIndexPlan.output.attrs.filter { attr =>
        index.indexedColumns contains attr.name
      }

      // 2) Perform on-the-fly shuffle with the same partition structure of index
      // so that we could avoid incurring shuffle whole index data at merge stage.
      val shuffled = if (attrs.size != index.indexedColumns.size) {
        // There should be a project node which excludes one or more indexedColumns.
        // In this case, shuffle will be replaced to RoundRobinPartitioning which can cause
        // wrong result issue. So shuffle should be located before the project node.
        var projectSeen = false
        val transformed = complementIndexPlan transformUp {
          case project @ Project(_, child) =>
            val childAttrs = child.output.attrs.filter { attr =>
              index.indexedColumns contains attr.name
            }
            projectSeen = true
            assert(childAttrs.size == index.indexedColumns.size)
            project.copy(child = RepartitionByExpression(childAttrs, child, index.numBuckets))
        }
        assert(projectSeen)
        transformed
      } else {
        RepartitionByExpression(attrs, complementIndexPlan, index.numBuckets)
      }
      // 3) Merge index plan & newly shuffled plan by using bucket-aware union.
      // Currently, BucketUnion does not keep the sort order within a bucket.
      BucketUnion(Seq(indexPlan, shuffled), bucketSpec)
    } else {
      indexPlan
    }
  }
}
