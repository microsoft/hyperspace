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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, BinaryOperator, ExprId, GetStructField, In, IsNotNull, Literal, NamedExpression, Not}
import org.apache.spark.sql.catalyst.optimizer.OptimizeIn
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LeafNode, LogicalPlan, Project, Union}
import org.apache.spark.sql.execution.datasources.InMemoryFileIndex
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{LongType, StructType}

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.{FileInfo, IndexConstants, IndexLogEntry, IndexLogEntryTags}
import com.microsoft.hyperspace.index.plans.logical.{BucketUnion, IndexHadoopFsRelation}
import com.microsoft.hyperspace.index.rules.PlanUtils._
import com.microsoft.hyperspace.util.{HyperspaceConf, ResolverUtils}

class NestedRuleHelper(spark: SparkSession) extends BaseRuleHelper(spark) {

  /**
   * Transform the current plan to utilize index.
   * The transformed plan reads data from indexes instead of the source relations.
   * Bucketing information of the index is retained if useBucketSpec is true.
   *
   * NOTE: This method currently only supports transformation of nodes with supported relations.
   *
   * @param index Index used in transformation of plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @return Transformed logical plan that leverages an index.
   */
  override protected[rules] def transformPlanToUseIndexOnlyScan(
      index: IndexLogEntry,
      plan: LogicalPlan,
      useBucketSpec: Boolean): LogicalPlan = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    // Note that depending on the case we transform only the base relation
    // and sometimes other portions of the plan (e.g., filters). For instance,
    // given the following input plan:
    //        Project(A,B) -> Filter(C = 10) -> Scan (A,B,C,D,E)
    // in the presence of a suitable index, we will transform to:
    //        Project(A,B) -> Filter(C = 10) -> Index Scan (A,B,C)
    // In the case of nested fields we will transform the project and
    // filter nodes too.
    plan transformUp {
      case l: LeafNode if provider.isSupportedRelation(l) =>
        val relation = provider.getRelation(l)
        val location = index.withCachedTag(IndexLogEntryTags.INMEMORYFILEINDEX_INDEX_ONLY) {
          new InMemoryFileIndex(spark, index.content.files, Map(), None)
        }

        val newSchema = StructType(
          index.schema.filter(i => relation.plan.schema.exists(j => i.name.contains(j.name))))

        val indexFsRelation = new IndexHadoopFsRelation(
          location,
          new StructType(),
          newSchema,
          if (useBucketSpec) Some(index.bucketSpec) else None,
          new ParquetFileFormat,
          Map(IndexConstants.INDEX_RELATION_IDENTIFIER))(spark, index)

        val resolvedFields = ResolverUtils.resolve(
          spark,
          (index.indexedColumns ++ index.includedColumns)
            .map(ResolverUtils.ResolvedColumn(_).name),
          relation.plan)
        val updatedOutput =
          if (resolvedFields.isDefined && resolvedFields.get.exists(_.isNested)) {
            indexFsRelation.schema.flatMap { s =>
              relation.plan.output
                .find { a =>
                  ResolverUtils.ResolvedColumn(s.name).name.startsWith(a.name)
                }
                .map { a =>
                  AttributeReference(s.name, s.dataType, a.nullable, a.metadata)(
                    NamedExpression.newExprId,
                    a.qualifier)
                }
            }
          } else {
            relation.plan.output
              .filter(attr => indexFsRelation.schema.fieldNames.contains(attr.name))
              .map(_.asInstanceOf[AttributeReference])
          }
        relation.createLogicalRelation(indexFsRelation, updatedOutput)

      // Given that the index may have top level field for a nested one
      // it is needed to transform the projection to use that index field
      case p: Project if hasNestedColumns(p, index) =>
        transformProject(p)

      // Given that the index may have top level field for a nested one
      // it is needed to transform the filter to use that index field
      case f: Filter if hasNestedColumns(f, index) =>
        transformFilter(f)
    }
  }

  /**
   * Transform the current plan to utilize the given index along with newly appended source files.
   *
   * With HybridScan, indexes with newly appended files to its source relation are also
   * eligible and we reconstruct new plans for the appended files so as to merge with
   * bucketed index data correctly.
   *
   * @param index Index used in transformation of plan.
   * @param plan Current logical plan.
   * @param useBucketSpec Option whether to use BucketSpec for reading index data.
   * @param useBucketUnionForAppended Option whether to use BucketUnion to merge appended data.
   * @return Transformed logical plan that leverages an index and merges appended data.
   */
  override protected[rules] def transformPlanToUseHybridScan(
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
        transformPlanToReadAppendedFiles(index, plan, unhandledAppendedFiles)
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
   * @param index Index used in transformation of plan.
   * @param originalPlan Original plan.
   * @param filesAppended Appended files to the source relation.
   * @return Transformed linear logical plan for appended files.
   */
  override protected[rules] def transformPlanToReadAppendedFiles(
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
   * The method transforms the project part of a plan to support indexes on
   * nested fields.
   *
   * For example, given the following query:
   * {{{
   *   df
   *     .filter("nested.leaf.cnt > 10 and nested.leaf.id == 'leaf_id9'")
   *     .select("Date", "nested.leaf.cnt")
   * }}}
   *
   * Having this simple projection:
   * {{{
   *   Project [Date#100, nested#102.leaf.cnt]
   * }}}
   *
   * The projection part should become:
   * {{{
   *   Project [Date#330, __hs_nested.nested.leaf.cnt#335]
   * }}}
   *
   * @param project The project that needs to be transformed.
   * @return The transformed project with support for nested indexed fields.
   */
  protected[rules] def transformProject(project: Project): Project = {
    val projectedFields = project.projectList.map { exp =>
      val fieldName = extractNamesFromExpression(exp).toKeep.head
      val escapedFieldName = PlanUtils.prefixNestedField(fieldName)
      val attr = extractAttributeRef(exp, fieldName)
      val fieldType = extractTypeFromExpression(exp, fieldName)
      // Try to find it in the project transformed child.
      getExprId(project.child, escapedFieldName) match {
        case Some(exprId) =>
          attr.copy(escapedFieldName, fieldType, attr.nullable, attr.metadata)(
            exprId,
            attr.qualifier)
        case _ =>
          attr
      }
    }
    project.copy(projectList = projectedFields)
  }

  /**
   * The method transforms the filter part of a plan to support indexes on
   * nested fields. The process is to go through all expression nodes and
   * do the following things:
   *  - Replace retrieval of nested values with index ones.
   *  - In some specific cases remove the `isnotnull` check because that
   *    is used on the root of the nested field (ie: `isnotnull(nested#102)`
   *    does not makes any sense when using the index field).
   *
   * For example, given the following query:
   * {{{
   *   df
   *     .filter("nested.leaf.cnt > 10 and nested.leaf.id == 'leaf_id9'")
   *     .select("Date", "nested.leaf.cnt")
   * }}}
   *
   * Having this simple filter:
   * {{{
   *   Filter (isnotnull(nested#102) && (nested#102.leaf.cnt > 10) &&
   *           (nested#102.leaf.id = leaf_id9))
   * }}}
   *
   * The filter part should become:
   * {{{
   *   Filter ((__hs_nested.nested.leaf.cnt#335 > 10) &&
   *           (__hs_nested.nested#.leaf.id#336 = leaf_id9))
   * }}}
   *
   * @param filter The filter that needs to be transformed.
   * @return The transformed filter with support for nested indexed fields.
   */
  protected[rules] def transformFilter(filter: Filter): Filter = {
    val names = extractNamesFromExpression(filter.condition)
    val transformedCondition = filter.condition.transformDown {
      case bo @ BinaryOperator(IsNotNull(AttributeReference(name, _, _, _)), other) =>
        if (names.toDiscard.contains(name)) {
          other
        } else {
          bo
        }
      case bo @ BinaryOperator(other, IsNotNull(AttributeReference(name, _, _, _))) =>
        if (names.toDiscard.contains(name)) {
          other
        } else {
          bo
        }
      case g: GetStructField =>
        val n = getChildNameFromStruct(g)
        if (names.toKeep.contains(n)) {
          val escapedFieldName = PlanUtils.prefixNestedField(n)
          getExprId(filter, escapedFieldName) match {
            case Some(exprId) =>
              val fieldType = extractTypeFromExpression(g, n)
              val attr = extractAttributeRef(g, n)
              attr.copy(escapedFieldName, fieldType, attr.nullable, attr.metadata)(
                exprId,
                attr.qualifier)
            case _ =>
              g
          }
        } else {
          g
        }
      case o =>
        o
    }
    filter.copy(condition = transformedCondition)
  }

  /**
   * Returns true if the given project is a supported project. If all of the registered
   * providers return None, this returns false.
   *
   * @param project Project to check if it's supported.
   * @return True if the given project is a supported relation.
   */
  protected[rules] def hasNestedColumns(project: Project, index: IndexLogEntry): Boolean = {
    val indexCols =
      (index.indexedColumns ++ index.includedColumns).map(i => ResolverUtils.ResolvedColumn(i))
    val hasNestedCols = indexCols.exists(_.isNested)
    if (hasNestedCols) {
      val projectListFields = project.projectList.flatMap(extractNamesFromExpression(_).toKeep)
      val containsNestedFields =
        projectListFields.exists(i => indexCols.exists(j => j.isNested && j.name == i))
      var containsNestedChildren = false
      project.child.foreach {
        case f: Filter =>
          val filterSupported = hasNestedColumns(f, index)
          containsNestedChildren = containsNestedChildren || filterSupported
        case _ =>
      }
      containsNestedFields || containsNestedChildren
    } else {
      false
    }
  }

  /**
   * Returns true if the given filter has nested columns.
   *
   * @param filter Filter to check if it's supported.
   * @return True if the given project is a supported relation.
   */
  protected[rules] def hasNestedColumns(filter: Filter, index: IndexLogEntry): Boolean = {
    val indexCols =
      (index.indexedColumns ++ index.includedColumns).map(i => ResolverUtils.ResolvedColumn(i))
    val hasNestedCols = indexCols.exists(_.isNested)
    if (hasNestedCols) {
      val filterFields = extractNamesFromExpression(filter.condition).toKeep.toSeq
      val resolvedFilterFields = filterFields.map(ResolverUtils.ResolvedColumn(_))
      resolvedFilterFields.exists(i => indexCols.exists(j => j == i || j.name == i.name))
    } else {
      false
    }
  }

  /**
   * The method retrieves the expression id for a given field name.
   *
   * This method should be mainly used when transforming plans and the
   * leaves are already transformed.
   *
   * @param plan The logical plan from which to get the expression id.
   * @param fieldName The name of the field to search for.
   * @return An [[ExprId]] if that could be found in the plan otherwise [[None]].
   */
  private def getExprId(plan: LogicalPlan, fieldName: String): Option[ExprId] = {
    plan.output.find(a => a.name.equalsIgnoreCase(fieldName)).map(_.exprId)
  }
}
