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

package com.microsoft.hyperspace.index.dataskipping

import scala.collection.mutable

import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.hyperspace.utils.StructTypeUtils
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.dataskipping.expressions._
import com.microsoft.hyperspace.index.dataskipping.sketches.{PartitionSketch, Sketch}
import com.microsoft.hyperspace.index.dataskipping.util.DataFrameUtils
import com.microsoft.hyperspace.util.HyperspaceConf

/**
 * DataSkippingIndex is an index that can accelerate queries by filtering out
 * files in relations using sketches.
 *
 * @param sketches List of sketches for this index
 * @param schema Index data schema
 * @param properties Properties for this index; see [[Index.properties]] for details.
 */
case class DataSkippingIndex(
    sketches: Seq[Sketch],
    schema: StructType,
    override val properties: Map[String, String] = Map.empty)
    extends Index {
  assert(sketches.nonEmpty, "At least one sketch is required.")

  override def kind: String = DataSkippingIndex.kind

  override def kindAbbr: String = DataSkippingIndex.kindAbbr

  override def indexedColumns: Seq[String] = sketches.flatMap(_.indexedColumns).distinct

  override def referencedColumns: Seq[String] = sketches.flatMap(_.referencedColumns).distinct

  override def withNewProperties(newProperties: Map[String, String]): DataSkippingIndex = {
    copy(properties = newProperties)
  }

  override def statistics(extended: Boolean = false): Map[String, String] = {
    Map("sketches" -> sketches.mkString(", "))
  }

  override def canHandleDeletedFiles: Boolean = true

  override def write(ctx: IndexerContext, indexData: DataFrame): Unit = {
    writeImpl(ctx, indexData, SaveMode.Overwrite)
  }

  override def optimize(ctx: IndexerContext, indexDataFilesToOptimize: Seq[FileInfo]): Unit = {
    val indexData =
      ctx.spark.read.schema(schema).parquet(indexDataFilesToOptimize.map(_.name): _*)
    writeImpl(ctx, indexData, SaveMode.Overwrite)
  }

  override def refreshIncremental(
      ctx: IndexerContext,
      appendedSourceData: Option[DataFrame],
      deletedSourceDataFiles: Seq[FileInfo],
      indexContent: Content): (Index, Index.UpdateMode) = {
    if (appendedSourceData.nonEmpty) {
      writeImpl(
        ctx,
        DataSkippingIndex.createIndexData(ctx, sketches, appendedSourceData.get),
        SaveMode.Overwrite)
    }
    if (deletedSourceDataFiles.nonEmpty) {
      val spark = ctx.spark
      import spark.implicits._
      val deletedFileIds = deletedSourceDataFiles.map(_.id).toDF(IndexConstants.DATA_FILE_NAME_ID)
      val updatedIndexData = spark.read
        .parquet(indexContent.files.map(_.toString): _*)
        .join(deletedFileIds, Seq(IndexConstants.DATA_FILE_NAME_ID), "left_anti")
      val writeMode = if (appendedSourceData.nonEmpty) {
        SaveMode.Append
      } else {
        SaveMode.Overwrite
      }
      writeImpl(ctx, updatedIndexData, writeMode)
    }
    val updateMode = if (deletedSourceDataFiles.isEmpty) {
      Index.UpdateMode.Merge
    } else {
      Index.UpdateMode.Overwrite
    }
    (this, updateMode)
  }

  override def refreshFull(
      ctx: IndexerContext,
      sourceData: DataFrame): (DataSkippingIndex, DataFrame) = {
    val resolvedSketches = ExpressionUtils.resolve(ctx.spark, sketches, sourceData)
    val indexData = DataSkippingIndex.createIndexData(ctx, resolvedSketches, sourceData)
    val updatedIndex = copy(sketches = resolvedSketches, schema = indexData.schema)
    (updatedIndex, indexData)
  }

  override def equals(that: Any): Boolean =
    that match {
      case DataSkippingIndex(thatSketches, thatSchema, _) =>
        sketches.toSet == thatSketches.toSet && schema == thatSchema
      case _ => false
    }

  override def hashCode: Int = sketches.map(_.hashCode).sum

  /**
   * Translate the given filter/join condition for the source data to a
   * predicate that can be used to filter out unnecessary source data files
   * when applied to index data.
   *
   * For example, a filter condition "A = 1" can be translated into an index
   * predicate "Min_A <= 1 && Max_A >= 1" to filter out files which cannot
   * satisfy the condition for any rows in the file.
   *
   * It is assumed that the condition is in negation normal form. If it is not,
   * then it may fail to translate the condition which would have been possible
   * otherwise. This is a valid assumption for Spark 2.4 and later.
   */
  def translateFilterCondition(
      spark: SparkSession,
      condition: Expression,
      source: LogicalPlan): Option[Expression] = {
    val resolvedExprs =
      ExpressionUtils.getResolvedExprs(spark, sketches, source).getOrElse { return None }
    val predMap = buildPredicateMap(condition, source, resolvedExprs)

    // Create a single index predicate for a single source predicate node,
    // by combining individual index predicates with And.
    // True is returned if there are no index predicates for the source predicate node.
    def toIndexPred(sourcePred: Expression): Expression = {
      predMap.get(sourcePred).map(_.reduceLeft(And)).getOrElse(TrueLiteral)
    }

    // Compose an index predicate visiting the source predicate tree recursively.
    def composeIndexPred(sourcePred: Expression): Expression =
      sourcePred match {
        case and: And => And(toIndexPred(and), and.mapChildren(composeIndexPred))
        case or: Or => And(toIndexPred(or), or.mapChildren(composeIndexPred))
        case leaf => toIndexPred(leaf)
      }

    val indexPredicate = composeIndexPred(condition)

    // Apply constant folding to get the final predicate.
    // This is a trimmed down version of the BooleanSimplification rule.
    // It's just enough to determine whether the index is applicable or not.
    val optimizePredicate: PartialFunction[Expression, Expression] = {
      case And(TrueLiteral, right) => right
      case And(left, TrueLiteral) => left
      case Or(TrueLiteral, _) => TrueLiteral
      case Or(_, TrueLiteral) => TrueLiteral
    }
    val optimizedIndexPredicate = indexPredicate.transformUp(optimizePredicate)

    // Return None if the index predicate is True - meaning no conversion can be done.
    if (optimizedIndexPredicate == TrueLiteral) {
      None
    } else {
      Some(optimizedIndexPredicate)
    }
  }

  private def writeImpl(ctx: IndexerContext, indexData: DataFrame, writeMode: SaveMode): Unit = {
    // require instead of assert, as the condition can potentially be broken by
    // code which is external to dataskipping.
    require(
      indexData.schema.sameType(schema),
      "Schema of the index data doesn't match the index schema: " +
        s"index data schema = ${indexData.schema.toDDL}, index schema = ${schema.toDDL}")
    indexData.cache()
    indexData.count() // force cache
    val indexDataSize = DataFrameUtils.getSizeInBytes(indexData)
    val targetIndexDataFileSize = HyperspaceConf.DataSkipping.targetIndexDataFileSize(ctx.spark)
    val maxIndexDataFileCount = HyperspaceConf.DataSkipping.maxIndexDataFileCount(ctx.spark)
    val numFiles = {
      val n = indexDataSize / targetIndexDataFileSize
      math.min(math.max(1, n), maxIndexDataFileCount).toInt
    }
    val repartitionedIndexData = indexData.repartition(numFiles)
    repartitionedIndexData.write.mode(writeMode).parquet(ctx.indexDataPath.toString)
    indexData.unpersist()
  }

  /**
   * Collects index predicates for each node in the source predicate.
   */
  private def buildPredicateMap(
      predicate: Expression,
      source: LogicalPlan,
      resolvedExprs: Map[Sketch, Seq[Expression]])
      : scala.collection.Map[Expression, Seq[Expression]] = {
    val predMap = mutable.Map[Expression, mutable.Buffer[Expression]]()
    val sketchesWithIndex = sketches.zipWithIndex
    val nameMap = source.output.map(attr => attr.exprId -> attr.name).toMap
    val attrMap = buildAttrMap(predicate, resolvedExprs, nameMap)
    val valueExtractor = AttrValueExtractor(attrMap)
    def updatePredMap(sourcePred: Expression): Unit = {
      val indexPreds = sketchesWithIndex.flatMap {
        case (sketch, idx) =>
          sketch.convertPredicate(
            sourcePred,
            resolvedExprs(sketch),
            aggrNames(idx).map(UnresolvedAttribute.quoted),
            nameMap,
            valueExtractor)
      }
      if (indexPreds.nonEmpty) {
        predMap.getOrElseUpdate(sourcePred, mutable.Buffer.empty) ++= indexPreds
      }
    }
    def forEachTerm(p: Expression, f: Expression => Unit): Unit = {
      f(p)
      p match {
        case And(_, _) | Or(_, _) => p.children.foreach(forEachTerm(_, f))
        case _ =>
      }
    }
    forEachTerm(predicate, updatePredMap)
    predMap
  }

  private def buildAttrMap(
      predicate: Expression,
      resolvedExprs: Map[Sketch, Seq[Expression]],
      nameMap: Map[ExprId, String]): Map[Attribute, Expression] = {
    val partitionSketchIdx = sketches.indexWhere(_.isInstanceOf[PartitionSketch])
    if (partitionSketchIdx != -1) {
      val partitionSketch = sketches(partitionSketchIdx)
      val sketchValues = aggrNames(partitionSketchIdx).map(UnresolvedAttribute.quoted)
      val exprExtractors = resolvedExprs(partitionSketch).map(NormalizedExprExtractor(_, nameMap))
      val exprsAndValues = exprExtractors.zip(sketchValues)
      predicate.references
        .flatMap(a => exprsAndValues.find(_._1.unapply(a).isDefined).map(a -> _._2))
        .toMap
    } else {
      Map.empty
    }
  }

  private def aggrNames(i: Int): Seq[String] = {
    aggregateFunctions
      .slice(sketchOffsets(i), sketchOffsets(i + 1))
      .map(_.expr.asInstanceOf[NamedExpression].name)
  }

  /**
   * Sketch offsets are used to map each sketch to its corresponding columns
   * in the dataframe.
   */
  @transient
  private lazy val sketchOffsets: Seq[Int] =
    sketches.map(_.aggregateFunctions.length).scanLeft(0)(_ + _)

  @transient
  private lazy val aggregateFunctions = DataSkippingIndex.getNamedAggregateFunctions(sketches)
}

object DataSkippingIndex {
  // $COVERAGE-OFF$ https://github.com/scoverage/scalac-scoverage-plugin/issues/125
  final val kind = "DataSkippingIndex"
  final val kindAbbr = "DS"
  // $COVERAGE-ON$

  /**
   * Creates index data for the given source data.
   */
  def createIndexData(
      ctx: IndexerContext,
      sketches: Seq[Sketch],
      sourceData: DataFrame): DataFrame = {
    val fileNameCol = "input_file_name"
    val aggregateFunctions = getNamedAggregateFunctions(sketches)
    val indexDataWithFileName = sourceData
      .groupBy(input_file_name().as(fileNameCol))
      .agg(aggregateFunctions.head, aggregateFunctions.tail: _*)

    // Construct a dataframe to convert file names to file ids.
    val spark = ctx.spark
    val relation = RelationUtils.getRelation(spark, sourceData.queryExecution.optimizedPlan)
    import spark.implicits._
    val fileIdDf = ctx.fileIdTracker
      .getIdToFileMapping()
      .toDF(IndexConstants.DATA_FILE_NAME_ID, fileNameCol)

    indexDataWithFileName
      .join(
        fileIdDf.hint("broadcast"),
        IndexUtils.getPath(IndexUtils.decodeInputFileName(indexDataWithFileName(fileNameCol))) ===
          IndexUtils.getPath(fileIdDf(fileNameCol)))
      .select(
        IndexConstants.DATA_FILE_NAME_ID,
        indexDataWithFileName.columns.filterNot(_ == fileNameCol).map(c => s"`$c`"): _*)
  }

  def getNamedAggregateFunctions(sketches: Seq[Sketch]): Seq[Column] = {
    sketches.flatMap { s =>
      val aggrs = s.aggregateFunctions
      assert(aggrs.nonEmpty)
      aggrs.zipWithIndex.map {
        case (aggr, idx) =>
          new Column(aggr.toAggregateExpression).as(getNormalizeColumnName(s"${s}_$idx"))
      }
    }
  }

  /**
   * Returns a normalized column name valid for a Parquet format.
   */
  private def getNormalizeColumnName(name: String): String = {
    name.replaceAll("[ ,;{}()\n\t=]", "_")
  }
}
