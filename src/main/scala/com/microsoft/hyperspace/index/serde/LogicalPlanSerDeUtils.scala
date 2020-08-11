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

package com.microsoft.hyperspace.index.serde

import java.util.Base64

import org.apache.hadoop.fs.Path
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Exists, InSubquery, ListQuery, ScalarSubquery, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.orc.OrcFileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat

import com.microsoft.hyperspace.HyperspaceException

/**
 * Utility functions for logical plan serialization/deserialization.
 */
object LogicalPlanSerDeUtils {

  /**
   * Serialize the given logical plan to an array of bytes. Use Kryo serializer internally.
   *
   * @param logicalPlan The logical plan to be serialized.
   * @param spark The Spark session.
   * @return The Base64 encoded string representation of the serialized plan.
   */
  def serialize(logicalPlan: LogicalPlan, spark: SparkSession): String = {
    // Replace non-serializable operators/expressions with corresponding wrappers.
    val replacedPlan = replace(logicalPlan)

    // Create a Kryo serializer to serialize the plan.
    val kryoSerializer = new KryoSerializer(spark.sparkContext.getConf)
    val bytes = KryoSerDeUtils.serialize(kryoSerializer, replacedPlan)
    Base64.getEncoder.encodeToString(bytes)
  }

  /**
   * Deserialize logical plan from the given array of bytes. Use Kryo deserializer internally.
   *
   * @param serializedPlan The array of bytes that store the serialized plan.
   * @param spark The Spark session.
   * @return The deserialized plan.
   */
  def deserialize(serializedPlan: String, spark: SparkSession): LogicalPlan = {
    // Create a Kryo serializer to deserialize the plan.
    val kryoSerializer = new KryoSerializer(spark.sparkContext.getConf)

    // Read binary data objects back and covert to logical plan.
    val bytes = Base64.getDecoder.decode(serializedPlan)
    val deserializedPlan = KryoSerDeUtils.deserialize[LogicalPlan](kryoSerializer, bytes)

    // Restore non-serializable operators/expressions from their corresponding wrappers.
    restore(deserializedPlan, spark)
  }

  /**
   * Replace all occurrences of non-serializable operators and expressions in the given logical
   * plan with their corresponding wrappers (for proper serialization).
   *
   * @param plan The original logical plan.
   * @return The logical plan after replacing all non-serializable operators and expressions.
   */
  private def replace(plan: LogicalPlan): LogicalPlan = {
    // Replace all subquery expressions that are non-serializable in the logical plan.
    val newPlan = plan transformAllExpressions {
      case e: ScalarSubquery =>
        ScalarSubqueryWrapper(replace(e.plan), e.children, e.exprId)
      case e: ListQuery =>
        ListQueryWrapper(replace(e.plan), e.children, e.exprId, e.childOutputs)
      case e: Exists =>
        ExistsWrapper(replace(e.plan), e.children, e.exprId)
      case e: ScalaUDF =>
        ScalaUDFWrapper(
          e.function,
          e.dataType,
          e.children,
          e.inputEncoders,
          e.udfName,
          e.nullable,
          e.udfDeterministic)
      case e: InSubquery =>
        InSubqueryWrapper(
          e.values,
          ListQueryWrapper(
            replace(e.query.plan),
            e.query.children,
            e.query.exprId,
            e.query.childOutputs))
    }

    // Replace all non-serializable operators.
    newPlan transform {
      case p: With =>
        // Need to transform each cteRelation.
        With(replace(p.child), p.cteRelations.map {
          case (r, s) => (r, SubqueryAlias(s.alias, replace(s.child)))
        })
      case p: Intersect =>
        IntersectWrapper(replace(p.left), replace(p.right), p.isAll)
      case p: Except =>
        ExceptWrapper(replace(p.left), replace(p.right), p.isAll)
      case LogicalRelation(
          HadoopFsRelation(
            location: InMemoryFileIndex,
            partitionSchema,
            dataSchema,
            bucketSpec,
            ExtractSerializableFileFormat(fileFormat),
            options),
          output,
          catalogTable,
          isStreaming) =>
        LogicalRelationWrapper(
          HadoopFsRelationWrapper(
            InMemoryFileIndexWrapper(location.rootPaths.map(path => path.toString)),
            partitionSchema,
            dataSchema,
            bucketSpec,
            fileFormat,
            options),
          output,
          catalogTable,
          isStreaming)
    }
  }

  /**
   * Restore all occurrences of non-serializable operators and expressions in the given logical
   * plan (after proper deserialization).
   *
   * @param plan The logical plan after deserialization.
   * @param spark The Spark session.
   * @return The deserialized logical plan after restoring all non-serializable operators and
   *         expressions.
   */
  private def restore(plan: LogicalPlan, spark: SparkSession): LogicalPlan = {
    // Restore all non-serializable operators.
    val newPlan = plan transform {
      case p: With =>
        // Need to transform each cteRelation.
        With(restore(p.child, spark), p.cteRelations.map {
          case (r, s) => (r, SubqueryAlias(s.alias, restore(s.child, spark)))
        })
      case p: IntersectWrapper =>
        Intersect(restore(p.left, spark), restore(p.right, spark), p.isAll)
      case p: ExceptWrapper =>
        Except(restore(p.left, spark), restore(p.right, spark), p.isAll)
      case LogicalRelationWrapper(
          HadoopFsRelationWrapper(
            location: InMemoryFileIndexWrapper,
            partitionSchema,
            dataSchema,
            bucketSpec,
            ExtractFileFormat(fileFormat),
            options),
          output,
          catalogTable,
          isStreaming) =>
        LogicalRelation(
          HadoopFsRelation(
            new InMemoryFileIndex(
              spark,
              location.rootPathStrings.map(path => new Path(path)),
              Map(),
              None),
            partitionSchema,
            dataSchema,
            bucketSpec,
            fileFormat,
            options)(spark),
          output,
          catalogTable,
          isStreaming)
    }

    // Restore all subquery expressions that are non-serializable in the logical plan.
    newPlan transformAllExpressions {
      case e: ScalarSubqueryWrapper =>
        ScalarSubquery(restore(e.plan, spark), e.children, e.exprId)
      case e: ListQueryWrapper =>
        ListQuery(restore(e.plan, spark), e.children, e.exprId, e.childOutputs)
      case e: ExistsWrapper =>
        Exists(restore(e.plan, spark), e.children, e.exprId)
      case e: ScalaUDFWrapper =>
        ScalaUDF(
          e.function,
          e.dataType,
          e.children,
          e.inputEncoders,
          e.udfName,
          e.nullable,
          e.udfDeterministic)
      case e: InSubqueryWrapper =>
        InSubquery(
          e.values,
          ListQuery(
            restore(e.query.plan, spark),
            e.query.children,
            e.query.exprId,
            e.query.childOutputs))
    }
  }

  object ExtractFileFormat {
    def unapply(fileFormat: FileFormat): Option[FileFormat] = fileFormat match {
      case CSVFileFormatWrapper => Some(new CSVFileFormat)
      case JsonFileFormatWrapper => Some(new JsonFileFormat)
      // Add CSVFileFormat and JsonFileFormat in below checks for backward compatibility.
      case _: ParquetFileFormat | _: OrcFileFormat | _: CSVFileFormat | _: JsonFileFormat =>
        Some(fileFormat)
      case other =>
        throw HyperspaceException(s"Unsupported file format found: ${other.toString}.")
    }
  }

  object ExtractSerializableFileFormat {
    def unapply(fileFormat: FileFormat): Option[FileFormat] = fileFormat match {
      case _: CSVFileFormat => Some(CSVFileFormatWrapper)
      case _: JsonFileFormat => Some(JsonFileFormatWrapper)
      case _: ParquetFileFormat | _: OrcFileFormat => Some(fileFormat)
      case other =>
        throw HyperspaceException(s"Unsupported file format found: ${other.toString}.")
    }
  }
}
