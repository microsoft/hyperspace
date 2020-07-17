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

package com.microsoft.hyperspace.index

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.sql.{SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTable}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, ExprId, Predicate, Unevaluable}
import org.apache.spark.sql.catalyst.plans.logical.{BinaryNode, LeafNode, LogicalPlan}
import org.apache.spark.sql.execution.FileRelation
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, OutputWriterFactory, PartitionDirectory}
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.types.{DataType, StructType}

package object serde {

  /**
   * Wrappers for logical plan nodes that cannot be serialized directly. There are two cases:
   *
   * 1. Some nodes, such as the three subquery expressions (i.e., ScalarSubquery, ListQuery,
   * Exists) listed below, have parent classes that do not have a default constructor without
   * parameters, which does not conform to Java's serialization protocol.
   *
   * 2. Other nodes, such as the InMemoryFileIndex and HadoopFsRelation below, contain states
   * that are tied to the current Spark runtime (e.g., Spark session, FileSystem configuration),
   * which cannot be retained via serialization and have to be reconstructed in deserialization.
   *
   * The Wrappers simply keep the fields of the corresponding objects that can be serialized and
   * get rid of those that cannot be. As a result, the Wrappers themselves can be serialized and
   * deserialized successfully just using Java's serialization protocol.
   *
   * TODO: While the current list of wrappers already cover all queries in the TPC-H and
   *   TPC-DS benchmarks, it is definitely possible that users may want to ser/de certain
   *   LogicalPlan nodes that have not been covered here (e.g., LogicalRdd, or Leaf nodes that
   *   implement the DataSourceV2 API. This list therefore needs to be expanded when necessary.
   */
  trait LogicalPlanWrapper

  trait LogicalPlanWrapperWithNoDefaultConstructor extends LogicalPlanWrapper

  trait LogicalPlanWrapperWithInMemoryStates extends LogicalPlanWrapper

  /**
   * A base class for all subquery expression wrappers.
   *
   * Since all subquery expression wrappers need to inherit from Expression, they need to
   * implement the two functions eval() and doGenCode(). Extending Unevaluable automatically
   * provides desired implementations for these two functions.
   */
  abstract class SubqueryExpressionWrapper
      extends Unevaluable
      with LogicalPlanWrapperWithNoDefaultConstructor {
    override def nullable: Boolean = throw new UnsupportedOperationException()
    override def dataType: DataType = throw new UnsupportedOperationException()
  }

  case class ScalarSubqueryWrapper(plan: LogicalPlan, children: Seq[Expression], exprId: ExprId)
      extends SubqueryExpressionWrapper {
    // Implementation picked from [[ScalarSubquery]] class. toString fails without this.
    override def dataType: DataType = plan.schema.fields.head.dataType
    override def toString: String = s"scalar-subquery-wrapper#${exprId.id}"
  }

  case class ListQueryWrapper(
      plan: LogicalPlan,
      children: Seq[Expression],
      exprId: ExprId,
      childOutputs: Seq[Attribute])
      extends SubqueryExpressionWrapper {
    override def toString: String = s"list-wrapper#${exprId.id}"
  }

  case class InSubqueryWrapper(values: Seq[Expression], query: ListQueryWrapper)
      extends Predicate
      with Unevaluable {
    // Implementation picked from [[InSubquery]] class. Serialization fails without this.
    override def children: Seq[Expression] = values :+ query
    override def nullable: Boolean =
      throw new UnsupportedOperationException(s"Should not be invoked for: $this")
  }

  case class ExistsWrapper(plan: LogicalPlan, children: Seq[Expression], exprId: ExprId)
      extends SubqueryExpressionWrapper {
    override def toString: String = s"exists-wrapper#${exprId.id}"
  }

  case class ScalaUDFWrapper(
      function: AnyRef,
      dataType: DataType,
      children: Seq[Expression],
      inputsNullSafe: Seq[Boolean],
      inputTypes: Seq[DataType],
      udfName: Option[String],
      nullable: Boolean,
      udfDeterministic: Boolean)
      extends Expression
      with Unevaluable
      with LogicalPlanWrapperWithNoDefaultConstructor

  case class IntersectWrapper(left: LogicalPlan, right: LogicalPlan, isAll: Boolean)
      extends BinaryNode
      with LogicalPlanWrapperWithNoDefaultConstructor {
    // This implementation is copied from Intersect.
    override def output: Seq[Attribute] =
      left.output.zip(right.output).map {
        case (leftAttr, rightAttr) =>
          leftAttr.withNullability(leftAttr.nullable && rightAttr.nullable)
      }
  }

  case class ExceptWrapper(left: LogicalPlan, right: LogicalPlan, isAll: Boolean)
      extends BinaryNode
      with LogicalPlanWrapperWithNoDefaultConstructor {
    // This implementation is copied from Except.
    override def output: Seq[Attribute] = left.output
  }

  case class InMemoryFileIndexWrapper(rootPathStrings: Seq[String])
      extends FileIndex
      with LogicalPlanWrapperWithInMemoryStates {
    override def listFiles(
        partitionFilters: Seq[Expression],
        dataFilters: Seq[Expression]): Seq[PartitionDirectory] =
      throw new UnsupportedOperationException()
    override def inputFiles: Array[String] = throw new UnsupportedOperationException()
    override def refresh(): Unit = throw new UnsupportedOperationException()
    override def sizeInBytes: Long = throw new UnsupportedOperationException()
    override def partitionSchema: StructType = throw new UnsupportedOperationException()
    override def rootPaths: Seq[Path] = throw new UnsupportedOperationException()
  }

  case class HadoopFsRelationWrapper(
      location: FileIndex,
      partitionSchema: StructType,
      dataSchema: StructType,
      bucketSpec: Option[BucketSpec],
      fileFormat: FileFormat,
      options: Map[String, String])
      extends BaseRelation
      with FileRelation
      with LogicalPlanWrapperWithInMemoryStates {
    override def sqlContext: SQLContext = throw new UnsupportedOperationException()
    override def schema: StructType = throw new UnsupportedOperationException()
    override def inputFiles: Array[String] = throw new UnsupportedOperationException()
  }

  case class LogicalRelationWrapper(
      relation: BaseRelation,
      output: Seq[AttributeReference],
      catalogTable: Option[CatalogTable],
      override val isStreaming: Boolean)
      extends LeafNode
      with LogicalPlanWrapperWithInMemoryStates

  abstract class FileFormatWrapper extends FileFormat {
    override def inferSchema(
      sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus]): Option[StructType] = throw new UnsupportedOperationException()

    override def prepareWrite(
      sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType): OutputWriterFactory = throw new UnsupportedOperationException()
  }

  case object CSVFileFormatWrapper extends FileFormatWrapper

  case object JsonFileFormatWrapper extends FileFormatWrapper
}
