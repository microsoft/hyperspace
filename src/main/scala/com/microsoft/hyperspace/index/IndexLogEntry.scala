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
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.Content.Directory.FileInfo

// IndexLogEntry-specific fingerprint to be temporarily used where fingerprint is not defined.
case class NoOpFingerprint() {
  val kind: String = "NoOp"
  val properties: Map[String, String] = Map()
}

// IndexLogEntry-specific Content that uses IndexLogEntry-specific fingerprint.
case class Content(root: String, directories: Seq[Content.Directory])
object Content {
  case class Directory(path: String, files: Seq[FileInfo], fingerprint: NoOpFingerprint)
  object Directory {
    // modifiedTime is an Epoch time in milliseconds. (ms since 1970-01-01T00:00:00.000 UTC).
    case class FileInfo(name: String, size: Long, modifiedTime: Long)
    object FileInfo {
      def apply(s: FileStatus): FileInfo =
        FileInfo(s.getPath.toString, s.getLen, s.getModificationTime)
    }
  }
}

// IndexLogEntry-specific CoveringIndex that represents derived dataset.
case class CoveringIndex(properties: CoveringIndex.Properties) {
  val kind = "CoveringIndex"
}
object CoveringIndex {
  case class Properties(columns: Properties.Columns, schemaString: String, numBuckets: Int)
  object Properties {
    case class Columns(indexed: Seq[String], included: Seq[String])
  }
}

// IndexLogEntry-specific Signature that stores the signature provider and value.
case class Signature(provider: String, value: String)

// IndexLogEntry-specific LogicalPlanFingerprint to store fingerprint of logical plan.
case class LogicalPlanFingerprint(properties: LogicalPlanFingerprint.Properties) {
  val kind = "LogicalPlan"
}
object LogicalPlanFingerprint {
  case class Properties(signatures: Seq[Signature])
}

// IndexLogEntry-specific Hdfs that represents the source data.
case class Hdfs(properties: Hdfs.Properties) {
  val kind = "HDFS"
}
object Hdfs {
  case class Properties(content: Content)
}

// IndexLogEntry-specific Relation that represents the source relation.
case class Relation(
    rootPaths: Seq[String],
    data: Hdfs,
    dataSchemaJson: String,
    fileFormat: String,
    options: Map[String, String])

// IndexLogEntry-specific SparkPlan that represents the source plan.
case class SparkPlan(properties: SparkPlan.Properties) {
  val kind = "Spark"
}

object SparkPlan {
  case class Properties(
      relations: Seq[Relation],
      rawPlan: String, // null for now
      sql: String, // null for now
      fingerprint: LogicalPlanFingerprint)
}

// IndexLogEntry-specific Source that uses SparkPlan as a plan.
case class Source(plan: SparkPlan)

// IndexLogEntry that captures index-related information.
case class IndexLogEntry(
    name: String,
    derivedDataset: CoveringIndex,
    content: Content,
    source: Source,
    extra: Map[String, String])
    extends LogEntry(IndexLogEntry.VERSION) {

  def schema: StructType =
    DataType.fromJson(derivedDataset.properties.schemaString).asInstanceOf[StructType]

  def created: Boolean = state.equals(Constants.States.ACTIVE)

  def indexedColumns: Seq[String] = derivedDataset.properties.columns.indexed

  def includedColumns: Seq[String] = derivedDataset.properties.columns.included

  def numBuckets: Int = derivedDataset.properties.numBuckets

  def relations: Seq[Relation] = source.plan.properties.relations

  def allSourceFiles: Set[FileInfo] = {
    relations
      .flatMap(_.data.properties.content.directories.flatMap(_.files))
      .toSet
  }

  def bucketSpec: BucketSpec =
    BucketSpec(
      numBuckets = numBuckets,
      bucketColumnNames = indexedColumns,
      sortColumnNames = indexedColumns)

  def config: IndexConfig = IndexConfig(name, indexedColumns, includedColumns)

  def signature: Signature = {
    val sourcePlanSignatures = source.plan.properties.fingerprint.properties.signatures
    assert(sourcePlanSignatures.length == 1)
    sourcePlanSignatures.head
  }

  override def equals(o: Any): Boolean = o match {
    case that: IndexLogEntry =>
      config.equals(that.config) &&
        signature.equals(that.signature) &&
        numBuckets.equals(that.numBuckets) &&
        content.root.equals(that.content.root) &&
        source.equals(that.source) &&
        state.equals(that.state)
    case _ => false
  }

  override def hashCode(): Int = {
    config.hashCode + signature.hashCode + numBuckets.hashCode + content.hashCode
  }
}

object IndexLogEntry {
  val VERSION: String = "0.1"

  def schemaString(schema: StructType): String = schema.json
}
