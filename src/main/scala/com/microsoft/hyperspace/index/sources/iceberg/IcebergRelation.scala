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

package com.microsoft.hyperspace.index.sources.iceberg

import collection.JavaConverters._
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.iceberg.{FileScanTask, Schema, Table, TableScan}
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.index.{Content, FileIdTracker, Hdfs, IndexConstants, Relation}
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.util.PathUtils

/**
 * Implementation for file-based relation used by [[IcebergFileBasedSource]]
 */
class IcebergRelation(
    spark: SparkSession,
    table: Table,
    snapshotId: Option[Long],
    override val plan: LogicalPlan)
    extends FileBasedRelation {

  /**
   * The schema of the underlying table.
   *
   * In Spark 3.0, V2ScanRelationPushDown replaces DataSourceV2Relation with
   * DataSourceV2ScanRelation, with a changed schema only containing projected
   * columns. The colums might not include the partition columns, so we need to
   * use the schema of the table and construct output from it to make it work
   * with partition-aware hybrid scan.
   */
  override def schema: StructType = SparkSchemaUtil.convert(table.schema)

  override def output: Seq[Attribute] = {
    plan.output ++
      table.schema.columns.asScala
        .filterNot(col => plan.output.exists(attr => col.name == attr.name))
        .map(col => AttributeReference(col.name, SparkSchemaUtil.convert(col.`type`))())
  }

  /**
   * Computes the signature of the current relation.
   */
  override def signature: String = {
    snapshotId.getOrElse(table.currentSnapshot().snapshotId()).toString + table.location()
  }

  /**
   * All the files that the current Iceberg table uses for read.
   */
  override lazy val allFiles: Seq[FileStatus] = {
    fileScanTasks.map(toFileStatus)
  }

  /**
   * The optional partition base path of the current relation.
   */
  override def partitionBasePath: Option[String] = {
    if (table.spec().isUnpartitioned) {
      None
    } else {
      Some(PathUtils.makeAbsolute(table.location(), spark.sessionState.newHadoopConf()).toString)
    }
  }

  /**
   * Creates [[Relation]] for IndexLogEntry using the current relation.
   *
   * @param fileIdTracker [[FileIdTracker]] to use when populating the data of [[Relation]].
   * @return [[Relation]] object that describes the current relation.
   */
  override def createRelationMetadata(fileIdTracker: FileIdTracker): Relation = {
    val files = allFiles

    val sourceDataProperties =
      Hdfs.Properties(Content.fromLeafFiles(files, fileIdTracker).get)
    val fileFormatName = "iceberg"
    val currentSnapshot = table.currentSnapshot()
    val basePathOpt =
      partitionBasePath.map(p => Map("basePath" -> p)).getOrElse(Map.empty)
    val opts = Map(
      "snapshot-id" -> currentSnapshot.snapshotId().toString,
      "as-of-timestamp" -> currentSnapshot.timestampMillis().toString) ++
      basePathOpt

    Relation(
      Seq(
        PathUtils
          .makeAbsolute(table.location(), spark.sessionState.newHadoopConf())
          .toString),
      Hdfs(sourceDataProperties),
      SparkSchemaUtil.convert(table.schema),
      fileFormatName,
      opts)
  }

  /**
   * Returns whether the current relation has parquet source files or not.
   *
   * @return Always true since Iceberg table files are stored as Parquet.
   */
  override def hasParquetAsSourceFormat: Boolean = true

  /**
   * For [[IcebergRelation]], each file path should be in this format:
   *   `/path/to/file` or `X:/path/to/file` for Windows file system.
   */
  override def pathNormalizer: String => String = {
    // For Windows,
    //   original file path: file:/C:/path/to/file
    //   input_file_name(): C:/path/to/file
    // For Linux,
    //   original file path: file:///path/to/file or file:/path/to/file
    //   input_file_name(): /path/to/file
    if (Path.WINDOWS) {
      _.stripPrefix("file:/")
    } else {
      _.replaceFirst("^file:/{1,3}", "/")
    }
  }

  /**
   * Options of the current relation.
   *
   * In Spark 3, DataSourceV2Relation can be converted into DataSourceV2ScanRelation
   * and we cannot access the options. This should be okay, as the only values we were
   * using were "path" and "snapshot-id", which can be retrieved with other means.
   */
  override def options: Map[String, String] = Map[String, String]()

  /**
   * The partition schema of the current relation.
   */
  override def partitionSchema: StructType = {
    val fields = table.spec().fields().asScala.map { p =>
      table.schema().findField(p.name())
    }
    val schema = new Schema(fields.asJava)
    SparkSchemaUtil.convert(schema)
  }

  override def getOrCreateFileIndex(spark: SparkSession): InMemoryFileIndex = {
    val rootPaths = if (snapshotId.isEmpty) {
      if (table.spec().isUnpartitioned) {
        Seq(PathUtils.makeAbsolute(table.location(), spark.sessionState.newHadoopConf()))
      } else {
        // We'd like to use a single root path here, but
        // then the partition discovery fails
        // because there is a directory called "metadata"
        // under the root path.
        fileScanTasks.map(t => toPath(t).getParent).distinct
      }
    } else {
      // Listing all files with InMemoryFileIndex should be avoided,
      // but there is no other way as there is no FileIndex implementation
      // for Iceberg that supports snapshots.
      fileScanTasks.map(toPath)
    }
    new InMemoryFileIndex(
      spark,
      rootPaths,
      partitionBasePath
        .map(PartitioningAwareFileIndex.BASE_PATH_PARAM -> _)
        .toMap,
      Some(schema),
      FileStatusCache.getOrCreate(spark))
  }

  /**
   * Creates [[HadoopFsRelation]] based on the current relation.
   *
   * This is mainly used in conjunction with [[createLogicalRelation]].
   */
  override def createHadoopFsRelation(
      location: FileIndex,
      dataSchema: StructType,
      options: Map[String, String]): HadoopFsRelation = {
    HadoopFsRelation(
      location,
      partitionSchema,
      dataSchema,
      None,
      new ParquetFileFormat,
      options + IndexConstants.INDEX_RELATION_IDENTIFIER)(spark)
  }

  /**
   * Creates [[LogicalRelation]] based on the current relation.
   *
   * This is mainly used to read the index files.
   */
  override def createLogicalRelation(
      hadoopFsRelation: HadoopFsRelation,
      newOutput: Seq[AttributeReference]): LogicalRelation = {
    val updatedOutput =
      newOutput.filter(attr => hadoopFsRelation.schema.fieldNames.contains(attr.name))
    new LogicalRelation(hadoopFsRelation, updatedOutput, None, false)
  }

  private def newScan(): TableScan = {
    if (snapshotId.isDefined) {
      table.newScan().useSnapshot(snapshotId.get)
    } else {
      table.newScan()
    }
  }

  private def fileScanTasks: Seq[FileScanTask] = {
    newScan().planFiles().iterator().asScala.toSeq
  }

  private def toPath(fileScanTask: FileScanTask): Path = {
    PathUtils.makeAbsolute(
      new Path(fileScanTask.file().path().toString),
      spark.sessionState.newHadoopConf())
  }

  private def toFileStatus(fileScanTask: FileScanTask): FileStatus = {
    val path = toPath(fileScanTask)
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    val fullPath = if (!path.isAbsolute) {
      new Path(s"${fs.getWorkingDirectory.toString}/${path.toString}")
    } else {
      path
    }
    val modTime = fs.listStatus(fullPath).head.getModificationTime
    toFileStatus(fileScanTask.file().fileSizeInBytes(), modTime, fullPath)
  }

  private def toFileStatus(fileSize: Long, modificationTime: Long, path: Path): FileStatus = {
    new FileStatus(
      /* length */ fileSize,
      /* isDir */ false,
      /* blockReplication */ 0,
      /* blockSize */ 1,
      /* modificationTime */ modificationTime,
      path)
  }
}
