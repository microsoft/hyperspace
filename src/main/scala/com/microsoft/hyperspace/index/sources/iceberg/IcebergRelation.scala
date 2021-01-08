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
import java.io.IOException
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.iceberg.{CombinedScanTask, FileScanTask, Table, TableProperties, TableScan}
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.expressions.Expressions
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.hive.HiveCatalogs
import org.apache.iceberg.spark.{SparkFilters, SparkSchemaUtil}
import org.apache.iceberg.spark.source.IcebergSource
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.sources.v2.DataSourceOptions
import org.apache.spark.sql.sources.v2.reader.SupportsPushDownFilters
import org.apache.spark.sql.types.StructType
import scala.util.Try

import com.microsoft.hyperspace.index.{Content, FileIdTracker, Hdfs, IndexConstants, IndexLogEntry, Relation}
import com.microsoft.hyperspace.index.plans.logical.IndexHadoopFsRelation
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.util.PathUtils

/**
 * Implementation for file-based relation used by [[IcebergFileBasedSource]]
 */
class IcebergRelation(spark: SparkSession, override val plan: DataSourceV2Relation)
  extends FileBasedRelation {

  /**
   * Computes the signature of the current relation.
   */
  override def signature: String = plan.source match {
    case _: IcebergSource =>
      val table = findTable(new DataSourceOptions(plan.options.asJava))
      val snapshotId = plan.options.getOrElse("snapshot-id",
        table.currentSnapshot().snapshotId())
      snapshotId + table.location()
  }

  /**
   * All the files that the current relation references to.
   */
  override def allFiles: Seq[FileStatus] = plan.source match {
    case source: IcebergSource =>
      val dsOpts = new DataSourceOptions(plan.options.asJava)
      val table = findTable(dsOpts)
      val reader = source.createReader(dsOpts)
      val filters = reader.asInstanceOf[SupportsPushDownFilters].pushedFilters()
      tasks(table, dsOpts, filters, reader.readSchema()).flatMap { t =>
        t.files().asScala.map(toFileStatus)
      }
  }

  /**
   * The optional partition base path of the current relation.
   */
  override def partitionBasePath: Option[String] = plan.source match {
    case _: IcebergSource =>
      val table = Try(findTable(new DataSourceOptions(plan.options.asJava))).toOption
      table match {
        case Some(t) =>
          if (t.spec().isUnpartitioned) {
            None
          } else {
            Some(
              PathUtils.makeAbsolute(t.location(), spark.sessionState.newHadoopConf()).toString)
          }
        case _ => None
      }
    case _ => None
  }

  /**
   * Creates [[Relation]] for IndexLogEntry using the current relation.
   *
   * @param fileIdTracker [[FileIdTracker]] to use when populating the data of [[Relation]].
   * @return [[Relation]] object that describes the current relation.
   */
  override def createRelationMetadata(fileIdTracker: FileIdTracker): Relation = {
    plan.source match {
      case source: IcebergSource =>
        val dsOpts = new DataSourceOptions(plan.options.asJava)
        val table = findTable(dsOpts)
        val reader = source.createReader(dsOpts)
        val files = allFiles

        val sourceDataProperties =
          Hdfs.Properties(Content.fromLeafFiles(files, fileIdTracker).get)
        val fileFormatName = "iceberg"
        val currentSnapshot = table.currentSnapshot()
        val basePathOpt = partitionBasePath.map(e => Map("basePath" -> e)).getOrElse(Map.empty)
        val opts = plan.options - "path" +
            ("snapshot-id" -> currentSnapshot.snapshotId().toString) +
            ("as-of-timestamp" -> currentSnapshot.timestampMillis().toString) ++
            basePathOpt

        Relation(
          Seq(PathUtils.makeAbsolute(table.location(),
            spark.sessionState.newHadoopConf()).toString),
          Hdfs(sourceDataProperties),
          reader.readSchema().json,
          fileFormatName,
          opts)
    }
  }

  /**
   * Returns whether the current relation has parquet source files or not.
   *
   * @return Always true since delta lake files are stored as Parquet.
   */
  override def hasParquetAsSourceFormat: Boolean = true

  /**
   * Returns list of pairs of (file path, file id) to build lineage column.
   *
   * File paths should be the same format as "input_file_name()" of the given relation type.
   * For [[IcebergRelation]], each file path should be in this format:
   *   `file:/path/to/file`
   *
   * @param fileIdTracker [[FileIdTracker]] to create the list of (file path, file id).
   * @return List of pairs of (file path, file id).
   */
  override def lineagePairs(fileIdTracker: FileIdTracker): Seq[(String, Long)] = {
    fileIdTracker.getFileToIdMap.toSeq.map { kv =>
      (kv._1._1, kv._2)
    }
  }

  /**
   * Options of the current relation.
   */
  override def options: Map[String, String] = plan.options

  /**
   * The partition schema of the current relation.
   */
  override def partitionSchema: StructType = plan.source match {
    case _: IcebergSource =>
      val dsOpts = new DataSourceOptions(plan.options.asJava)
      val table = findTable(dsOpts)
      SparkSchemaUtil.convert(table.spec().schema())

  }

  /**
   * Creates [[HadoopFsRelation]] based on the current relation.
   *
   * This is mainly used in conjunction with [[createLogicalRelation]].
   */
  override def createHadoopFsRelation(location: FileIndex,
      dataSchema: StructType,
      options: Map[String, String]): HadoopFsRelation = plan.source match {
    case _: IcebergSource =>
      HadoopFsRelation(
        location,
        new StructType(),
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

  private def findTable(options: DataSourceOptions): Table = {
    val conf = spark.sessionState.newHadoopConf()
    val path = options.get("path")
    if (!path.isPresent) {
      throw new IllegalArgumentException("Cannot open table: path is not set")
    }
    if (path.get.contains("/")) {
      val tables = new HadoopTables(conf)
      tables.load(path.get)
    }
    else {
      val hiveCatalog = HiveCatalogs.loadCatalog(conf)
      val tableIdentifier = TableIdentifier.parse(path.get)
      hiveCatalog.loadTable(tableIdentifier)
    }
  }

  // TODO: need Iceberg's `tasks()` method to be public to get rid of this method
  private def tasks(table: Table, options: DataSourceOptions, pushedFilters: Seq[Filter],
      requestedSchema: StructType): Seq[CombinedScanTask] = {

    val caseSensitive = spark.conf.get("spark.sql.caseSensitive").toBoolean
    val snapshotId = Try(options.get("snapshot-id").get().toLong).toOption
    val asOfTimestamp = Try(options.get("as-of-timestamp").get().toLong).toOption
    val startSnapshotId = Try(options.get("start-snapshot-id").get().toLong).toOption
    val endSnapshotId = Try(options.get("end-snapshot-id").get().toLong).toOption
    val splitSize = Try(options.get("split-size").get().toLong).toOption
    val splitLookBack = Try(options.get("lookback").get().toInt).toOption
    val splitOpenFileCost = Try(options.get("file-open-cost").get().toLong).toOption
    val pushedExpressions = pushedFilters.map(SparkFilters.convert)
    val lazySchema = if (requestedSchema != null) {
      SparkSchemaUtil.prune(
        table.schema,
        requestedSchema,
        pushedExpressions.reduceOption(Expressions.and).getOrElse(Expressions.alwaysTrue()),
        caseSensitive)
    } else {
      table.schema
    }

    var scan: TableScan = table.newScan.caseSensitive(caseSensitive).project(lazySchema)

    if (snapshotId.isDefined) {
      scan = scan.useSnapshot(snapshotId.get)
    }
    if (asOfTimestamp.isDefined) {
      scan = scan.asOfTime(asOfTimestamp.get)
    }
    if (startSnapshotId.isDefined) {
      if (endSnapshotId.isDefined) {
        scan = scan.appendsBetween(startSnapshotId.get, endSnapshotId.get)
      } else {
        scan = scan.appendsAfter(startSnapshotId.get)
      }
    }
    if (splitSize.isDefined) {
      scan = scan.option(TableProperties.SPLIT_SIZE, splitSize.get.toString)
    }
    if (splitLookBack.isDefined) {
      scan = scan.option(TableProperties.SPLIT_LOOKBACK, splitLookBack.get.toString)
    }
    if (splitOpenFileCost.isDefined) {
      scan = scan.option(TableProperties.SPLIT_OPEN_FILE_COST, splitOpenFileCost.get.toString)
    }

    pushedExpressions.foreach { f =>
      scan = scan.filter(f)
    }

    try {
      scan.planTasks().asScala.toSeq
    } catch {
      case e: IOException =>
        throw new RuntimeException(s"Failed to close table scan: $scan", e)
    }
  }

  private def toFileStatus(fileScanTask: FileScanTask): FileStatus = {
    val jPath = new Path(fileScanTask.file().path().toString)
    val fs = jPath.getFileSystem(spark.sessionState.newHadoopConf())
    val fullPath = if (!jPath.isAbsolute) {
      new Path(s"${fs.getWorkingDirectory.toString}/${jPath.toString}")
    } else {
      jPath
    }
    val modTime = fs.listStatus(fullPath).head.getModificationTime
    toFileStatus(
      fileScanTask.file().fileSizeInBytes(),
      modTime,
      fullPath)
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
