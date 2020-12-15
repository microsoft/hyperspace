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

package com.microsoft.hyperspace.actions

import scala.util.{Failure, Success, Try}

import org.apache.commons.io.FilenameUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.index._

class RefreshScan(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager,
    scanPattern: String)
    extends RefreshIncrementalAction(spark, logManager, dataManager) {

  /** df representing the complete set of data files which will be indexed once this refresh action
   * finishes. */
  override protected lazy val df = {
    val relation = previousIndexLogEntry.relations.head
    val previouslyIndexedData = relation.data.properties.content
    val newlyIndexedData = previouslyIndexedData.fileInfos -- deletedFiles ++ appendedFiles
    val newlyIndexedDataFiles: Seq[String] = newlyIndexedData.map(_.name).toSeq

    val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]
    spark.read
      .schema(dataSchema)
      .format(relation.fileFormat)
      .options(relation.options)
      .load(newlyIndexedDataFiles: _*)
  }

  def isMatch(path: String, scanPattern: String): Boolean = {
    val scanSplits = scanPattern.split(Path.SEPARATOR)
    scanSplits.nonEmpty && path.split(Path.SEPARATOR).contains(scanSplits.head)
  }

  def resolve(path: String, scanPattern: String): String = {
    val scanSplits: Array[String] = scanPattern.split(Path.SEPARATOR)
    val pathSplits: Array[String] = path.split(Path.SEPARATOR)
    val splitPoint: Int = pathSplits.lastIndexOf(scanSplits.head)
    var (prefix, suffix) = pathSplits.splitAt(splitPoint)

    for (j <- 0 until math.max(scanSplits.length, suffix.length)) {
      val resolvedPart = (Try(suffix(j)), Try(scanSplits(j))) match {
        case (Success(path), Success(scan)) if FilenameUtils.wildcardMatch(path, scan) => path
        case (Success(path), Success(scan)) if FilenameUtils.wildcardMatch(scan, path) => scan
        case (Success(_), Success(_)) => throw new Exception("Incompatible scan pattern")
        case (Success(path), Failure(_)) => path
        case (Failure(_), Success(scan)) => scan
        case _ => throw new Exception("Unexpected Exception")
      }

      prefix :+= resolvedPart
    }
    prefix.mkString(Path.SEPARATOR)
  }

  /** paths resolved with scan pattern.
   * Paths merged With scan pattern to choose more selective option
   * e.g. if rootPath
   */
  private lazy val resolvedPaths = {
    val relation = previousIndexLogEntry.relations.head
    relation.rootPaths.collect {
      case path if isMatch(path, scanPattern) => resolve(path, scanPattern)
    }

    // Remove this after testing
    // Seq("glob2/y=2023")
  }

  override def logEntry: LogEntry = {
    // TODO: Deduplicate from super.logEntry()
    val entry = getIndexLogEntry(spark, df, indexConfig, indexDataPath)

    // If there is no deleted files, there are index data files only for appended data in this
    // version and we need to add the index data files of previous index version.
    // Otherwise, as previous index data is rewritten in this version while excluding
    // indexed rows from deleted files, all necessary index data files exist in this version.
    val updatedEntry = if (deletedFiles.isEmpty) {
      // Merge new index files with old index files.
      val mergedContent = Content(previousIndexLogEntry.content.root.merge(entry.content.root))
      entry.copy(content = mergedContent)
    } else {
      // New entry.
      entry
    }

    val relation = entry.source.plan.properties.relations.head
    val updatedRelation =
      relation.copy(rootPaths = previousIndexLogEntry.relations.head.rootPaths)
    updatedEntry.copy(
      source = updatedEntry.source.copy(plan = updatedEntry.source.plan.copy(
        properties = updatedEntry.source.plan.properties.copy(relations = Seq(updatedRelation)))))
  }

  /** Deleted files which match resolved paths */
  override protected lazy val deletedFiles: Seq[FileInfo] = {
    // Helper function to check if a file belongs to one of the resolved paths.
    def fromResolvedPaths(file: FileInfo): Boolean = {
      resolvedPaths.exists(p => FilenameUtils.wildcardMatch(file.name, p))
    }

    val originalFiles = previousIndexLogEntry.relations.head.data.properties.content.fileInfos
      .filter(fromResolvedPaths)

    (originalFiles -- currentFiles).toSeq
  }

  override protected lazy val currentFiles: Set[FileInfo] = {
    val relation = previousIndexLogEntry.relations.head
    val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]
    val changedDf = spark.read
      .schema(dataSchema)
      .format(relation.fileFormat)
      .options(relation.options)
      .load(resolvedPaths: _*)
    changedDf.queryExecution.optimizedPlan
      .collect {
        case LogicalRelation(
            HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
            _,
            _,
            _) =>
          location
            .allFiles()
            .map { f =>
              // For each file, if it already has a file id, add that id to its corresponding
              // FileInfo. Note that if content of an existing file is changed, it is treated
              // as a new file (i.e. its current file id is no longer valid).
              val id = fileIdTracker.addFile(f)
              FileInfo(f, id, asFullPath = true)
            }
      }
      .flatten
      .toSet
  }
}
