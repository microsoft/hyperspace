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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants.States.{ACTIVE, REFRESHING}
import com.microsoft.hyperspace.index._

/**
 * Base abstract class containing common code for different types of index refresh actions.
 *
 * @param spark SparkSession
 * @param logManager Index LogManager for index being refreshed
 * @param dataManager Index DataManager for index being refreshed
 */
// TODO: This class depends directly on LogEntry. This should be updated such that
//   it works with IndexLogEntry only. (for example, this class can take in
//   derivedDataset specific logic for refreshing).
private[actions] abstract class RefreshActionBase(
    spark: SparkSession,
    final override protected val logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends CreateActionBase(dataManager)
    with Action {
  private lazy val previousLogEntry: LogEntry = {
    logManager.getLog(baseId).getOrElse {
      throw HyperspaceException("LogEntry must exist for refresh operation")
    }
  }

  protected lazy val previousIndexLogEntry = previousLogEntry.asInstanceOf[IndexLogEntry]

  // Refresh maintains the same number of buckets as the existing index to be consistent
  // throughout all index versions. For "full" refresh mode, we could allow to change configs
  // like num buckets or lineage column as it is newly building the index data. This might
  // be done with a different refresh mode if necessary.
  override protected final def numBucketsForIndex(spark: SparkSession): Int = {
    previousIndexLogEntry.numBuckets
  }

  // Refresh maintains the same lineage column config as the existing index.
  // See above getNumBucketsConfig for more detail.
  override protected final def indexLineageEnabled(spark: SparkSession): Boolean = {
    previousIndexLogEntry.hasLineageColumn(spark)
  }

  // Reconstruct a df from schema
  protected lazy val df = {
    val rels = previousIndexLogEntry.relations
    val dataSchema = DataType.fromJson(rels.head.dataSchemaJson).asInstanceOf[StructType]
    spark.read
      .schema(dataSchema)
      .format(rels.head.fileFormat)
      .options(rels.head.options)
      .load(rels.head.rootPaths: _*)
  }

  protected lazy val indexConfig: IndexConfig = {
    val ddColumns = previousIndexLogEntry.derivedDataset.properties.columns
    IndexConfig(previousIndexLogEntry.name, ddColumns.indexed, ddColumns.included)
  }

  override def logEntry: LogEntry = getIndexLogEntry(spark, df, indexConfig, indexDataPath)

  final override val transientState: String = REFRESHING

  final override val finalState: String = ACTIVE

  override def validate(): Unit = {
    if (!previousIndexLogEntry.state.equalsIgnoreCase(ACTIVE)) {
      throw HyperspaceException(
        s"Refresh is only supported in $ACTIVE state. " +
          s"Current index state is ${previousIndexLogEntry.state}")
    }
  }

  /**
   * Compare list of source data files from previous IndexLogEntry to list
   * of current source data files, validate fileInfo for existing files and
   * identify deleted source data files.
   * Finally, append the previously known deleted files to the result. These
   * are the files for which the index was never updated in the past.
   */
  protected lazy val deletedFiles: Seq[FileInfo] = {
    val relation = previousIndexLogEntry.relations.head
    val originalFiles = relation.data.properties.content.fileInfos

    (originalFiles -- currentFiles).toSeq
  }

  /**
   * Retrieve the source file list from reconstructed "df" for refresh.
   * Build Set[FileInfo] to compare the source file list with the previous index version.
   */
  protected lazy val currentFiles: Set[FileInfo] = {
    df.queryExecution.optimizedPlan
      .collect {
        case LogicalRelation(
            HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
            _,
            _,
            _) =>
          location
            .allFiles()
            .map(f => FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))
      }
      .flatten
      .toSet
  }

  /**
   * Compare list of source data files from previous IndexLogEntry to list
   * of current source data files, validate fileInfo for existing files and
   * identify newly appended source data files.
   * Finally, append the previously known appended files to the result. These
   * are the files for which index was never updated in the past.
   */
  protected lazy val appendedFiles: Seq[FileInfo] = {
    val relation = previousIndexLogEntry.relations.head
    val originalFiles = relation.data.properties.content.fileInfos

    (currentFiles -- originalFiles).toSeq
  }
}
