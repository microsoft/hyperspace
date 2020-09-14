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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.util.ResolverUtils

class RefreshIncremental(
    spark: SparkSession,
    logManager: IndexLogManager,
    dataManager: IndexDataManager)
    extends RefreshActionBase(spark, logManager, dataManager) {
  final override def op(): Unit = {
    // TODO: The current implementation picks the number of buckets from session config.
    //   This should be user-configurable to allow maintain the existing bucket numbers
    //   in the index log entry.
    write(spark, indexableDf, indexConfig)
  }

  private lazy val indexableDf = {
    // Currently we only support to create an index on a LogicalRelation.
    assert(previousIndexLogEntry.relations.size == 1)
    val relation = previousIndexLogEntry.relations.head

    // TODO: improve this to take last modified time of files into account.
    val indexedFiles = relation.data.properties.content.files.map(_.toString)

    /*
    // Option 1 to create indexable df:
    // To remove pre-indexed files, add file name column, add file filter and remove file name
    // column.
    val predicate = indexedFiles.mkString("('", "','", "')")
    val temporaryColumn = "_file_name"
    return df.withColumn(temporaryColumn, input_file_name)
      .where(s"$temporaryColumn not in $predicate")
      .drop("_file_name")
     */

    // Option 2 to create indexableDf
    val allFiles = df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        location.allFiles().map(_.getPath.toString)
    }.flatten

    val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]

    // Create a df with only diff files from original list of files
    spark.read
      .schema(dataSchema)
      .format(relation.fileFormat)
      .options(relation.options)
      .load(allFiles.diff(indexedFiles): _*)
  }

  /**
   * Create a logEntry with all data files, and index content merged with previous index content.
   * @return Merged index log entry. This contains ALL data files (files which were indexed
   *         previously, and files which are being indexed in this operation). Also contains ALL
   *         index files (index files for previously indexed data as well as newly created files).
   */
  override def logEntry: LogEntry = {
    val entry = getIndexLogEntry(spark, df, indexConfig, indexDataPath)
    val mergedContent = Content(
      previousIndexLogEntry.content.root.merge(entry.content.root)
    )
    entry.copy(content = mergedContent)
  }
}
