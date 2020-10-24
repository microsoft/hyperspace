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

package com.microsoft.hyperspace.index.sources.default

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.{Content, Hdfs, Relation}
import com.microsoft.hyperspace.index.sources.SourceProvider
import com.microsoft.hyperspace.util.HashingUtils

object DefaultSourceProvider extends SourceProvider {
  override def createRelation(logicalRelation: LogicalRelation): Option[Relation] = {
    logicalRelation.relation match {
      case HadoopFsRelation(
          location: PartitioningAwareFileIndex,
          _,
          dataSchema,
          _,
          fileFormat,
          options) =>
        val files = location.allFiles
        // Note that source files are currently fingerprinted when the optimized plan is
        // fingerprinted by LogicalPlanFingerprint.
        val sourceDataProperties = Hdfs.Properties(Content.fromLeafFiles(files))
        val fileFormatName = fileFormat match {
          case d: DataSourceRegister => d.shortName
          case other => throw HyperspaceException(s"Unsupported file format: $other")
        }
        // "path" key in options can incur multiple data read unexpectedly.
        val opts = options - "path"
        Some(
          Relation(
            location.rootPaths.map(_.toString),
            Hdfs(sourceDataProperties),
            dataSchema.json,
            fileFormatName,
            opts))
      case _ => None
    }
  }

  override def signature(relation: LogicalRelation): Option[String] = {
    // Currently we are only collecting plan fingerprint from hdfs file based scan nodes.
    relation.relation match {
      case HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _) =>
        val fingerprint = location.allFiles.foldLeft("") { (acc: String, f: FileStatus) =>
          HashingUtils.md5Hex(acc + getFingerprint(f))
        }
        Some(fingerprint)
    }
  }

  /**
   * Get the fingerprint of a file.
   *
   * @param fileStatus file status.
   * @return the fingerprint of a file.
   */
  private def getFingerprint(fileStatus: FileStatus): String = {
    fileStatus.getLen.toString + fileStatus.getModificationTime.toString +
      fileStatus.getPath.toString
  }

  override def reconstructDataFrame(spark: SparkSession, relation: Relation): Option[DataFrame] = {
    val dataSchema = DataType.fromJson(relation.dataSchemaJson).asInstanceOf[StructType]
    val df = spark.read
      .schema(dataSchema)
      .format(relation.fileFormat)
      .options(relation.options)
      .load(relation.rootPaths: _*)
    Some(df)
  }
}
