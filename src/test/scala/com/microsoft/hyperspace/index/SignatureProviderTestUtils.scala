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

import scala.collection.mutable

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex, PartitionSpec}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.StructType

object SignatureProviderTestUtils {
  def createFileStatus(length: Long, modificationTime: Long, path: Path): FileStatus =
    new FileStatus(length, false, 0, 0, modificationTime, path)

  def createLogicalRelation(
      spark: SparkSession,
      fileStatuses: Seq[FileStatus],
      dataSchema: StructType = StructType(Seq())): LogicalRelation = {
    val fileIndex = new MockPartitioningAwareFileIndex(spark, fileStatuses)
    LogicalRelation(
      HadoopFsRelation(
        fileIndex,
        partitionSchema = StructType(Seq()),
        dataSchema,
        bucketSpec = None,
        new ParquetFileFormat,
        CaseInsensitiveMap(Map.empty))(spark))
  }

  private class MockPartitioningAwareFileIndex(sparkSession: SparkSession, files: Seq[FileStatus])
      extends PartitioningAwareFileIndex(sparkSession, Map.empty, None) {

    override def partitionSpec(): PartitionSpec = PartitionSpec(StructType(Seq()), Seq())

    override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] =
      throw new NotImplementedError

    override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] =
      throw new NotImplementedError

    override def rootPaths: Seq[Path] = throw new NotImplementedError

    override def refresh(): Unit = throw new NotImplementedError

    override def allFiles: Seq[FileStatus] = files
  }
}
