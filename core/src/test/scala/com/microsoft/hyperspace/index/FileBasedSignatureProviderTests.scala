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
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitionSpec, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.StructType
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.microsoft.hyperspace.SparkInvolvedSuite

@RunWith(classOf[JUnitRunner])
class FileBasedSignatureProviderTests extends FunSuite with SparkInvolvedSuite {
  private val fileLength = 100
  private val fileModificationTime = 10000
  private val filePath = new Path("filePath")

  private val fileLengthDelta = 10
  private val fileModificationTimeDelta = 10
  private val newFilePath = new Path("newPath")

  test("Logical relations from a same file have the same signature.") {
    val signature1 =
      createFileBasedSignature(Seq(createFileStatus(fileLength, fileModificationTime, filePath)))
    val signature2 =
      createFileBasedSignature(Seq(createFileStatus(fileLength, fileModificationTime, filePath)))
    assert(signature1.equals(signature2))
  }

  test("Logical relations from a file with different length have different signatures.") {
    val signature1 =
      createFileBasedSignature(Seq(createFileStatus(fileLength, fileModificationTime, filePath)))
    val signature2 = createFileBasedSignature(
      Seq(createFileStatus(fileLength + fileLengthDelta, fileModificationTime, filePath)))
    assert(!signature1.equals(signature2))
  }

  test(
    "Logical relations from a file with different modification time have different signatures.") {
    val signature1 =
      createFileBasedSignature(Seq(createFileStatus(fileLength, fileModificationTime, filePath)))
    val signature2 = createFileBasedSignature(
      Seq(
        createFileStatus(fileLength, fileModificationTime + fileModificationTimeDelta, filePath)))
    assert(!signature1.equals(signature2))
  }

  test("Logical relations from a file with different path have different signatures.") {
    val signature1 =
      createFileBasedSignature(Seq(createFileStatus(fileLength, fileModificationTime, filePath)))
    val signature2 = createFileBasedSignature(
      Seq(createFileStatus(fileLength, fileModificationTime, newFilePath)))
    assert(!signature1.equals(signature2))
  }

  test("Logical relations from same files have the same signature.") {
    val signature1 = createFileBasedSignature(
      Seq(
        createFileStatus(fileLength, fileModificationTime, filePath),
        createFileStatus(
          fileLength + fileLengthDelta,
          fileModificationTime + fileModificationTimeDelta,
          newFilePath)))
    val signature2 = createFileBasedSignature(
      Seq(
        createFileStatus(fileLength, fileModificationTime, filePath),
        createFileStatus(
          fileLength + fileLengthDelta,
          fileModificationTime + fileModificationTimeDelta,
          newFilePath)))
    assert(signature1.equals(signature2))
  }

  test("Logical relations from different files have different signatures.") {
    val signature1 = createFileBasedSignature(
      Seq(
        createFileStatus(fileLength, fileModificationTime, filePath),
        createFileStatus(fileLength + fileLengthDelta, fileModificationTime, newFilePath)))
    val signature2 = createFileBasedSignature(
      Seq(
        createFileStatus(fileLength, fileModificationTime, filePath),
        createFileStatus(
          fileLength,
          fileModificationTime + fileModificationTimeDelta,
          newFilePath)))
    assert(!signature1.equals(signature2))
  }

  test("Create FileBasedSignatureProvider.") {
    val fileBasedSignatureProvider = new FileBasedSignatureProvider
    assert(
      LogicalPlanSignatureProvider
        .create(fileBasedSignatureProvider.name)
        .getClass
        .equals(fileBasedSignatureProvider.getClass))
  }

  test("Creating signature provider with invalid name fail.") {
    intercept[IllegalArgumentException](LogicalPlanSignatureProvider.create("randomProvider"))
  }

  private def createFileStatus(length: Long, modificationTime: Long, path: Path): FileStatus = {
    new FileStatus(length, false, 0, 0, modificationTime, path)
  }

  private def createFileBasedSignature(files: Seq[FileStatus]): String = {
    new FileBasedSignatureProvider().signature(createLogicalRelation(files))
  }

  private def createLogicalRelation(fileStatuses: Seq[FileStatus]): LogicalRelation = {
    val fileIndex = new MockPartitioningAwareFileIndex(spark, fileStatuses)
    LogicalRelation(
      HadoopFsRelation(
        fileIndex,
        partitionSchema = StructType(Seq()),
        dataSchema = StructType(Seq()),
        bucketSpec = None,
        new ParquetFileFormat,
        CaseInsensitiveMap(Map.empty))(spark))
  }

  private class MockPartitioningAwareFileIndex(sparkSession: SparkSession, files: Seq[FileStatus])
      extends PartitioningAwareFileIndex(sparkSession, Map.empty, None) {
    override def partitionSpec(): PartitionSpec = PartitionSpec(StructType(Seq()), Seq())
    override protected def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = ???
    override protected def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = ???
    override def rootPaths: Seq[Path] = ???
    override def refresh(): Unit = ???
    override def allFiles: Seq[FileStatus] = files
  }
}
