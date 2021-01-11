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
import org.apache.spark.SparkFunSuite

import com.microsoft.hyperspace.{HyperspaceException, SparkInvolvedSuite}

class FileBasedSignatureProviderTest extends SparkFunSuite with SparkInvolvedSuite {
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

  private def createFileStatus(length: Long, modificationTime: Long, path: Path): FileStatus =
    SignatureProviderTestUtils.createFileStatus(length, modificationTime, path)

  private def createFileBasedSignature(files: Seq[FileStatus]): String =
    new FileBasedSignatureProvider()
      .signature(SignatureProviderTestUtils.createLogicalRelation(spark, files)) match {
      case Some(s) => s
      case None => throw HyperspaceException("Invalid plan for signature generation.")
    }
}
