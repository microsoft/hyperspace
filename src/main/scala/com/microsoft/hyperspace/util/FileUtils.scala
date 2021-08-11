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

package com.microsoft.hyperspace.util

import java.io.OutputStreamWriter
import java.nio.charset.StandardCharsets

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

/**
 * File related utility functions.
 */
object FileUtils {

  /**
   * Create a file with input content.
   *
   * @param fileSystem file system.
   * @param filePath the file to be created or overwritten.
   * @param contents the contents to be written in the file.
   */
  def createFile(fileSystem: FileSystem, filePath: Path, contents: String): Unit = {
    require(contents.nonEmpty, "Content cannot be empty.")
    val writer = new OutputStreamWriter(fileSystem.create(filePath), StandardCharsets.UTF_8)
    writer.write(contents)
    writer.close()
  }

  /**
   * Read the file contents.
   *
   * @param fileSystem file system.
   * @param filePath The file to be read.
   * @return the file content.
   */
  def readContents(fileSystem: FileSystem, filePath: Path): String = {
    new String(loadByteArray(fileSystem, filePath))
  }

  /**
   * Get the size of directory in HDFS.
   *
   * @param directoryPath the directory path.
   * @return the size in byte of the directory.
   */
  def getDirectorySize(
      directoryPath: Path,
      hadoopConfiguration: Configuration = new Configuration): Long = {
    val fileSystem = directoryPath.getFileSystem(hadoopConfiguration)
    fileSystem.getContentSummary(directoryPath).getLength
  }

  /**
   * Create the directory if it does not exist.
   *
   * @param dirPath the directory path.
   */
  def createDirectory(
      dirPath: Path,
      hadoopConfiguration: Configuration = new Configuration): Unit = {
    val fileSystem = dirPath.getFileSystem(hadoopConfiguration)
    if (!fileSystem.exists(dirPath)) {
      fileSystem.mkdirs(dirPath)
    }
  }

  /**
   * Deletes given directory path with option of deleting recursively.
   *
   * @param dirPath the directory path to be deleted.
   * @param isRecursive option to delete recursively.
   */
  def delete(
      dirPath: Path,
      hadoopConfiguration: Configuration = new Configuration,
      isRecursive: Boolean = true): Unit = {
    dirPath.getFileSystem(hadoopConfiguration).delete(dirPath, isRecursive)
  }

  /**
   * Save the given array of bytes to the given storage location.
   *
   * @param fileSystem The file system.
   * @param path The path to the storage location.
   * @param bytes The array of bytes to be written.
   */
  def saveByteArray(fileSystem: FileSystem, path: Path, bytes: Array[Byte]): Unit = {
    require(bytes.nonEmpty, "Cannot save empty array of bytes.")
    val fileOutputStream = fileSystem.create(path)
    fileOutputStream.write(bytes)
    fileOutputStream.close()
  }

  /**
   * Load the array of bytes from the given storage location.
   *
   * @param fileSystem The file system.
   * @param path The path to the storage location.
   * @return The array of bytes read from storage.
   */
  def loadByteArray(fileSystem: FileSystem, path: Path): Array[Byte] = {
    val fileInputStream = fileSystem.open(path)
    val len = fileSystem.getFileStatus(path).getLen
    val bytes = new Array[Byte](len.toInt)
    fileInputStream.readFully(bytes)
    fileInputStream.close()
    bytes
  }
}
