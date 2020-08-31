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

import java.io.FileNotFoundException

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path, PathFilter}
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.util.PathUtils

// IndexLogEntry-specific fingerprint to be temporarily used where fingerprint is not defined.
case class NoOpFingerprint() {
  val kind: String = "NoOp"
  val properties: Map[String, String] = Map()
}

// IndexLogEntry-specific Content that uses IndexLogEntry-specific fingerprint.
case class Content(root: Directory, fingerprint: NoOpFingerprint = NoOpFingerprint()) {
  // List of fully qualified paths of all files mentioned in this Content object.
  @JsonIgnore
  lazy val files: Seq[Path] = {
    // Recursively append parent dir path to subdirs and files.
    def rec(prefixPath: Path, directory: Directory): Seq[Path] = {
      val files = directory.files.map(f => new Path(prefixPath, f.name))
      files ++ directory.subDirs.flatMap { dir =>
        rec(new Path(prefixPath, dir.name), dir)
      }
    }

    rec(new Path(root.name), root)
  }
}

object Content {
  // Create a Content object from a directory path by recursively listing its leaf files. All
  // files from the directory tree will be part of the Content.
  def fromPath(path: Path): Content = Content(Directory.fromDir(path))

  // Create a Content object from a specified list of leaf files. Any files not listed here will
  // NOT be part of the returned object
  def fromLeafFiles(files: Seq[FileStatus]): Content = {
    /* from org.apache.spark.sql.execution.datasources.PartitionAwareFileIndex. */
    val leafDirToChildrenFiles = files.toArray.groupBy(_.getPath.getParent)
    val rootPath = getRoot(leafDirToChildrenFiles.head._1)

    val subDirs = leafDirToChildrenFiles
      .filterNot(_._1.isRoot)
      .map {
        case (dir, statuses) => createSubDirectoryTree(dir, statuses)
      }
      .toSeq

    val rootFiles = leafDirToChildrenFiles.getOrElse(rootPath, Array()).map(FileInfo(_))

    Content(Directory(rootPath.toString, rootFiles, subDirs))
  }

  // Return file system root path from any path. E.g. "file:/C:/a/b/c" will have root "file:/C:/".
  // For linux systems, this root will be "file:/". Other hdfs compatible file systems will have
  // corresponding roots.
  private def getRoot(path: Path): Path = {
    if (path.isRoot) path else getRoot(path.getParent)
  }

  // Create leaf subdirectory. Leaf Subdirectory is the first parent of leaf files.
  private def createSubDirectoryTree(dirPath: Path, statuses: Array[FileStatus]): Directory = {
    if (dirPath.getParent.isRoot) {
      Directory(dirPath.getName, files = statuses.map(FileInfo(_)))
    } else {
      createNonLeafSubDirectoryHelper(
        dirPath.getParent,
        Directory(dirPath.getName, files = statuses.map(FileInfo(_))))
    }
  }

  // Create Non-Leaf subdirectory tree starting from the topmost non-root directory.
  private def createNonLeafSubDirectoryHelper(dirPath: Path, child: Directory): Directory = {
    if (dirPath.getParent.isRoot) {
      Directory(dirPath.getName, files = Seq(), subDirs = Seq(child))
    } else {
      createNonLeafSubDirectoryHelper(
        dirPath.getParent,
        Directory(dirPath.getName, files = Seq(), subDirs = Seq(child)))
    }
  }
}

case class Directory(name: String, files: Seq[FileInfo] = Seq(), subDirs: Seq[Directory] = Seq())

object Directory {

  /**
   * Create a Directory object from a directory path by recursively listing its leaf files. All
   * files from the directory tree will be part of the Content.
   *
   * @param path Starting directory path under which the files will be considered part of the
   *             Directory object.
   * @param pathFilter Filter for accepting paths. The default filter is picked from spark
   *                   codebase, which filters out files like _SUCCESS.
   * @param throwIfNotExists Throws FileNotFoundException if path is not found. Else creates a
   *                         blank directory tree with no files.
   * @return Directory tree starting at root, and containing the files from "path" argument.
   */
  def fromDir(
      path: Path,
      pathFilter: PathFilter = PathUtils.DataPathFilter,
      throwIfNotExists: Boolean = false): Directory = {
    val files: Seq[FileInfo] = {
      try {
        val fs = path.getFileSystem(new Configuration)
        val statuses = fs.listStatus(path).filter(s => pathFilter.accept(s.getPath))

        // TODO: Implement support for recursive listing of files below and remove the assert.
        assert(statuses.forall(!_.isDirectory))

        statuses.map(FileInfo(_)).toSeq
      } catch {
        case _: FileNotFoundException if !throwIfNotExists => Seq()
        case e: Throwable => throw e
      }
    }
    if (path.isRoot) {
      Directory(path.toString, files)
    } else {
      prependDirectory(path.getParent, Directory(path.getName, files))
    }
  }

  private def prependDirectory(path: Path, child: Directory): Directory = {
    if (path.isRoot) {
      Directory(path.toString, Seq(), Seq(child))
    } else {
      prependDirectory(path.getParent, Directory(path.getName, Seq(), Seq(child)))
    }
  }
}

// modifiedTime is an Epoch time in milliseconds. (ms since 1970-01-01T00:00:00.000 UTC).
case class FileInfo(name: String, size: Long, modifiedTime: Long)

object FileInfo {
  def apply(s: FileStatus): FileInfo = {
    require(s.isFile, s"${FileInfo.getClass.getName} is applicable for files, not directories.")
    FileInfo(s.getPath.getName, s.getLen, s.getModificationTime)
  }
}

// IndexLogEntry-specific CoveringIndex that represents derived dataset.
case class CoveringIndex(properties: CoveringIndex.Properties) {
  val kind = "CoveringIndex"
}
object CoveringIndex {
  case class Properties(columns: Properties.Columns, schemaString: String, numBuckets: Int)
  object Properties {
    case class Columns(indexed: Seq[String], included: Seq[String])
  }
}

// IndexLogEntry-specific Signature that stores the signature provider and value.
case class Signature(provider: String, value: String)

// IndexLogEntry-specific LogicalPlanFingerprint to store fingerprint of logical plan.
case class LogicalPlanFingerprint(properties: LogicalPlanFingerprint.Properties) {
  val kind = "LogicalPlan"
}
object LogicalPlanFingerprint {
  case class Properties(signatures: Seq[Signature])
}

// IndexLogEntry-specific Hdfs that represents the source data.
case class Hdfs(properties: Hdfs.Properties) {
  val kind = "HDFS"
}
object Hdfs {
  case class Properties(content: Content)
}

// IndexLogEntry-specific Relation that represents the source relation.
case class Relation(
    rootPaths: Seq[String],
    data: Hdfs,
    dataSchemaJson: String,
    fileFormat: String,
    options: Map[String, String])

// IndexLogEntry-specific SparkPlan that represents the source plan.
case class SparkPlan(properties: SparkPlan.Properties) {
  val kind = "Spark"
}

object SparkPlan {
  case class Properties(
      relations: Seq[Relation],
      rawPlan: String, // null for now
      sql: String, // null for now
      fingerprint: LogicalPlanFingerprint)
}

// IndexLogEntry-specific Source that uses SparkPlan as a plan.
case class Source(plan: SparkPlan)

// IndexLogEntry that captures index-related information.
case class IndexLogEntry(
    name: String,
    derivedDataset: CoveringIndex,
    content: Content,
    source: Source,
    extra: Map[String, String])
    extends LogEntry(IndexLogEntry.VERSION) {

  def schema: StructType =
    DataType.fromJson(derivedDataset.properties.schemaString).asInstanceOf[StructType]

  def created: Boolean = state.equals(Constants.States.ACTIVE)

  def indexedColumns: Seq[String] = derivedDataset.properties.columns.indexed

  def includedColumns: Seq[String] = derivedDataset.properties.columns.included

  def numBuckets: Int = derivedDataset.properties.numBuckets

  def relations: Seq[Relation] = source.plan.properties.relations

  def config: IndexConfig = IndexConfig(name, indexedColumns, includedColumns)

  def signature: Signature = {
    val sourcePlanSignatures = source.plan.properties.fingerprint.properties.signatures
    assert(sourcePlanSignatures.length == 1)
    sourcePlanSignatures.head
  }

  override def equals(o: Any): Boolean = o match {
    case that: IndexLogEntry =>
      config.equals(that.config) &&
        signature.equals(that.signature) &&
        numBuckets.equals(that.numBuckets) &&
        content.root.equals(that.content.root) &&
        source.equals(that.source) &&
        state.equals(that.state)
    case _ => false
  }

  override def hashCode(): Int = {
    config.hashCode + signature.hashCode + numBuckets.hashCode + content.hashCode
  }
}

object IndexLogEntry {
  val VERSION: String = "0.1"

  def schemaString(schema: StructType): String = schema.json
}
