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

import scala.annotation.tailrec
import scala.collection.mutable.{HashMap, ListBuffer}
import scala.collection.mutable

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path, PathFilter}
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}

import com.microsoft.hyperspace.HyperspaceException
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
    // Recursively find files from directory tree.
    rec(new Path(root.name), root, (f, prefix) => new Path(prefix, f.name))
  }

  @JsonIgnore
  lazy val fileInfos: Set[FileInfo] = {
    rec(
      new Path(root.name),
      root,
      (f, prefix) =>
        FileInfo(new Path(prefix, f.name).toString, f.size, f.modifiedTime, f.id)).toSet
  }

  private def rec[T](
      prefixPath: Path,
      directory: Directory,
      func: (FileInfo, Path) => T): Seq[T] = {
    val files = directory.files.map(f => func(f, prefixPath))
    files ++ directory.subDirs.flatMap { dir =>
      rec(new Path(prefixPath, dir.name), dir, func)
    }
  }
}

object Content {

  /**
   * Create a Content object from a directory path by recursively listing its leaf files. All
   * files from the directory tree will be part of the Directory.
   *
   * @param path Starting directory path under which the files will be considered part of the
   *             Directory object.
   * @param fileIdTracker FileIdTracker to keep mapping of file properties to assigned file ids.
   * @param pathFilter Filter for accepting paths. The default filter is picked from spark
   *                   codebase, which filters out files like _SUCCESS.
   * @param throwIfNotExists Throws FileNotFoundException if path is not found. Else creates a
   *                         blank directory tree with no files.
   * @return Content object with Directory tree starting at root, and containing all leaf files
   *         from "path" argument.
   */
  def fromDirectory(
      path: Path,
      fileIdTracker: FileIdTracker,
      pathFilter: PathFilter = PathUtils.DataPathFilter,
      throwIfNotExists: Boolean = false): Content =
    Content(Directory.fromDirectory(path, fileIdTracker, pathFilter, throwIfNotExists))

  /**
   * Create a Content object from a specified list of leaf files. Any files not listed here will
   * NOT be part of the returned object.
   *
   * @param files List of leaf files.
   * @param fileIdTracker FileIdTracker to keep mapping of file properties to assigned file ids.
   * @return Content object with Directory tree from leaf files.
   */
  def fromLeafFiles(
      files: Seq[FileStatus],
      fileIdTracker: FileIdTracker): Option[Content] = {
    if (files.nonEmpty) {
      Some(Content(Directory.fromLeafFiles(files, fileIdTracker)))
    } else {
      None
    }
  }
}

/**
 * Directory is a representation of file system directory. It consists of a name (directory name),
 * a list of files represented by sequence of [[FileInfo]], and a list of subdirectories.
 *
 * @param name Directory name.
 * @param files List of leaf files in this directory.
 * @param subDirs List of sub-directories in this directory.
 */
case class Directory(
    name: String,
    files: Seq[FileInfo] = Seq(),
    subDirs: Seq[Directory] = Seq()) {

  /**
   * Merge two Directory objects. For e.g., merging the following directories
   * /file:/C:/
   *          a/
   *            b/
   *              f1, f2
   * and
   * /file:/C:/
   *          a/
   *            f3, f4
   * will be
   * /file:/C:/
   *           a/
   *             f3, f4
   *             b/
   *               f1, f2
   *
   * @param that The other directory to merge this with.
   * @return Merged directory.
   * @throws HyperspaceException If two directories to merge have different names.
   */
  def merge(that: Directory): Directory = {
    if (name.equals(that.name)) {
      val allFiles = files ++ that.files
      val subDirMap = subDirs.map(dir => dir.name -> dir).toMap
      val thatSubDirMap = that.subDirs.map(dir => dir.name -> dir).toMap
      val mergedSubDirs = (subDirMap.keySet ++ thatSubDirMap.keySet).toSeq.map { dirName =>
        if (subDirMap.contains(dirName) && thatSubDirMap.contains(dirName)) {
          // If both directories contain a subDir with same name, merge corresponding subDirs
          // recursively.
          subDirMap(dirName).merge(thatSubDirMap(dirName))
        } else {
          // Pick the subDir from whoever contains it.
          subDirMap.getOrElse(dirName, thatSubDirMap(dirName))
        }
      }

      Directory(name, allFiles, subDirs = mergedSubDirs)
    } else {
      throw HyperspaceException(
        s"Merging directories with names $name and ${that.name} failed. " +
          "Directory names must be same for merging directories.")
    }
  }
}

object Directory {

  /**
   * Create a Directory object from a directory path by recursively listing its leaf files. All
   * files from the directory tree will be part of the Directory.
   *
   * If the directory doesn't exist on file system, it will either throw an exception if
   * throwIfNotExists flag is set. Otherwise, this will create an empty Directory object
   * starting at the root, ending at the directory path specified.
   *
   * @param path Starting directory path under which the files will be considered part of the
   *             Directory object.
   * @param fileIdTracker FileIdTracker to keep mapping of file properties to assigned file ids.
   * @param pathFilter Filter for accepting paths. The default filter is picked from spark
   *                   codebase, which filters out files like _SUCCESS.
   * @param throwIfNotExists If true, throw FileNotFoundException if path is not found. If set to
   *                         false, create a blank directory tree with no files.
   * @return Directory tree starting at root, and containing the files from "path" argument.
   */
  def fromDirectory(
      path: Path,
      fileIdTracker: FileIdTracker,
      pathFilter: PathFilter = PathUtils.DataPathFilter,
      throwIfNotExists: Boolean = false): Directory = {
    val fs = path.getFileSystem(new Configuration)
    val leafFiles = listLeafFiles(path, pathFilter, throwIfNotExists, fs)

    if (leafFiles.nonEmpty) {
      fromLeafFiles(leafFiles, fileIdTracker)
    } else {
      // leafFiles is empty either because the directory doesn't exist on disk or this directory
      // and all its subdirectories, if present, are empty. In both cases, create an empty
      // directory object.
      createEmptyDirectory(path)
    }
  }

  @tailrec
  private def createEmptyDirectory(path: Path, subDirs: Seq[Directory] = Seq()): Directory = {
    if (path.isRoot) {
      Directory(path.toString, subDirs = subDirs)
    } else {
      createEmptyDirectory(path.getParent, Seq(Directory(path.getName, subDirs = subDirs)))
    }
  }

  /**
   * Create a Content object from a specified list of leaf files. Any files not listed here will
   * NOT be part of the returned object.
   * fileIdTracker is used to keep track of file ids. For a new source data file, FileIdTracker
   * generates a new unique file id and assigns it to the file.
   * Pre-requisite: files list should be non-empty.
   * Pre-requisite: all files must be leaf files.
   *
   * @param files List of leaf files.
   * @param fileIdTracker FileIdTracker to keep mapping of file properties to assigned file ids.
   *                      Note: If a new leaf file is discovered, the input fileIdTracker gets
    *                     updated by adding it to the files it is tracking.
   * @return Content object with Directory tree from leaf files.
   */
  def fromLeafFiles(
      files: Seq[FileStatus],
      fileIdTracker: FileIdTracker): Directory = {
    require(
      files.nonEmpty,
      s"Empty files list found while creating a ${Directory.getClass.getName}.")

    require(
      files.forall(!_.isDirectory),
      "All files must be leaf files for creation of Directory.")

    /* from org.apache.spark.sql.execution.datasources.InMemoryFileIndex. */
    val leafDirToChildrenFiles = files.toArray.groupBy(_.getPath.getParent)

    // Hashmap from directory path to Directory object, used below for quick access from path.
    val pathToDirectory = HashMap[Path, Directory]()

    // Set size hint for performance improvement.
    fileIdTracker.setSizeHint(files.length)

    for ((dirPath, files) <- leafDirToChildrenFiles) {
      val allFiles = files.map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = false))

      if (pathToDirectory.contains(dirPath)) {
        // Map already contains this directory. Just append the files to its existing list.
        pathToDirectory(dirPath).files.asInstanceOf[ListBuffer[FileInfo]].appendAll(allFiles)
      } else {
        var curDirPath = dirPath
        // Create a new Directory object and add it to Map
        val subDirs = ListBuffer[Directory]()
        var directory = Directory(curDirPath.getName, files = allFiles, subDirs = subDirs)
        pathToDirectory.put(curDirPath, directory)

        // Keep creating parent Directory objects and add to the map if non-existing.
        while (curDirPath.getParent != null && !pathToDirectory.contains(curDirPath.getParent)) {
          curDirPath = curDirPath.getParent

          directory = Directory(
            if (curDirPath.isRoot) curDirPath.toString else curDirPath.getName,
            subDirs = ListBuffer(directory),
            files = ListBuffer[FileInfo]())

          pathToDirectory.put(curDirPath, directory)
        }

        // Either root is reached (parent == null) or an existing directory is found. If it's the
        // latter, add the newly created directory tree to its subDirs.
        if (curDirPath.getParent != null) {
          pathToDirectory(curDirPath.getParent).subDirs
            .asInstanceOf[ListBuffer[Directory]]
            .append(directory)
        }
      }
    }

    pathToDirectory(getRoot(files.head.getPath))
  }

  // Return file system root path from any path. E.g. "file:/C:/a/b/c" will have root "file:/C:/".
  // For linux systems, this root will be "file:/". Other hdfs compatible file systems will have
  // corresponding roots.
  @tailrec
  private def getRoot(path: Path): Path = {
    if (path.isRoot) path else getRoot(path.getParent)
  }

  private def listLeafFiles(
      path: Path,
      pathFilter: PathFilter = PathUtils.DataPathFilter,
      throwIfNotExists: Boolean = false,
      fs: FileSystem): Seq[FileStatus] = {
    try {
      val (files, directories) = fs.listStatus(path).partition(_.isFile)
      // TODO: explore fs.listFiles(recursive = true) for better performance of file listing.
      files.filter(s => pathFilter.accept(s.getPath)) ++
        directories.flatMap(d => listLeafFiles(d.getPath, pathFilter, throwIfNotExists, fs))
    } catch {
      case _: FileNotFoundException if !throwIfNotExists => Seq()
      case e: Throwable => throw e
    }
  }
}

// modifiedTime is an Epoch time in milliseconds. (ms since 1970-01-01T00:00:00.000 UTC).
// id is a unique identifier generated by Hyperspace, for each unique combination of
// file's name, size and modifiedTime.
case class FileInfo(name: String, size: Long, modifiedTime: Long, id: Long) {
  override def equals(o: Any): Boolean = o match {
    case that: FileInfo =>
      name.equals(that.name) &&
      size.equals(that.size) &&
      modifiedTime.equals(that.modifiedTime)
    case _ => false
  }

  override def hashCode(): Int = {
    name.hashCode + size.hashCode + modifiedTime.hashCode
  }
}

object FileInfo {
  def apply(s: FileStatus, id: Long, asFullPath: Boolean): FileInfo = {
    require(s.isFile, s"${FileInfo.getClass.getName} is applicable for files, not directories.")
    if (asFullPath) {
      FileInfo(s.getPath.toString, s.getLen, s.getModificationTime, id)
    } else {
      FileInfo(s.getPath.getName, s.getLen, s.getModificationTime, id)
    }
  }
}

// IndexLogEntry-specific CoveringIndex that represents derived dataset.
case class CoveringIndex(properties: CoveringIndex.Properties) {
  val kind = "CoveringIndex"
}
object CoveringIndex {
  case class Properties(columns: Properties.Columns,
    schemaString: String,
    numBuckets: Int,
    properties: Map[String, String])

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

/**
 * Captures any HDFS updates.
 *
 * @param appendedFiles Appended files.
 * @param deletedFiles Deleted files.
 */
case class Update(
    appendedFiles: Option[Content] = None,
    deletedFiles: Option[Content] = None)

// IndexLogEntry-specific Hdfs that represents the source data.
case class Hdfs(properties: Hdfs.Properties) {
  val kind = "HDFS"
}
object Hdfs {

  /**
   * Hdfs file properties.
   *
   * @param content Content object representing Hdfs file based data source.
   * @param update Captures any updates since 'content' was created.
   */
  case class Properties(content: Content, update: Option[Update] = None)
}

/**
 * IndexLogEntry-specific Relation that represents the source relation.
 *
 * @param rootPaths List of root paths for the source relation.
 * @param data Source data for the relation.
 *             Hdfs.properties.content captures source data which derived dataset was created from.
 *             Hdfs.properties.update captures any updates since the derived dataset was created.
 * @param dataSchemaJson Schema in json format.
 * @param fileFormat File format name.
 * @param options Options to read the source relation.
 */
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
    properties: Map[String, String])
    extends LogEntry(IndexLogEntry.VERSION) {

  def schema: StructType =
    DataType.fromJson(derivedDataset.properties.schemaString).asInstanceOf[StructType]

  def created: Boolean = state.equals(Constants.States.ACTIVE)

  def relations: Seq[Relation] = {
    // Only one relation is currently supported.
    assert(source.plan.properties.relations.size == 1)
    source.plan.properties.relations
  }

  // FileInfo's 'name' contains the full path to the file.
  @JsonIgnore
  lazy val sourceFileInfoSet: Set[FileInfo] = {
    relations.head.data.properties.content.fileInfos
  }

  def sourceUpdate: Option[Update] = {
    relations.head.data.properties.update
  }

  def hasSourceUpdate: Boolean = {
    sourceUpdate.isDefined && (appendedFiles.nonEmpty || deletedFiles.nonEmpty)
  }

  // FileInfo's 'name' contains the full path to the file.
  @JsonIgnore
  lazy val appendedFiles: Set[FileInfo] = {
    sourceUpdate.flatMap(_.appendedFiles).map(_.fileInfos).getOrElse(Set())
  }

  // FileInfo's 'name' contains the full path to the file.
  @JsonIgnore
  lazy val deletedFiles: Set[FileInfo] = {
    sourceUpdate.flatMap(_.deletedFiles).map(_.fileInfos).getOrElse(Set())
  }

  def copyWithUpdate(
      latestFingerprint: LogicalPlanFingerprint,
      appended: Seq[FileInfo],
      deleted: Seq[FileInfo]): IndexLogEntry = {
    def toFileStatus(f: FileInfo) = {
      new FileStatus(f.size, false, 0, 1, f.modifiedTime, new Path(f.name))
    }
    copy(
      source = source.copy(
        plan = source.plan.copy(
          properties = source.plan.properties.copy(
            fingerprint = latestFingerprint,
            relations = Seq(
              relations.head.copy(
                data = relations.head.data.copy(
                  properties = relations.head.data.properties.copy(
                    update = Some(
                      Update(
                        appendedFiles =
                          Content.fromLeafFiles(appended.map(toFileStatus), fileIdTracker),
                        deletedFiles =
                          Content.fromLeafFiles(deleted.map(toFileStatus), fileIdTracker)))))))))))
  }

  def bucketSpec: BucketSpec =
    BucketSpec(
      numBuckets = numBuckets,
      bucketColumnNames = indexedColumns,
      sortColumnNames = indexedColumns)

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

  def numBuckets: Int = derivedDataset.properties.numBuckets

  def config: IndexConfig = IndexConfig(name, indexedColumns, includedColumns)

  def indexedColumns: Seq[String] = derivedDataset.properties.columns.indexed

  def includedColumns: Seq[String] = derivedDataset.properties.columns.included

  def signature: Signature = {
    val sourcePlanSignatures = source.plan.properties.fingerprint.properties.signatures
    assert(sourcePlanSignatures.length == 1)
    sourcePlanSignatures.head
  }

  def hasLineageColumn: Boolean =
    derivedDataset.properties.properties.getOrElse(
      IndexConstants.LINEAGE_PROPERTY, IndexConstants.INDEX_LINEAGE_ENABLED_DEFAULT).toBoolean

  @JsonIgnore
  lazy val fileIdTracker: FileIdTracker = {
    val tracker = new FileIdTracker
    tracker.addFileInfo(sourceFileInfoSet ++ content.fileInfos)
    tracker
  }

  override def hashCode(): Int = {
    config.hashCode + signature.hashCode + numBuckets.hashCode + content.hashCode
  }

  /**
   * A mutable map for holding auxiliary information of this index log entry while applying rules.
   */
  @JsonIgnore
  private val tags: mutable.Map[(LogicalPlan, IndexLogEntryTag[_]), Any] = mutable.Map.empty

  def setTagValue[T](plan: LogicalPlan, tag: IndexLogEntryTag[T], value: T): Unit = {
    tags((plan, tag)) = value
  }

  def getTagValue[T](plan: LogicalPlan, tag: IndexLogEntryTag[T]): Option[T] = {
    tags.get((plan, tag)).map(_.asInstanceOf[T])
  }

  def unsetTagValue[T](plan: LogicalPlan, tag: IndexLogEntryTag[T]): Unit = {
    tags.remove((plan, tag))
  }

  def setTagValue[T](tag: IndexLogEntryTag[T], value: T): Unit = {
    tags((null, tag)) = value
  }

  def getTagValue[T](tag: IndexLogEntryTag[T]): Option[T] = {
    tags.get((null, tag)).map(_.asInstanceOf[T])
  }

  def unsetTagValue[T](tag: IndexLogEntryTag[T]): Unit = {
    tags.remove((null, tag))
  }
}

// A tag of a `IndexLogEntry`, which defines name and type.
case class IndexLogEntryTag[T](name: String)

object IndexLogEntry {
  val VERSION: String = "0.1"

  def schemaString(schema: StructType): String = schema.json
}

/**
 * Provides functionality to generate unique file ids for files.
 */
class FileIdTracker {
  private var maxId: Long = -1L

  // Combination of file properties, used as key, to identify a
  // unique file for which an id is generated.
  type key = (
    String, // Full path.
      Long, // Size.
      Long  // Modified time.
    )
  private val fileToIdMap: mutable.HashMap[key, Long] = mutable.HashMap()

  def getMaxFileId: Long = maxId

  def getFileToIdMap: HashMap[key, Long] = fileToIdMap

  def getFileId(path: String, size: Long, modifiedTime: Long): Option[Long] =
    fileToIdMap.get((path, size, modifiedTime))

  def setSizeHint(size: Int): Unit = fileToIdMap.sizeHint(size)

  /**
   * Add a set of FileInfos to the fileToIdMap. The assumption is
   * that the each FileInfo already has a valid file id if an entry
   * with that key already exists in the fileToIdMap, then it has
   * the same file id (i.e. no new file id is generated and only
   * maxId is updated according to the new entries).
   *
   * @param files Set of FileInfo instances to add to the fileToIdMap.
   */
  def addFileInfo(files: Set[FileInfo]): Unit = {
    setSizeHint(files.size)
    files.foreach { f =>
      if (f.id == IndexConstants.UNKNOWN_FILE_ID) {
        throw HyperspaceException(
          s"Cannot add file info with unknown id. (file: ${f.name}).")
      }

      val key = (f.name, f.size, f.modifiedTime)
      fileToIdMap.put(key, f.id) match {
        case Some(id) =>
          if (id != f.id) {
            throw HyperspaceException(
              "Adding file info with a conflicting id. " +
                s"(existing id: $id, new id: ${f.id}, file: ${f.name}).")
          }

        case None =>
          maxId = Math.max(maxId, f.id)
      }
    }
  }

  /**
   * Try to add file properties to fileToIdMap. If the file is already in
   * the map then return its current id. Otherwise, generate a new id,
   * according to current maxId, and update fileToIdMap.
   *
   * @param file FileStatus to lookup in fileToIdMap.
   * @return Assigned id to the given file.
   */
  def addFile(file: FileStatus): Long = {
    fileToIdMap.getOrElseUpdate(
      (file.getPath.toString, file.getLen, file.getModificationTime),
      {
        maxId += 1
        maxId
      })
  }
}
