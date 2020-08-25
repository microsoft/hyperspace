package com.microsoft.hyperspace.index

import java.io.FileNotFoundException

import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import com.microsoft.hyperspace.util.{JsonUtils, PathUtils}
// scalastyle:off
case class NewContent(root: Directory) {
  @JsonIgnore
  lazy val files: Seq[Path] = rec(new Path(root.name), root)

  private def rec(prefixPath: Path, directory: Directory): Seq[Path] = {
    val files = directory.files.map(f => new Path(prefixPath, f.name))
    files ++ directory.subDirs.flatMap { dir =>
      rec(new Path(prefixPath, dir.name), dir)
    }
  }
}
object NewContent {
  def apply(path: Path): NewContent = NewContent(Directory(path))

  def fromLeafFiles(files: Seq[FileStatus]): NewContent = {
    val leafDirToChildrenFiles: Map[Path, Array[FileStatus]] =
      files.toArray.groupBy(_.getPath.getParent)
    val rootPath = getRoot(leafDirToChildrenFiles.head._1)

    val subDirs = leafDirToChildrenFiles
      .filterNot(_._1.isRoot)
      .map {
        case (dir, statuses) =>
          createDirectoryTreeWithoutRoot(dir, statuses)
      }
      .toSeq

    val rootFiles: Seq[FileInfo1] =
      leafDirToChildrenFiles.getOrElse(rootPath, Array()).map(FileInfo1(_))
    NewContent(Directory(rootPath.toString, rootFiles, subDirs))
  }

  private def getRoot(path: Path): Path = {
    if (path.isRoot) path else getRoot(path.getParent)
  }

  private def createDirectoryTreeWithoutRoot(
      dirPath: Path,
      statuses: Array[FileStatus]): Directory = {
    if (dirPath.getParent.isRoot) {
      Directory(dirPath.getName, files = statuses.map(FileInfo1(_)))
    } else {
      createTopLevelNonRootDirectory(
        dirPath.getParent,
        Directory(dirPath.getName, files = statuses.map(FileInfo1(_))))
    }
  }

  private def createTopLevelNonRootDirectory(dirPath: Path, child: Directory): Directory = {
    if (dirPath.getParent.isRoot) {
      Directory(dirPath.getName, files = Seq(), subDirs = Seq(child))
    } else {
      createTopLevelNonRootDirectory(
        dirPath.getParent,
        Directory(dirPath.getName, files = Seq(), subDirs = Seq(child)))
    }
  }
}

case class Directory(name: String, files: Seq[FileInfo1] = Seq(), subDirs: Seq[Directory] = Seq())

object Directory {
  def apply(path: Path): Directory = {
    val files: Seq[FileInfo1] = {
      try {
        val fs = path.getFileSystem(new Configuration)
        val statuses = fs.listStatus(path)

        // Assuming index directories don't contain nested directories. Only Leaf files.
        assert(statuses.forall(!_.isDirectory))

        statuses.map(FileInfo1(_)).toSeq
      } catch {
        // FileNotFoundException is an expected exception before index gets created.
        case _: FileNotFoundException => Seq()
        case e: Throwable => throw e
      }
    }
    if (path.isRoot) {
      Directory(path.toString, files, Seq())
    } else {
      Directory(path.getParent, Directory(path.getName, files, Seq()))
    }
  }

  def apply(path: Path, child: Directory): Directory = {
    if (path.isRoot) {
      Directory(path.toString, Seq(), Seq(child))
    } else {
      Directory(path.getParent, Directory(path.getName, Seq(), Seq(child)))
    }
  }
}

case class FileInfo1(name: String, size: Long, modifiedTime: Long)

object FileInfo1 {
  def apply(s: FileStatus): FileInfo1 =
    FileInfo1(s.getPath.getName, s.getLen, s.getModificationTime)
}

object Main extends App {
  //val path = PathUtils.makeAbsolute(new Path("C:\\Users\\apdave\\github\\hyperspace-1\\src\\main\\scala\\com\\microsoft\\hyperspace\\index\\rules"))
  val path = PathUtils.makeAbsolute(new Path("C:\\Users\\apdave\\repo2\\testdata\\sampleparquet"))
  val content = NewContent(path)
  println(content)
  val str = JsonUtils.toJson(content)
  println(str)
  val content2 = JsonUtils.fromJson[NewContent](str)
  println(content2)
  println(content == content2)
  for (file <- content2.files) println(file)
  val fs = content2.files.head.getFileSystem(new Configuration)
  var newContent = NewContent.fromLeafFiles(content2.files.map(fs.getFileStatus))
  println(JsonUtils.toJson(newContent))

  val paths: Seq[FileStatus] = (content2.files ++ Seq(
    "file:/C:/Users/apdave/repo2/testdata/sampleparquet/part-00000-5782bdd3-4729-44e6-b54b-51b557f66792-c000.snappy.parquet",
    "file:/C:/Users/apdave/repo2/testdata/sampleparquet/part-00000-e8c8821f-a1b2-4b3b-be9e-687b6fa6d057-c000.snappy.parquet",
    "file:/C:/Users/apdave/repo2/testdata/sampleparquet/_SUCCESS",
    "file:/C:/ApoorveLaptop-{3D0D96FA-6031-4AEC-A22C-D55D24797ACB}.txt",
    "file:/C:/spark/README.md",
    "file:/C:/spark/RELEASE",
    "file:/C:/spark/NOTICE",
    "file:/C:/spark/LICENSE")
    .map(new Path(_)))
    .map(fs.getFileStatus)

  val newContent2 = NewContent.fromLeafFiles(paths)
  println(JsonUtils.toJson(newContent2))
  for (file <- newContent2.files) println(file)
}
