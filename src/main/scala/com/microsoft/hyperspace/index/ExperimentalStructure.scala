package com.microsoft.hyperspace.index

import java.io.FileNotFoundException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}

import com.microsoft.hyperspace.index.Content1.Directory
import com.microsoft.hyperspace.util.{JsonUtils, PathUtils}
// scalastyle:off
case class Content1(root: Content1.Directory) {
  def files: Seq[Path] = rec(new Path(root.name), root)

  private def rec(prefixPath: Path, directory: Directory): Seq[Path] = {
    val files = directory.files.map(f => new Path(prefixPath, f.name))
    files ++ directory.subDirs.flatMap {
      dir => rec(new Path(prefixPath, dir.name), dir)
    }
  }
}
object Content1 {

  case class Directory(
    name: String,
    files: Seq[Directory.FileInfo] = Seq(),
    subDirs: Seq[Directory] = Seq())

  object Directory {

    case class FileInfo(name: String, size: Long, modifiedTime: Long)

    object FileInfo {
      def apply(s: FileStatus): FileInfo =
        FileInfo(s.getPath.getName, s.getLen, s.getModificationTime)
    }

    def apply(path: Path): Directory = {
      val files: Seq[FileInfo] = {
        try {
          val fs = path.getFileSystem(new Configuration)
          val statuses = fs.listStatus(path)

          // Assuming index directories don't contain nested directories. Only Leaf files.
          assert(statuses.forall(!_.isDirectory))

          statuses.map(FileInfo(_)).toSeq
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

  def apply(path: Path): Content1 = Content1(Directory(path))
}

object Main extends App {
  //val path = PathUtils.makeAbsolute(new Path("C:\\Users\\apdave\\github\\hyperspace-1\\src\\main\\scala\\com\\microsoft\\hyperspace\\index\\rules"))
  val path = new Path("C:\\Users\\apdave\\repo2\\testdata\\sampleparquet")
  val content = Content1(path)
  println(content)
  val str = JsonUtils.toJson(content)
  println(str)
  val content2 = JsonUtils.fromJson[Content1](str)
  println(content2)
  println(content == content2)
  for (file <- content2.files) println(file)
}
