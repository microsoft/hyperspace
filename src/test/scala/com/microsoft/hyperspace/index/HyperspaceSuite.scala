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

import java.io.File
import java.nio.charset.StandardCharsets

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.util.hyperspace.Utils

import com.microsoft.hyperspace.{BuildInfo, Hyperspace, SparkInvolvedSuite}
import com.microsoft.hyperspace.util.{FileUtils, PathUtils}

trait HyperspaceSuite extends SparkFunSuite with SparkInvolvedSuite {

  // Temporary directory
  lazy val tempDir: Path = new Path(Utils.createTempDir().getAbsolutePath)

  // Returns a path starting from a temporary directory for the test.
  def inTempDir(path: String): String = new Path(tempDir, path).toString

  val indexLocationDirName: String = "indexLocation"

  // This is the system path that PathResolver uses to get the root of the indexes.
  // Each test suite that extends HyperspaceSuite should define this.
  lazy val systemPath: Path = PathUtils.makeAbsolute(inTempDir(indexLocationDirName))

  override def beforeAll(): Unit = {
    super.beforeAll()
    FileUtils.delete(tempDir)
    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, systemPath.toUri.toString)
    clearCache()
  }

  override def afterAll(): Unit = {
    clearCache()
    spark.conf.unset(IndexConstants.INDEX_SYSTEM_PATH)
    FileUtils.delete(tempDir)
    super.afterAll()
  }

  protected def clearCache(): Unit = {
    Hyperspace.getContext(spark).indexCollectionManager match {
      case cachingManager: CachingIndexCollectionManager =>
        cachingManager.clearCache()
      case _ =>
    }
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f
    finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
   * Drops view `viewName` after calling `f`.
   */
  protected def withView(viewNames: String*)(f: => Unit): Unit = {
    try f
    finally {
      viewNames.foreach { name =>
        spark.sql(s"DROP VIEW IF EXISTS $name")
      }
    }
  }

  /**
   * Vacuum indexes with the given names after calling `f`.
   */
  protected def withIndex(indexNames: String*)(f: => Unit): Unit = {
    try f
    finally {
      val hs = new Hyperspace(spark)
      try {
        indexNames.foreach { name =>
          hs.deleteIndex(name)
          hs.vacuumIndex(name)
        }
      } catch {
        case e: Exception =>
          logError(s"Exception thrown during clean up: indexes = $indexNames, exception = $e")
      }
    }
  }

  protected def withTempPathAsString(f: String => Unit): Unit = {
    // The following is from SQLHelper.withTempPath with a modification to pass
    // String instead of File to "f". The reason this is copied instead of extending
    // SQLHelper is that some of the existing suites extend QueryTest and it causes
    // "inheriting conflicting members" issue.
    val path = Utils.createTempDir()
    path.delete()
    // Create an environment specific path string. Utils.createTempDir() returns `/path/to/file`
    // format, however some of APIs (e.g. Iceberg) cannot handle the path string properly
    // in Windows. Therefore, convert the path string to an environment specific one by
    // using `new Path`.
    val pathStr = new Path(path.toString).toString
    try f(pathStr)
    finally Utils.deleteRecursively(path)
  }

  def getExpectedResult(name: String): String = {
    org.apache.commons.io.FileUtils.readFileToString(
      new File(s"src/test/resources/expected/spark-${BuildInfo.sparkShortVersion}", name),
      StandardCharsets.UTF_8)
  }
}
