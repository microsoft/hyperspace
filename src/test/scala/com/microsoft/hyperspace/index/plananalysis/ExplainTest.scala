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

package com.microsoft.hyperspace.index.plananalysis

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.{Hyperspace, Implicits}
import com.microsoft.hyperspace.index.{HyperspaceSuite, IndexConfig, IndexConstants}
import com.microsoft.hyperspace.util.PathUtils
import com.microsoft.hyperspace.util.PathUtils.DataPathFilter

class ExplainTest extends SparkFunSuite with HyperspaceSuite {
  private val sampleParquetDataLocation = inTempDir("sampleparquet")
  private val fileSystem = new Path(sampleParquetDataLocation).getFileSystem(new Configuration)
  private var sampleParquetDataFullPath: String = ""
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sparkSession = spark
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    spark.conf.set("spark.sql.legacy.bucketedTableScan.outputOrdering", true) // For Spark 3.0

    import sparkSession.implicits._
    hyperspace = new Hyperspace(sparkSession)
    fileSystem.delete(new Path(sampleParquetDataLocation), true)
    val sampleData = Seq(("data1", 1), ("data2", 2), ("data3", 3))
    val dfFromSample = sampleData.toDF("Col1", "Col2")
    dfFromSample.write.parquet(sampleParquetDataLocation)
    sampleParquetDataFullPath = PathUtils.makeAbsolute(sampleParquetDataLocation).toString
  }

  override def afterAll(): Unit = {
    fileSystem.delete(new Path(sampleParquetDataLocation), true)
    super.afterAll()
  }

  after {
    clearCache()
    fileSystem.delete(systemPath, true)
    spark.conf.unset(IndexConstants.DISPLAY_MODE)
    spark.conf.unset(IndexConstants.HIGHLIGHT_BEGIN_TAG)
    spark.conf.unset(IndexConstants.HIGHLIGHT_END_TAG)
    spark.disableHyperspace()
  }

  test("Testing default display mode") {
    val df = spark.read.parquet(sampleParquetDataLocation)
    val indexConfig = IndexConfig("joinIndex", Seq("Col1"), Seq("Col2"))
    hyperspace.createIndex(df, indexConfig)

    val joinIndexFilePath = getIndexFilesPath("joinIndex")
    val joinIndexPath = getIndexRootPath("joinIndex")

    val expectedOutput = getExpectedResult("selfJoin.txt")
      .replace("$joinIndexLocation", truncate(s"InMemoryFileIndex[$joinIndexFilePath]"))
      .replace("$joinIndexPath", joinIndexPath.toString)
      .replace(
        "$sampleParquetDataLocation",
        truncate(s"InMemoryFileIndex[$sampleParquetDataFullPath]"))

    val selfJoinDf = df.join(df, df("Col1") === df("Col1"))
    verifyExplainOutput(selfJoinDf, expectedOutput.toString(), verbose = true) { df =>
      df
    }
  }

  test("Testing subquery scenario") {
    val df = spark.read.parquet(sampleParquetDataLocation)
    val indexConfig =
      IndexConfig("filterIndex", Seq("Col2"), Seq("Col1"))
    df.createOrReplaceTempView("query")
    hyperspace.createIndex(df, indexConfig)

    val filterIndexFilePath = getIndexFilesPath("filterIndex")
    val filterIndexPath = getIndexRootPath("filterIndex")

    val expectedOutput = getExpectedResult("subquery.txt")
      .replace("$filterIndexLocation", truncate(s"InMemoryFileIndex[$filterIndexFilePath]"))
      .replace("$filterIndexPath", filterIndexPath.toString)
      .replace(
        "$sampleParquetDataLocation",
        truncate(s"InMemoryFileIndex[$sampleParquetDataFullPath]"))

    val dfSubquery =
      spark.sql("""select Col1 from query where
          |Col1 == (select Col1 from query where Col2==1)""".stripMargin)
    verifyExplainOutput(dfSubquery, expectedOutput.toString(), verbose = true) { df =>
      df
    }
  }

  test("Testing plaintext mode") {
    spark.conf.set(IndexConstants.DISPLAY_MODE, IndexConstants.DisplayMode.PLAIN_TEXT)
    val displayMode = new PlainTextMode(getHighlightConf("", ""))
    testDifferentMode(displayMode)
  }

  test("Testing HTML mode") {
    spark.conf.set(IndexConstants.DISPLAY_MODE, IndexConstants.DisplayMode.HTML)
    val highlightBegin = """<b style="background: #ff9900">"""
    val highlightEnd = """</b>"""
    spark.conf.set(IndexConstants.HIGHLIGHT_BEGIN_TAG, highlightBegin)
    spark.conf.set(IndexConstants.HIGHLIGHT_END_TAG, highlightEnd)
    val displayMode = new HTMLMode(getHighlightConf(highlightBegin, highlightEnd))
    testDifferentMode(displayMode)
  }

  test("Testing console mode") {
    spark.conf.set(IndexConstants.DISPLAY_MODE, IndexConstants.DisplayMode.CONSOLE)
    val displayMode = new ConsoleMode(getHighlightConf("", ""))
    testDifferentMode(displayMode)
  }

  test("Testing default display mode when optimized plan is materialized") {
    val df = spark.read.parquet(sampleParquetDataLocation)
    val indexConfig = IndexConfig("joinIndex", Seq("Col1"), Seq("Col2"))
    hyperspace.createIndex(df, indexConfig)

    // The format of the explain output looks as follows:
    val joinIndexFilePath = getIndexFilesPath("joinIndex")
    val joinIndexPath = getIndexRootPath("joinIndex")

    val expectedOutput = getExpectedResult("selfJoin.txt")
      .replace("$joinIndexLocation", truncate(s"InMemoryFileIndex[$joinIndexFilePath]"))
      .replace("$joinIndexPath", joinIndexPath.toString)
      .replace(
        "$sampleParquetDataLocation",
        truncate(s"InMemoryFileIndex[$sampleParquetDataFullPath]"))

    val selfJoinDf = df.join(df, df("Col1") === df("Col1"))

    // Materialize the lazily evaluated optimized plan before plan comparison.
    spark.enableHyperspace()
    selfJoinDf.queryExecution.optimizedPlan
    spark.disableHyperspace()

    verifyExplainOutput(selfJoinDf, expectedOutput.toString(), verbose = true) { df =>
      df
    }
  }

  private def testDifferentMode(displayMode: DisplayMode): Unit = {
    val df = spark.read.parquet(sampleParquetDataLocation)
    val indexConfig = IndexConfig("filterIndex", Seq("Col2"), Seq("Col1"))
    hyperspace.createIndex(df, indexConfig)

    val filterIndexFilePath = getIndexFilesPath("filterIndex")
    val filterIndexPath = getIndexRootPath("filterIndex")

    val expectedOutput = getExpectedResult("filter.txt")
      .replace("$filterIndexLocation", truncate(s"InMemoryFileIndex[$filterIndexFilePath]"))
      .replace("$filterIndexPath", filterIndexPath.toString)
      .replace(
        "$sampleParquetDataLocation",
        truncate(s"InMemoryFileIndex[$sampleParquetDataFullPath]"))
      .replace("$begin", displayMode.beginEndTag.open)
      .replace("$end", displayMode.beginEndTag.close)
      .replace("$highlightBegin", displayMode.highlightTag.open)
      .replace("$highlightEnd", displayMode.highlightTag.close)
      .replace(System.lineSeparator(), displayMode.newLine)

    def filterQuery(query: DataFrame): DataFrame = {
      query.filter("Col2 == 2").select("Col1")
    }
    verifyExplainOutput(df, expectedOutput, verbose = false) { filterQuery }
  }

  private def getIndexRootPath(indexName: String): Path =
    new Path(systemPath, s"$indexName/v__=0")

  private def getIndexFilesPath(indexName: String): Path = {
    val path = getIndexRootPath(indexName)
    val fs = path.getFileSystem(new Configuration)
    // Pick any files path but remove the _SUCCESS file.
    fs.listStatus(path).filter(s => DataPathFilter.accept(s.getPath)).head.getPath
  }

  private def verifyExplainOutput(df: DataFrame, expected: String, verbose: Boolean)(
      query: DataFrame => DataFrame) {
    def normalize(str: String): String = {
      // Expression ids are removed before comparison since they can be different for each run.
      str.replaceAll("""#(\d+)|subquery(\d+)""", "#")
    }

    val dfWithHyperspaceDisabled = query(df)
    val actual1 =
      PlanAnalyzer.explainString(dfWithHyperspaceDisabled, spark, hyperspace.indexes, verbose)
    assert(!spark.isHyperspaceEnabled())
    assert(normalize(actual1) === normalize(expected))

    // Run with Hyperspace enabled and it shouldn't affect the result of `explainString`.
    spark.enableHyperspace()
    val dfWithHyperspaceEnabled = query(df)
    val actual2 =
      PlanAnalyzer.explainString(dfWithHyperspaceEnabled, spark, hyperspace.indexes, verbose)
    assert(spark.isHyperspaceEnabled())
    assert(normalize(actual2) === normalize(expected))
  }

  private def getHighlightConf(
      highlightBegin: String,
      highlightEnd: String): Map[String, String] = {
    Map[String, String](
      IndexConstants.HIGHLIGHT_BEGIN_TAG -> highlightBegin,
      IndexConstants.HIGHLIGHT_END_TAG -> highlightEnd)
  }

  /**
   * Helper method to truncate long string.
   * Note: This method truncates long InMemoryFileIndex string to get the similar explainString for
   * comparing with Hyperspace's explain API output. For reference, the similar truncation logic for
   * InMemoryFileIndex string is in spark code base in DataSourceScanExec.scala in simpleString
   * method.
   *
   * @param s long string.
   * @return truncated string.
   */
  private def truncate(s: String): String = {
    StringUtils.abbreviate(s, 100)
  }
}
