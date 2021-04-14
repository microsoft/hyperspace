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

    val defaultDisplayMode = new PlainTextMode(getHighlightConf("", ""))

    // Constructing expected output for given query from explain API
    val expectedOutput = new StringBuilder

    val joinIndexFilePath = getIndexFilesPath("joinIndex")
    val joinIndexPath = getIndexRootPath("joinIndex")

    // The format of the explain output looks as follows:
    // scalastyle:off filelinelengthchecker
    expectedOutput
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("Plan with indexes:")
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("SortMergeJoin [Col1#11], [Col1#21], Inner")
      .append(defaultDisplayMode.newLine)
      .append("<----:- *(1) Project [Col1#11, Col2#12]---->")
      .append(defaultDisplayMode.newLine)
      .append("<----:  +- *(1) Filter isnotnull(Col1#11)---->")
      .append(defaultDisplayMode.newLine)
      .append(s"<----:     +- *(1) FileScan Hyperspace(Type: CI, Name: joinIndex, LogVersion: 1) [Col1#11,Col2#12] Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[$joinIndexFilePath]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col1)], ReadSchema: struct<Col1:string,Col2:int>, SelectedBucketsCount: 200 out of 200---->")
      .append(defaultDisplayMode.newLine)
      .append("<----+- *(2) Project [Col1#21, Col2#22]---->")
      .append(defaultDisplayMode.newLine)
      .append("   <----+- *(2) Filter isnotnull(Col1#21)---->")
      .append(defaultDisplayMode.newLine)
      .append(s"      <----+- *(2) FileScan Hyperspace(Type: CI, Name: joinIndex, LogVersion: 1) [Col1#21,Col2#22] Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[$joinIndexFilePath]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col1)], ReadSchema: struct<Col1:string,Col2:int>, SelectedBucketsCount: 200 out of 200---->")
      .append(defaultDisplayMode.newLine)
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("Plan without indexes:")
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("SortMergeJoin [Col1#11], [Col1#21], Inner")
      .append(defaultDisplayMode.newLine)
      .append("<----:- *(2) Sort [Col1#11 ASC NULLS FIRST], false, 0---->")
      .append(defaultDisplayMode.newLine)
      .append("<----:  +- Exchange hashpartitioning(Col1#11, 5)---->")
      .append(defaultDisplayMode.newLine)
      .append("<----:     +- *(1) Project [Col1#11, Col2#12]---->")
      .append(defaultDisplayMode.newLine)
      .append("<----:        +- *(1) Filter isnotnull(Col1#11)---->")
      .append(defaultDisplayMode.newLine)
      .append(s"<----:           +- *(1) FileScan parquet [Col1#11,Col2#12] Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[$sampleParquetDataFullPath]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col1)], ReadSchema: struct<Col1:string,Col2:int>---->")
      .append(defaultDisplayMode.newLine)
      .append("<----+- *(4) Sort [Col1#21 ASC NULLS FIRST], false, 0---->")
      .append(defaultDisplayMode.newLine)
      .append("   <----+- ReusedExchange [Col1#21, Col2#22], Exchange hashpartitioning(Col1#11, 5)---->")
      .append(defaultDisplayMode.newLine)
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("Indexes used:")
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append(s"joinIndex:$joinIndexPath")
      .append(defaultDisplayMode.newLine)
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("Physical operator stats:")
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("+----------------------------------------------------------+-------------------+------------------+----------+")
      .append(defaultDisplayMode.newLine)
      .append("|                                         Physical Operator|Hyperspace Disabled|Hyperspace Enabled|Difference|")
      .append(defaultDisplayMode.newLine)
      .append("+----------------------------------------------------------+-------------------+------------------+----------+")
      .append(defaultDisplayMode.newLine)
      .append("|                                                   *Filter|                  1|                 2|         1|")
      .append(defaultDisplayMode.newLine)
      .append("|                                             *InputAdapter|                  4|                 2|        -2|")
      .append(defaultDisplayMode.newLine)
      .append("|                                                  *Project|                  1|                 2|         1|")
      .append(defaultDisplayMode.newLine)
      .append("|                                           *ReusedExchange|                  1|                 0|        -1|")
      .append(defaultDisplayMode.newLine)
      .append("|*Scan Hyperspace(Type: CI, Name: joinIndex, LogVersion: 1)|                  0|                 2|         2|")
      .append(defaultDisplayMode.newLine)
      .append("|                                             *Scan parquet|                  1|                 0|        -1|")
      .append(defaultDisplayMode.newLine)
      .append("|                                          *ShuffleExchange|                  1|                 0|        -1|")
      .append(defaultDisplayMode.newLine)
      .append("|                                                     *Sort|                  2|                 0|        -2|")
      .append(defaultDisplayMode.newLine)
      .append("|                                        *WholeStageCodegen|                  4|                 3|        -1|")
      .append(defaultDisplayMode.newLine)
      .append("|                                             SortMergeJoin|                  1|                 1|         0|")
      .append(defaultDisplayMode.newLine)
      .append("+----------------------------------------------------------+-------------------+------------------+----------+")
      .append(defaultDisplayMode.newLine)
      .append(defaultDisplayMode.newLine)
    // scalastyle:on filelinelengthchecker

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

    val displayMode = new PlainTextMode(getHighlightConf("<----", "---->"))
    // Constructing expected output for given query from explain API
    val expectedOutput = new StringBuilder

    // The format of the explain output looks as follows:
    // scalastyle:off filelinelengthchecker
    expectedOutput
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Plan with indexes:")
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Project [Col1#135]")
      .append(displayMode.newLine)
      .append("+- Filter (isnotnull(Col1#135) && (Col1#135 = Subquery subquery145))")
      .append(displayMode.newLine)
      .append("   :  +- Subquery subquery145")
      .append(displayMode.newLine)
      .append("   :     +- *(1) Project [Col1#135]")
      .append(displayMode.newLine)
      .append("   :        +- *(1) Filter (isnotnull(Col2#136) && (Col2#136 = 1))")
      .append(displayMode.newLine)
      .append("   <----:           +- *(1) FileScan Hyperspace(Type: CI, Name: filterIndex, LogVersion: 1) [Col2#136,Col1#135]")
      .append(" Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[${getIndexFilesPath("filterIndex")}]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col2), EqualTo(Col2,1)], ")
      .append("ReadSchema: struct<Col2:int,Col1:string>---->")
      .append(displayMode.newLine)
      .append("   +- FileScan parquet [Col1#135] Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[$sampleParquetDataFullPath]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col1)], ReadSchema: struct<Col1:string>")
      .append(displayMode.newLine)
      .append("         +- Subquery subquery145")
      .append(displayMode.newLine)
      .append("            +- *(1) Project [Col1#135]")
      .append(displayMode.newLine)
      .append("               +- *(1) Filter (isnotnull(Col2#136) && (Col2#136 = 1))")
      .append(displayMode.newLine)
      .append("                  <----+- *(1) FileScan Hyperspace(Type: CI, Name: filterIndex, LogVersion: 1) [Col2#136,Col1#135] " +
        "Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[${getIndexFilesPath("filterIndex")}]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col2), EqualTo(Col2,1)], ")
      .append("ReadSchema: struct<Col2:int,Col1:string>---->")
      .append(displayMode.newLine)
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Plan without indexes:")
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Project [Col1#135]")
      .append(displayMode.newLine)
      .append("+- Filter (isnotnull(Col1#135) && (Col1#135 = Subquery subquery145))")
      .append(displayMode.newLine)
      .append("   :  +- Subquery subquery145")
      .append(displayMode.newLine)
      .append("   :     +- *(1) Project [Col1#135]")
      .append(displayMode.newLine)
      .append("   :        +- *(1) Filter (isnotnull(Col2#136) && (Col2#136 = 1))")
      .append(displayMode.newLine)
      .append("   <----:           +- *(1) FileScan parquet [Col1#135,Col2#136] Batched: true, " +
        "Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[$sampleParquetDataFullPath]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col2), EqualTo(Col2,1)], ")
      .append("ReadSchema: struct<Col1:string,Col2:int>---->")
      .append(displayMode.newLine)
      .append("   +- FileScan parquet [Col1#135] Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[$sampleParquetDataFullPath]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col1)], ReadSchema: struct<Col1:string>")
      .append(displayMode.newLine)
      .append("         +- Subquery subquery145")
      .append(displayMode.newLine)
      .append("            +- *(1) Project [Col1#135]")
      .append(displayMode.newLine)
      .append("               +- *(1) Filter (isnotnull(Col2#136) && (Col2#136 = 1))")
      .append(displayMode.newLine)
      .append(
        "                  <----+- *(1) FileScan parquet [Col1#135,Col2#136] Batched: true, " +
          "Format: Parquet, Location: ")
      .append(truncate("InMemoryFileIndex[" + sampleParquetDataFullPath + "]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col2), EqualTo(Col2,1)], " +
        "ReadSchema: struct<Col1:string,Col2:int>---->")
      .append(displayMode.newLine)
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Indexes used:")
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("filterIndex:" + getIndexRootPath("filterIndex"))
      .append(displayMode.newLine)
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Physical operator stats:")
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("+-----------------+-------------------+------------------+----------+")
      .append(displayMode.newLine)
      .append("|Physical Operator|Hyperspace Disabled|Hyperspace Enabled|Difference|")
      .append(displayMode.newLine)
      .append("+-----------------+-------------------+------------------+----------+")
      .append(displayMode.newLine)
      .append("|           Filter|                  1|                 1|         0|")
      .append(displayMode.newLine)
      .append("|          Project|                  1|                 1|         0|")
      .append(displayMode.newLine)
      .append("|     Scan parquet|                  1|                 1|         0|")
      .append(displayMode.newLine)
      .append("|WholeStageCodegen|                  1|                 1|         0|")
      .append(displayMode.newLine)
      .append("+-----------------+-------------------+------------------+----------+")
      .append(displayMode.newLine)
      .append(displayMode.newLine)
    // scalastyle:on filelinelengthchecker

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

    val defaultDisplayMode = new PlainTextMode(getHighlightConf("", ""))

    // Constructing expected output for given query from explain API
    val expectedOutput = new StringBuilder

    // The format of the explain output looks as follows:
    val joinIndexFilePath = getIndexFilesPath("joinIndex")

    val joinIndexPath = getIndexRootPath("joinIndex")

    // scalastyle:off filelinelengthchecker
    expectedOutput
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("Plan with indexes:")
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("SortMergeJoin [Col1#11], [Col1#21], Inner")
      .append(defaultDisplayMode.newLine)
      .append("<----:- *(1) Project [Col1#11, Col2#12]---->")
      .append(defaultDisplayMode.newLine)
      .append("<----:  +- *(1) Filter isnotnull(Col1#11)---->")
      .append(defaultDisplayMode.newLine)
      .append(s"<----:     +- *(1) FileScan Hyperspace(Type: CI, Name: joinIndex, LogVersion: 1) [Col1#11,Col2#12] Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[$joinIndexFilePath]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col1)], ReadSchema: struct<Col1:string,Col2:int>, SelectedBucketsCount: 200 out of 200---->")
      .append(defaultDisplayMode.newLine)
      .append("<----+- *(2) Project [Col1#21, Col2#22]---->")
      .append(defaultDisplayMode.newLine)
      .append("   <----+- *(2) Filter isnotnull(Col1#21)---->")
      .append(defaultDisplayMode.newLine)
      .append(s"      <----+- *(2) FileScan Hyperspace(Type: CI, Name: joinIndex, LogVersion: 1) [Col1#21,Col2#22] Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[$joinIndexFilePath]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col1)], ReadSchema: struct<Col1:string,Col2:int>, SelectedBucketsCount: 200 out of 200---->")
      .append(defaultDisplayMode.newLine)
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("Plan without indexes:")
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("SortMergeJoin [Col1#11], [Col1#21], Inner")
      .append(defaultDisplayMode.newLine)
      .append("<----:- *(2) Sort [Col1#11 ASC NULLS FIRST], false, 0---->")
      .append(defaultDisplayMode.newLine)
      .append("<----:  +- Exchange hashpartitioning(Col1#11, 5)---->")
      .append(defaultDisplayMode.newLine)
      .append("<----:     +- *(1) Project [Col1#11, Col2#12]---->")
      .append(defaultDisplayMode.newLine)
      .append("<----:        +- *(1) Filter isnotnull(Col1#11)---->")
      .append(defaultDisplayMode.newLine)
      .append(s"<----:           +- *(1) FileScan parquet [Col1#11,Col2#12] Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[$sampleParquetDataFullPath]") +
        ", PartitionFilters: [], PushedFilters: [IsNotNull(Col1)], ReadSchema: struct<Col1:string,Col2:int>---->")
      .append(defaultDisplayMode.newLine)
      .append("<----+- *(4) Sort [Col1#21 ASC NULLS FIRST], false, 0---->")
      .append(defaultDisplayMode.newLine)
      .append("   <----+- ReusedExchange [Col1#21, Col2#22], Exchange hashpartitioning(Col1#11, 5)---->")
      .append(defaultDisplayMode.newLine)
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("Indexes used:")
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append(s"joinIndex:$joinIndexPath")
      .append(defaultDisplayMode.newLine)
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("Physical operator stats:")
      .append(defaultDisplayMode.newLine)
      .append("=============================================================")
      .append(defaultDisplayMode.newLine)
      .append("+----------------------------------------------------------+-------------------+------------------+----------+")
      .append(defaultDisplayMode.newLine)
      .append("|                                         Physical Operator|Hyperspace Disabled|Hyperspace Enabled|Difference|")
      .append(defaultDisplayMode.newLine)
      .append("+----------------------------------------------------------+-------------------+------------------+----------+")
      .append(defaultDisplayMode.newLine)
      .append("|                                                   *Filter|                  1|                 2|         1|")
      .append(defaultDisplayMode.newLine)
      .append("|                                             *InputAdapter|                  4|                 2|        -2|")
      .append(defaultDisplayMode.newLine)
      .append("|                                                  *Project|                  1|                 2|         1|")
      .append(defaultDisplayMode.newLine)
      .append("|                                           *ReusedExchange|                  1|                 0|        -1|")
      .append(defaultDisplayMode.newLine)
      .append("|*Scan Hyperspace(Type: CI, Name: joinIndex, LogVersion: 1)|                  0|                 2|         2|")
      .append(defaultDisplayMode.newLine)
      .append("|                                             *Scan parquet|                  1|                 0|        -1|")
      .append(defaultDisplayMode.newLine)
      .append("|                                          *ShuffleExchange|                  1|                 0|        -1|")
      .append(defaultDisplayMode.newLine)
      .append("|                                                     *Sort|                  2|                 0|        -2|")
      .append(defaultDisplayMode.newLine)
      .append("|                                        *WholeStageCodegen|                  4|                 3|        -1|")
      .append(defaultDisplayMode.newLine)
      .append("|                                             SortMergeJoin|                  1|                 1|         0|")
      .append(defaultDisplayMode.newLine)
      .append("+----------------------------------------------------------+-------------------+------------------+----------+")
      .append(defaultDisplayMode.newLine)
      .append(defaultDisplayMode.newLine)
    // scalastyle:on filelinelengthchecker

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

    val expectedOutput = new StringBuilder
    expectedOutput
      .append(displayMode.beginEndTag.open)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Plan with indexes:")
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Project [Col1#]")
      .append(displayMode.newLine)
      .append("+- Filter (isnotnull(Col2#) && (Col2# = 2))")
      .append(displayMode.newLine)
      .append("   " + displayMode.highlightTag.open)
      .append("+- FileScan Hyperspace(Type: CI, Name: filterIndex, LogVersion: 1) [Col2#,Col1#] ")
      .append("Batched: true, Format: Parquet, Location: " +
        truncate(s"InMemoryFileIndex[${getIndexFilesPath("filterIndex")}]"))
      .append(", PartitionFilters: [], PushedFilters: [IsNotNull(Col2), EqualTo(Col2,2)], ")
      .append("ReadSchema: struct<Col2:int,Col1:string>" + displayMode.highlightTag.close)
      .append(displayMode.newLine)
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Plan without indexes:")
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Project [Col1#]")
      .append(displayMode.newLine)
      .append("+- Filter (isnotnull(Col2#) && (Col2# = 2))")
      .append(displayMode.newLine)
      .append("   " + displayMode.highlightTag.open + "+- FileScan parquet [Col1#,Col2#] ")
      .append("Batched: true, Format: Parquet, Location: ")
      // Note: The below conversion converts relative path to absolute path for comparison.
      .append(truncate(s"InMemoryFileIndex[$sampleParquetDataFullPath]") + ", ")
      .append("PartitionFilters: [], PushedFilters: [IsNotNull(Col2), EqualTo(Col2,2)], ")
      .append("ReadSchema: struct<Col1:string,Col2:int>" + displayMode.highlightTag.close)
      .append(displayMode.newLine)
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("Indexes used:")
      .append(displayMode.newLine)
      .append("=============================================================")
      .append(displayMode.newLine)
      .append("filterIndex:")
      .append(getIndexRootPath("filterIndex"))
      .append(displayMode.newLine)
      .append(displayMode.newLine)
      .append(displayMode.beginEndTag.close)

    def filterQuery(query: DataFrame): DataFrame = {
      query.filter("Col2 == 2").select("Col1")
    }
    verifyExplainOutput(df, expectedOutput.toString, verbose = false) { filterQuery }
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
