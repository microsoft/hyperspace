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

import org.apache.commons.lang.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.iceberg.{PartitionSpec => IcebergPartitionSpec, Table, TableProperties}
import org.apache.iceberg.hadoop.HadoopTables
import org.apache.iceberg.spark.SparkSchemaUtil
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._
import scala.collection.JavaConverters._

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData, TestConfig}
import com.microsoft.hyperspace.TestUtils.latestIndexLogEntry
import com.microsoft.hyperspace.index.IndexConstants.REFRESH_MODE_QUICK
import com.microsoft.hyperspace.index.plananalysis.{PlainTextMode, PlanAnalyzer}
import com.microsoft.hyperspace.util.PathUtils
import com.microsoft.hyperspace.util.PathUtils.DataPathFilter

class IcebergIntegrationTest extends QueryTest with HyperspaceSuite {
  override val systemPath = PathUtils.makeAbsolute("src/test/resources/icebergIntegrationTest")

  private val sampleData = SampleData.testData
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      "spark.hyperspace.index.sources.fileBasedBuilders",
      "com.microsoft.hyperspace.index.sources.iceberg.IcebergFileBasedSourceBuilder," +
        "com.microsoft.hyperspace.index.sources.default.DefaultFileBasedSourceBuilder")
    hyperspace = new Hyperspace(spark)
  }

  override def afterAll(): Unit = {
    super.afterAll()
    spark.conf.unset("spark.hyperspace.index.sources.fileBasedBuilders")
  }

  before {
    spark.enableHyperspace()
  }

  after {
    spark.disableHyperspace()
  }

  test("Verify createIndex and refreshIndex on Iceberg table.") {
    withTempPathAsString { dataPath =>
      import spark.implicits._

      val indexConfig = IndexConfig("iceIndex", Seq("clicks"), Seq("Query"))
      val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")

      val iceTable = createIcebergTable(dataPath, dfFromSample)
      dfFromSample.write.format("iceberg").mode("overwrite").save(dataPath)

      val iceDf = spark.read.format("iceberg").load(dataPath)
      hyperspace.createIndex(iceDf, indexConfig)

      withIndex("iceIndex") {
        def query(version: Option[Long] = None): DataFrame = {
          if (version.isDefined) {
            val iceDf =
              spark.read.format("iceberg").option("snapshot-id", version.get).load(dataPath)
            iceDf.filter(iceDf("clicks") <= 2000).select(iceDf("query"))
          } else {
            val iceDf = spark.read.format("iceberg").load(dataPath)
            iceDf.filter(iceDf("clicks") <= 2000).select(iceDf("query"))
          }
        }

        assert(isIndexUsed(query().queryExecution.optimizedPlan, indexConfig.indexName))

        val currentIceSnapshot = iceTable.currentSnapshot()
        val fileToDelete =
          currentIceSnapshot.addedFiles().asScala.toSeq.map(_.path().toString).head

        // Create a new version by deleting entries.
        iceTable.newDelete().deleteFile(fileToDelete).commit()

        // The index should not be applied for the updated version.
        assert(!isIndexUsed(query().queryExecution.optimizedPlan, indexConfig.indexName))

        // The index should be applied for the version at index creation.
        assert(
          isIndexUsed(
            query(Some(currentIceSnapshot.snapshotId())).queryExecution.optimizedPlan,
            indexConfig.indexName))

        hyperspace.refreshIndex(indexConfig.indexName)

        // The index should be applied for the updated version.
        assert(isIndexUsed(query().queryExecution.optimizedPlan, "iceIndex/v__=1"))

        // The index should not be applied for the version at index creation.
        assert(
          !isIndexUsed(
            query(Some(currentIceSnapshot.snapshotId())).queryExecution.optimizedPlan,
            indexConfig.indexName))
      }
    }
  }

  test("Verify incremental refresh index properly adds hive-partition columns.") {
    withTempPathAsString { testPath =>
      val absoluteTestPath = PathUtils.makeAbsolute(testPath).toString
      import spark.implicits._
      val (testData, appendData) = SampleData.testData.partition(_._1 != "2019-10-03")
      assert(testData.nonEmpty)
      assert(appendData.nonEmpty)

      val dfFromSample = testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")

      createIcebergTableWithDayPartition(testPath, dfFromSample)

      dfFromSample.write
        .format("iceberg")
        .mode("overwrite")
        .save(testPath)

      val df = spark.read.format("iceberg").load(testPath)
      hyperspace.createIndex(df, IndexConfig("index", Seq("clicks"), Seq("Date")))

      withIndex("index") {
        // Check if partition columns are correctly stored in index contents.
        val indexDf1 = spark.read.parquet(s"$systemPath/index").where("Date != '2019-10-03'")
        assert(testData.size == indexDf1.count())
        val oldEntry = latestIndexLogEntry(systemPath, "index")
        assert(oldEntry.relations.head.options("basePath").equals(absoluteTestPath))

        // Append data creating new partition and refresh index.
        appendData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .write
          .mode("append")
          .format("iceberg")
          .save(testPath)
        hyperspace.refreshIndex("index", "incremental")

        // Check if partition columns are correctly stored in index contents.
        val indexDf2 = spark.read.parquet(s"$systemPath/index").where("Date = '2019-10-03'")
        assert(appendData.size == indexDf2.count())
        val newEntry = latestIndexLogEntry(systemPath, "index")
        assert(newEntry.relations.head.options("basePath").equals(absoluteTestPath))
      }
    }
  }

  test(
    "Verify JoinIndexRule utilizes indexes correctly after quick refresh when some file " +
      "gets deleted and some appended to source data.") {
    withTempPathAsString { testPath =>
      import spark.implicits._

      val indexConfig = IndexConfig("index", Seq("c2"), Seq("c4"))
      val dfFromSample = SampleData.testData.toDF("c1", "c2", "c3", "c4", "c5").limit(10)

      val iceTable = createIcebergTable(testPath, dfFromSample)
      dfFromSample.write.format("iceberg").mode("overwrite").save(testPath)

      val df = spark.read.format("iceberg").load(testPath)
      val oldFiles = iceTable.currentSnapshot().addedFiles().asScala.toSeq.map(_.path().toString)

      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        hyperspace.createIndex(df, indexConfig)
      }

      withIndex(indexConfig.indexName) {
        // Create a new version by deleting a file.
        iceTable.newDelete().deleteFile(oldFiles.head).commit()

        // Append to original data.
        SampleData.testData
          .toDF("c1", "c2", "c3", "c4", "c5")
          .limit(3)
          .write
          .format("iceberg")
          .mode("append")
          .save(testPath)

        // Refresh index.
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_QUICK)

        {
          // Create a join query.
          val leftDf = spark.read.format("iceberg").load(testPath)
          val rightDf = spark.read.format("iceberg").load(testPath)

          def query(): DataFrame = {
            leftDf
              .join(rightDf, leftDf("c2") === rightDf("c2"))
              .select(leftDf("c2"), rightDf("c4"))
          }

          val appendedFiles = leftDf.inputFiles.diff(oldFiles)

          // Verify indexes are used, and all index files are picked.
          isIndexUsed(
            query().queryExecution.optimizedPlan,
            s"${indexConfig.indexName}/v__=0" +: appendedFiles: _*)

          // Verify correctness of results.
          spark.disableHyperspace()
          val dfWithHyperspaceDisabled = query()
          spark.enableHyperspace()
          val dfWithHyperspaceEnabled = query()
          checkAnswer(dfWithHyperspaceDisabled, dfWithHyperspaceEnabled)
        }

        // Append to original data again.
        SampleData.testData
          .toDF("c1", "c2", "c3", "c4", "c5")
          .limit(1)
          .write
          .mode("append")
          .format("iceberg")
          .save(testPath)

        {
          // Create a join query with updated dataset.
          val leftDf = spark.read.format("iceberg").load(testPath)
          val rightDf = spark.read.format("iceberg").load(testPath)

          def query(): DataFrame = {
            leftDf
              .join(rightDf, leftDf("c2") === rightDf("c2"))
              .select(leftDf("c2"), rightDf("c4"))
          }

          // Refreshed index as quick mode won't be applied with additional appended files.
          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
            spark.disableHyperspace()
            val dfWithHyperspaceDisabled = query()
            val basePlan = dfWithHyperspaceDisabled.queryExecution.optimizedPlan
            spark.enableHyperspace()
            val dfWithHyperspaceEnabled = query()
            assert(basePlan.equals(dfWithHyperspaceEnabled.queryExecution.optimizedPlan))
          }

          // Refreshed index as quick mode can be applied with Hybrid Scan config.
          withSQLConf(TestConfig.HybridScanEnabled: _*) {
            spark.disableHyperspace()
            val dfWithHyperspaceDisabled = query()
            val basePlan = dfWithHyperspaceDisabled.queryExecution.optimizedPlan
            spark.enableHyperspace()
            val dfWithHyperspaceEnabled = query()
            assert(!basePlan.equals(dfWithHyperspaceEnabled.queryExecution.optimizedPlan))
            checkAnswer(dfWithHyperspaceDisabled, dfWithHyperspaceEnabled)
          }
        }
      }
    }
  }

  test("Verify Hybrid Scan on Iceberg table.") {
    withTempPathAsString { dataPath =>
      import spark.implicits._
      val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")

      val iceTable = createIcebergTable(dataPath, dfFromSample)
      dfFromSample.write.format("iceberg").mode("overwrite").save(dataPath)

      val iceDf = spark.read.format("iceberg").load(dataPath)
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        hyperspace.createIndex(iceDf, IndexConfig("iceIndex", Seq("clicks"), Seq("Query")))
      }

      withIndex("iceIndex") {
        def query(version: Option[Long] = None): DataFrame = {
          if (version.isDefined) {
            val iceDf =
              spark.read.format("iceberg").option("snapshot-id", version.get).load(dataPath)
            iceDf.filter(iceDf("clicks") <= 2000).select(iceDf("query"))
          } else {
            val iceDf = spark.read.format("iceberg").load(dataPath)
            iceDf.filter(iceDf("clicks") <= 2000).select(iceDf("query"))
          }
        }

        assert(isIndexUsed(query().queryExecution.optimizedPlan, "iceIndex"))

        // Create a new version by deleting a file.
        val file = iceTable
          .currentSnapshot()
          .addedFiles()
          .iterator()
          .asScala
          .toSeq
          .map(_.path().toString)
          .head
        iceTable.newDelete().deleteFile(file).commit()

        withSQLConf(TestConfig.HybridScanEnabled: _*) {
          // The index should be applied for the updated version.
          assert(isIndexUsed(query().queryExecution.optimizedPlan, "iceIndex"))

          // Append data.
          dfFromSample
            .limit(3)
            .write
            .format("iceberg")
            .mode("append")
            .save(dataPath)

          iceTable.refresh()
          val appendedFiles = iceTable
            .currentSnapshot()
            .addedFiles()
            .asScala
            .toSeq
            .map(_.path().toString)

          // The index should be applied for the updated version.
          assert(
            isIndexUsed(query().queryExecution.optimizedPlan, "iceIndex" +: appendedFiles: _*))
        }
      }
    }
  }

  test("Explain is correct") {
    withTempPathAsString { dataPath =>
      spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
      spark.disableHyperspace()

      import spark.implicits._

      val sampleData = Seq(("data1", 1), ("data2", 2), ("data3", 3))
      val dfFromSample = sampleData.toDF("Col1", "Col2")

      createIcebergTable(dataPath, dfFromSample)

      dfFromSample.write.format("iceberg").mode("overwrite").save(dataPath)

      val iceDf = spark.read.format("iceberg").load(dataPath)

      val indexConfig = IndexConfig("joinIndex", Seq("Col1"), Seq("Col2"))
      hyperspace.createIndex(iceDf, indexConfig)

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
        .append("<----:- *(1) Sort [Col1#11 ASC NULLS FIRST], false, 0---->")
        .append(defaultDisplayMode.newLine)
        .append("<----:  +- *(1) Project [Col1#1, Col2#2]---->")
        .append(defaultDisplayMode.newLine)
        .append("<----:     +- *(1) Filter isnotnull(Col1#1)---->")
        .append(defaultDisplayMode.newLine)
        .append("<----:        +- *(1) ScanV2 iceberg[Col1#, Col2#] (Filters: [isnotnull(Col1#)], Options: " +
            truncate(s"[path=$dataPath,paths=[]]") + ")---->")
        .append(defaultDisplayMode.newLine)
        .append("<----+- *(2) Sort [Col1#21 ASC NULLS FIRST], false, 0---->")
        .append(defaultDisplayMode.newLine)
        .append("   <----+- *(2) Project [Col1#, Col2#]---->")
        .append(defaultDisplayMode.newLine)
        .append("      <----+- *(2) Filter isnotnull(Col1#)---->")
        .append(defaultDisplayMode.newLine)
        .append("         <----+- *(2) ScanV2 iceberg[Col1#, Col2#] (Filters: [isnotnull(Col1#)], Options: " +
            truncate(s"[path=$dataPath,paths=[]]") + ")---->")
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
        .append("|                                         *DataSourceV2Scan|                  2|                 0|        -2|")
        .append(defaultDisplayMode.newLine)
        .append("|*Scan Hyperspace(Type: CI, Name: joinIndex, LogVersion: 1)|                  0|                 2|         2|")
        .append(defaultDisplayMode.newLine)
        .append("|                                                     *Sort|                  2|                 0|        -2|")
        .append(defaultDisplayMode.newLine)
        .append("|                                                    Filter|                  2|                 2|         0|")
        .append(defaultDisplayMode.newLine)
        .append("|                                              InputAdapter|                  2|                 2|         0|")
        .append(defaultDisplayMode.newLine)
        .append("|                                                   Project|                  2|                 2|         0|")
        .append(defaultDisplayMode.newLine)
        .append("|                                             SortMergeJoin|                  1|                 1|         0|")
        .append(defaultDisplayMode.newLine)
        .append("|                                         WholeStageCodegen|                  3|                 3|         0|")
        .append(defaultDisplayMode.newLine)
        .append("+----------------------------------------------------------+-------------------+------------------+----------+")
        .append(defaultDisplayMode.newLine)
        .append(defaultDisplayMode.newLine)
      // scalastyle:on filelinelengthchecker

      val selfJoinDf = iceDf.join(iceDf, iceDf("Col1") === iceDf("Col1"))
      verifyExplainOutput(selfJoinDf, expectedOutput.toString(), verbose = true) { df =>
        df
      }
    }
  }

  def isIndexUsed(plan: LogicalPlan, expectedPathsSubStr: String*): Boolean = {
    val rootPaths = plan.collect {
      case LogicalRelation(
          HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        location.rootPaths
    }.flatten
    rootPaths.nonEmpty && rootPaths.forall(p =>
      expectedPathsSubStr.exists(p.toString.contains(_))) && expectedPathsSubStr.forall(p =>
      rootPaths.exists(_.toString.contains(p)))
  }

  def createIcebergTable(dataPath: String, sourceDf: DataFrame): Table = {
    val props = Map(TableProperties.WRITE_NEW_DATA_LOCATION -> dataPath).asJava
    val schema = SparkSchemaUtil.convert(sourceDf.schema)
    val part = IcebergPartitionSpec.builderFor(schema).build()
    new HadoopTables().create(schema, part, props, dataPath)
  }

  def createIcebergTableWithDayPartition(dataPath: String, sourceDf: DataFrame): Table = {
    val props = Map(TableProperties.WRITE_NEW_DATA_LOCATION -> dataPath).asJava
    val schema = SparkSchemaUtil.convert(sourceDf.schema)
    val part = IcebergPartitionSpec.builderFor(schema).identity("Date").build()
    new HadoopTables().create(schema, part, props, dataPath)
  }

  private def truncate(s: String): String = {
    StringUtils.abbreviate(s, 100)
  }

  private def getHighlightConf(
      highlightBegin: String,
      highlightEnd: String): Map[String, String] = {
    Map[String, String](
      IndexConstants.HIGHLIGHT_BEGIN_TAG -> highlightBegin,
      IndexConstants.HIGHLIGHT_END_TAG -> highlightEnd)
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
}
