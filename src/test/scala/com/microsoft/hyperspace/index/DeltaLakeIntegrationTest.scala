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

import java.sql.Timestamp

import scala.collection.mutable

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources._

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData, TestConfig}
import com.microsoft.hyperspace.TestUtils.latestIndexLogEntry
import com.microsoft.hyperspace.index.IndexConstants.REFRESH_MODE_QUICK
import com.microsoft.hyperspace.index.plans.logical.IndexHadoopFsRelation
import com.microsoft.hyperspace.util.PathUtils

class DeltaLakeIntegrationTest extends QueryTest with HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/deltaLakeIntegrationTest")

  private val sampleData = SampleData.testData
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      "spark.hyperspace.index.sources.fileBasedBuilders",
      "com.microsoft.hyperspace.index.sources.delta.DeltaLakeFileBasedSourceBuilder," +
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

  test("Verify createIndex and refreshIndex on Delta Lake table.") {
    withTempPathAsString { dataPath =>
      import spark.implicits._
      val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
      dfFromSample.write.format("delta").save(dataPath)

      val deltaDf = spark.read.format("delta").load(dataPath)
      hyperspace.createIndex(deltaDf, IndexConfig("deltaIndex", Seq("clicks"), Seq("Query")))

      withIndex("deltaIndex") {
        def query(version: Option[Long] = None): DataFrame = {
          if (version.isDefined) {
            val deltaDf =
              spark.read.format("delta").option("versionAsOf", version.get).load(dataPath)
            deltaDf.filter(deltaDf("clicks") <= 2000).select(deltaDf("query"))
          } else {
            val deltaDf = spark.read.format("delta").load(dataPath)
            deltaDf.filter(deltaDf("clicks") <= 2000).select(deltaDf("query"))
          }
        }

        assert(isIndexUsed(query().queryExecution.optimizedPlan, "deltaIndex"))

        // Create a new version by deleting entries.
        val deltaTable = DeltaTable.forPath(dataPath)
        deltaTable.delete("clicks > 2000")

        // The index should not be applied for the updated version.
        assert(!isIndexUsed(query().queryExecution.optimizedPlan, "deltaIndex"))

        // The index should be applied for the version at index creation.
        assert(isIndexUsed(query(Some(0)).queryExecution.optimizedPlan, "deltaIndex"))

        hyperspace.refreshIndex("deltaIndex")

        // The index should be applied for the updated version.
        assert(isIndexUsed(query().queryExecution.optimizedPlan, "deltaIndex/v__=1"))

        // The index should not be applied for the version at index creation.
        assert(!isIndexUsed(query(Some(0)).queryExecution.optimizedPlan, "deltaIndex"))

        withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
          // The index should be applied for the updated version.
          assert(isIndexUsed(query(Some(0)).queryExecution.optimizedPlan, "deltaIndex/v__=0"))
        }
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
      testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .write
        .partitionBy("Date")
        .format("delta")
        .save(testPath)

      val df = spark.read.format("delta").load(testPath)
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
          .partitionBy("Date")
          .mode("append")
          .format("delta")
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
      // Setup. Create data.
      val indexConfig = IndexConfig("index", Seq("c2"), Seq("c4"))
      import spark.implicits._
      SampleData.testData
        .toDF("c1", "c2", "c3", "c4", "c5")
        .limit(10)
        .write
        .format("delta")
        .save(testPath)

      val df = spark.read.format("delta").load(testPath)
      val oldFiles = df.inputFiles

      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        // Create index.
        hyperspace.createIndex(df, indexConfig)
      }

      withIndex("index") {
        // Create a new version by deleting entries.
        val deltaTable = DeltaTable.forPath(testPath)
        deltaTable.delete("c1 = 2019-10-03")

        // Append to original data.
        SampleData.testData
          .toDF("c1", "c2", "c3", "c4", "c5")
          .limit(3)
          .write
          .format("delta")
          .mode("append")
          .save(testPath)

        // Refresh index.
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_QUICK)

        {
          // Create a join query.
          val leftDf = spark.read.format("delta").load(testPath)
          val rightDf = spark.read.format("delta").load(testPath)

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
          .format("delta")
          .save(testPath)

        {
          // Create a join query with updated dataset.
          val leftDf = spark.read.format("delta").load(testPath)
          val rightDf = spark.read.format("delta").load(testPath)

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

  test("Verify Hybrid Scan on Delta Lake table.") {
    withTempPathAsString { dataPath =>
      import spark.implicits._
      val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
      dfFromSample.write.format("delta").save(dataPath)

      val deltaDf = spark.read.format("delta").load(dataPath)
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        hyperspace.createIndex(deltaDf, IndexConfig("deltaIndex", Seq("clicks"), Seq("Query")))
      }

      withIndex("deltaIndex") {
        def query(version: Option[Long] = None): DataFrame = {
          if (version.isDefined) {
            val deltaDf =
              spark.read.format("delta").option("versionAsOf", version.get).load(dataPath)
            deltaDf.filter(deltaDf("clicks") <= 2000).select(deltaDf("query"))
          } else {
            val deltaDf = spark.read.format("delta").load(dataPath)
            deltaDf.filter(deltaDf("clicks") <= 2000).select(deltaDf("query"))
          }
        }

        assert(isIndexUsed(query().queryExecution.optimizedPlan, "deltaIndex"))

        // Create a new version by deleting entries.
        val deltaTable = DeltaTable.forPath(dataPath)
        deltaTable.delete("clicks > 5000")

        withSQLConf(TestConfig.HybridScanEnabled: _*) {
          // The index should be applied for the updated version.
          assert(isIndexUsed(query().queryExecution.optimizedPlan, "deltaIndex"))
          val prevInputFiles = spark.read.format("delta").load(dataPath).inputFiles

          // Append data.
          dfFromSample
            .limit(3)
            .write
            .format("delta")
            .mode("append")
            .save(dataPath)

          val appendedFiles = spark.read
            .format("delta")
            .load(dataPath)
            .inputFiles
            .toSet -- prevInputFiles

          // The index should be applied for the updated version.
          assert(
            isIndexUsed(
              query().queryExecution.optimizedPlan,
              "deltaIndex" +: appendedFiles.toSeq: _*))
        }
      }
    }
  }

  test("Verify time travel query picks the closest index version for Hybrid Scan.") {
    withTempPathAsString { dataPath =>
      import spark.implicits._
      val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
      dfFromSample.write.format("delta").save(dataPath)

      val timestampMap = mutable.Map[Long, String]()
      timestampMap.put(0, getSparkFormattedTimestamps(System.currentTimeMillis).head)

      val deltaDf = spark.read.format("delta").load(dataPath)
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        hyperspace.createIndex(deltaDf, IndexConfig("deltaIndex", Seq("clicks"), Seq("Query")))
      }

      withIndex("deltaIndex") {
        def query(version: Option[Long] = None, asTimestamp: Boolean = false): DataFrame = {
          val deltaDf = if (version.isDefined) {
            if (asTimestamp) {
              spark.read
                .format("delta")
                .option("timestampAsOf", timestampMap(version.get))
                .load(dataPath)
            } else {
              spark.read.format("delta").option("versionAsOf", version.get).load(dataPath)
            }
          } else {
            spark.read.format("delta").load(dataPath)
          }
          deltaDf.filter(deltaDf("clicks") <= 2000).select(deltaDf("query"))
        }

        def checkExpectedIndexUsed(
            timeTravelVersion: Option[Long],
            expectedIndexPathSubStr: String*): Unit = {
          assert(
            isIndexUsed(
              query(timeTravelVersion, asTimestamp = false).queryExecution.optimizedPlan,
              expectedIndexPathSubStr: _*),
            s"timeTravelVer: $timeTravelVersion, expectedPaths: $expectedIndexPathSubStr)")
          if (timeTravelVersion.isDefined) {
            // Check time travel query with timestampAsOf instead of versionAsOf.
            val df = spark.read.format("delta").load(dataPath)
            val latestVer = getDeltaLakeTableVersion(df)
            // Delta Lake throws an exception if the timestamp is larger than the latest commit.
            if (latestVer != timeTravelVersion.get) {
              assert(
                isIndexUsed(
                  query(timeTravelVersion, asTimestamp = true).queryExecution.optimizedPlan,
                  expectedIndexPathSubStr: _*),
                s"timeTravelVer: $timeTravelVersion, expectedPaths: $expectedIndexPathSubStr)")
            }
          }
        }

        checkExpectedIndexUsed(None, "deltaIndex/v__=0")

        def appendData(doRefresh: Boolean): Unit = {
          // Append data.
          dfFromSample
            .limit(3)
            .write
            .format("delta")
            .mode("append")
            .save(dataPath)

          // Get the timestamp string for time travel query using "timestampAsOf" option.
          val timestampStr = getSparkFormattedTimestamps(System.currentTimeMillis).head
          // Sleep 1 second because the unit of commit timestamp is second.
          Thread.sleep(1000)

          if (doRefresh) {
            hyperspace.refreshIndex("deltaIndex", mode = "full")
          }

          val newDf = spark.read.format("delta").load(dataPath)
          timestampMap.put(getDeltaLakeTableVersion(newDf), timestampStr)
        }

        withSQLConf(TestConfig.HybridScanEnabled: _*) {
          appendData(doRefresh = false) // delta version 1
          appendData(doRefresh = true) // delta version 2

          // The index should be applied for the updated version.
          checkExpectedIndexUsed(None, "deltaIndex/v__=1")
          checkExpectedIndexUsed(Some(0), "deltaIndex/v__=0")

          appendData(doRefresh = false) // delta version 3
          appendData(doRefresh = false) // delta version 4
          appendData(doRefresh = false) // delta version 5
          appendData(doRefresh = true) // delta version 6
          appendData(doRefresh = false) // delta version 7
          appendData(doRefresh = true) // delta version 8

          val dataPathStr = new Path(dataPath).toString

          checkExpectedIndexUsed(None, "deltaIndex/v__=3")
          checkExpectedIndexUsed(Some(1), "deltaIndex/v__=1")
          checkExpectedIndexUsed(Some(3), "deltaIndex/v__=1", dataPathStr)
          checkExpectedIndexUsed(Some(5), "deltaIndex/v__=2")
          checkExpectedIndexUsed(Some(6), "deltaIndex/v__=2")
          checkExpectedIndexUsed(Some(7), "deltaIndex/v__=3")
        }
      }
    }
  }

  test("Verify time travel query works well with incremental refresh & optimizeIndex.") {
    withTempPathAsString { dataPath =>
      import spark.implicits._
      val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
      dfFromSample.write.format("delta").save(dataPath)

      val timestampMap = mutable.Map[Long, String]()
      timestampMap.put(0, getSparkFormattedTimestamps(System.currentTimeMillis).head)

      val indexName = "deltaIndex2"
      val deltaDf = spark.read.format("delta").load(dataPath)

      def appendData(doRefresh: Boolean): Unit = {
        // Append data.
        dfFromSample
          .limit(3)
          .write
          .format("delta")
          .mode("append")
          .save(dataPath)

        // Get the timestamp string for time travel query using "timestampAsOf" option.
        val timestampStr = getSparkFormattedTimestamps(System.currentTimeMillis).head
        // Sleep 1 second because the unit of commit timestamp is second.
        Thread.sleep(1000)

        if (doRefresh) {
          hyperspace.refreshIndex(indexName, mode = "incremental")
        }

        val newDf = spark.read.format("delta").load(dataPath)
        timestampMap.put(getDeltaLakeTableVersion(newDf), timestampStr)
      }

      def query(version: Option[Long] = None, asTimestamp: Boolean = false): DataFrame = {
        val deltaDf = if (version.isDefined) {
          if (asTimestamp) {
            spark.read
              .format("delta")
              .option("timestampAsOf", timestampMap(version.get))
              .load(dataPath)
          } else {
            spark.read.format("delta").option("versionAsOf", version.get).load(dataPath)
          }
        } else {
          spark.read.format("delta").load(dataPath)
        }
        deltaDf.filter(deltaDf("clicks") <= 2000).select(deltaDf("query"))
      }

      def checkExpectedIndexUsed(
          timeTravelVersion: Option[Long],
          expectedIndexVersion: Int): Unit = {
        assert(
          isIndexVersionUsed(
            query(timeTravelVersion, asTimestamp = false).queryExecution.optimizedPlan,
            indexName,
            expectedIndexVersion),
          s"timeTravelVer: ${timeTravelVersion.getOrElse("N/A")}, "
            + s"expectedIndexVer: $expectedIndexVersion")
        if (timeTravelVersion.isDefined) {
          val df = spark.read.format("delta").load(dataPath)
          // Check time travel query with timestampAsOf instead of versionAsOf.
          val latestVer = getDeltaLakeTableVersion(df)
          // Delta Lake throws an exception if the timestamp is larger than the latest commit.
          if (latestVer != timeTravelVersion.get) {
            assert(
              isIndexVersionUsed(
                query(timeTravelVersion, asTimestamp = true).queryExecution.optimizedPlan,
                "delta",
                expectedIndexVersion),
              s"timeTravelVer: ${timeTravelVersion.getOrElse("N/A")}, "
                + s"expectedIndexVer: $expectedIndexVersion")
          }
        }
      }

      appendData(doRefresh = false) // delta version 1
      appendData(doRefresh = false) // delta version 2

      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        hyperspace.createIndex(deltaDf, IndexConfig(indexName, Seq("clicks"), Seq("Query")))
      }

      withIndex(indexName) {
        checkExpectedIndexUsed(None, 1)

        withSQLConf(TestConfig.HybridScanEnabled: _*) {
          appendData(doRefresh = false) // delta version 3, index log version 1
          appendData(doRefresh = true) // delta version 4, index log version 3

          // The index should be applied for the updated version.
          checkExpectedIndexUsed(None, 3)
          checkExpectedIndexUsed(Some(0), 1)
          appendData(doRefresh = false) // delta version 5, index log version 3
          appendData(doRefresh = false) // delta version 6, index log version 3
          appendData(doRefresh = true) // delta version 7, index log version 5
          hyperspace.optimizeIndex(indexName) // delta version 8, index log version 7
          appendData(doRefresh = true) // delta version 9, index log version 9
          hyperspace.optimizeIndex(indexName) // delta version 10, index log version 11
          appendData(doRefresh = false) // delta version 11, index long version 11

          checkExpectedIndexUsed(None, 11)
          checkExpectedIndexUsed(Some(1), 1)
          checkExpectedIndexUsed(Some(2), 1)
          checkExpectedIndexUsed(Some(5), 3)
          checkExpectedIndexUsed(Some(6), 7)
          checkExpectedIndexUsed(Some(7), 7)
          checkExpectedIndexUsed(Some(8), 11)
          checkExpectedIndexUsed(Some(9), 11)
        }
      }
    }
  }

  private def getDeltaLakeTableVersion(df: DataFrame): Long = {
    df.queryExecution.optimizedPlan match {
      case LogicalRelation(
          HadoopFsRelation(location: TahoeLogFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        location.tableVersion
    }
  }

  private def getSparkFormattedTimestamps(values: Long*): Seq[String] = {
    // Simulates getting timestamps directly from Spark SQL.
    import spark.implicits._
    values
      .map(new Timestamp(_))
      .toDF("ts")
      .select($"ts".cast("string"))
      .as[String]
      .collect()
      .map(i => s"$i")
  }

  def isIndexVersionUsed(
      plan: LogicalPlan,
      expectedIndexName: String,
      expectedIndexVersion: Int): Boolean = {
    val res = plan.collect {
      case LogicalRelation(rel: IndexHadoopFsRelation, _, _, _) =>
        rel.toString().contains(s"Name: $expectedIndexName") && rel
          .toString()
          .contains(s"LogVersion: $expectedIndexVersion")
    }
    assert(res.length === 1)
    res.head
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
}
