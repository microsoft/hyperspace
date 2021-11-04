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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources._

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData, TestConfig}
import com.microsoft.hyperspace.TestUtils.{getFileIdTracker, latestIndexLogEntry}
import com.microsoft.hyperspace.index.IndexConstants.REFRESH_MODE_QUICK
import com.microsoft.hyperspace.index.plans.logical.IndexHadoopFsRelation
import com.microsoft.hyperspace.index.sources.delta.DeltaLakeRelation
import com.microsoft.hyperspace.util.PathUtils
import com.microsoft.hyperspace.util.PathUtils.DataPathFilter

class DeltaLakeIntegrationTest extends QueryTest with HyperspaceSuite {
  override val indexLocationDirName = "deltaLakeIntegrationTest"

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

      // Enable lineage column for Hybrid Scan delete support to test time travel.
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

        // Enable Hybrid Scan for time travel query validation.
        withSQLConf(TestConfig.HybridScanEnabled: _*) {
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
    withTempPathAsString { path =>
      import spark.implicits._
      val df = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
      df.write.format("delta").save(path)

      val tsMap = mutable.Map[Long, String]()
      tsMap.put(0, getSparkFormattedTimestamps(System.currentTimeMillis).head)

      val deltaDf = spark.read.format("delta").load(path)
      val indexName = "deltaIndex1"
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        hyperspace.createIndex(deltaDf, IndexConfig(indexName, Seq("clicks"), Seq("Query")))
      }

      withIndex(indexName) {
        checkExpectedIndexUsed(indexName, path, None, 1)

        withSQLConf(TestConfig.HybridScanEnabled: _*) {
          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 1, index log version 1
          appendAndRefresh(df, path, indexName, Some("full"), tsMap)
          // delta version 2, index log version 3 (refresh)

          // Without delta table version, the latest log version should be applied.
          checkExpectedIndexUsed(indexName, path, None, 3)
          // For delta table version 0, candidate log version is 1.
          checkExpectedIndexUsed(indexName, path, Some(0, tsMap(0)), 1)

          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 3, index log version 3
          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 4, index log version 3
          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 5, index log version 3
          appendAndRefresh(df, path, indexName, Some("full"), tsMap)
          // delta version 6, index log version 5 (refresh)
          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 7, index log version 5
          appendAndRefresh(df, path, indexName, Some("full"), tsMap)
          // delta version 8, index log version 7 (refresh)

          // Without delta table version, the latest log version should be applied.
          checkExpectedIndexUsed(indexName, path, None, 7)
          // For delta table version 1, candidate log versions are 1, 3 and both are same diff,
          // then pick the larger one (3).
          checkExpectedIndexUsed(indexName, path, Some(1, tsMap(1)), 3)
          // For delta table version 3, candidate log versions are 3, 5, but 3 is closer.
          checkExpectedIndexUsed(indexName, path, Some(3, tsMap(3)), 3)
          // For delta table version 5, candidate log versions are 3, 5, but 5 is closer.
          checkExpectedIndexUsed(indexName, path, Some(5, tsMap(5)), 5)
          // For delta table version 6, candidate log version is 5. (refresh)
          checkExpectedIndexUsed(indexName, path, Some(6, tsMap(6)), 5)
          // For delta table version 8, candidate log version is 7. (refresh)
          checkExpectedIndexUsed(indexName, path, Some(8, tsMap(8)), 7)
        }
      }
    }
  }

  test("Verify time travel query works well with incremental refresh & optimizeIndex.") {
    withTempPathAsString { path =>
      import spark.implicits._
      val df = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
      df.write.format("delta").save(path)

      val tsMap = mutable.Map[Long, String]()
      tsMap.put(0, getSparkFormattedTimestamps(System.currentTimeMillis).head)

      val indexName = "deltaIndex2"
      val deltaDf = spark.read.format("delta").load(path)

      appendAndRefresh(df, path, indexName, None, tsMap) // delta version 1
      appendAndRefresh(df, path, indexName, None, tsMap) // delta version 2

      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        hyperspace.createIndex(deltaDf, IndexConfig(indexName, Seq("clicks"), Seq("Query")))
      }

      withIndex(indexName) {
        checkExpectedIndexUsed(indexName, path, None, 1)

        withSQLConf(TestConfig.HybridScanEnabled: _*) {
          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 3, index log version 1
          appendAndRefresh(df, path, indexName, Some("incremental"), tsMap)
          // delta version 4, index log version 3 (refresh)

          // Without delta table version, the latest log version should be applied.
          checkExpectedIndexUsed(indexName, path, None, 3)
          // For delta table version 0, candidate log version is 1.
          checkExpectedIndexUsed(indexName, path, Some(0, tsMap(1)), 1)

          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 5, index log version 3
          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 6, index log version 3
          appendAndRefresh(df, path, indexName, Some("incremental"), tsMap)
          // delta version 7, index log version 5 (refresh)
          hyperspace.optimizeIndex(indexName)
          // delta version 7, index log version 7 (optimize)
          appendAndRefresh(df, path, indexName, Some("incremental"), tsMap)
          // delta version 8, index log version 9 (refresh)
          hyperspace.optimizeIndex(indexName)
          // delta version 8, index log version 11 (optimize)
          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 9, index long version 11

          // Without delta table version, the latest log version should be applied.
          checkExpectedIndexUsed(indexName, path, None, 11)

          // For delta table version 1, candidate log version is 1. (before index creation)
          checkExpectedIndexUsed(indexName, path, Some(1, tsMap(1)), 1)
          // For delta table version 2, candidate log version is 1. (index creation)
          checkExpectedIndexUsed(indexName, path, Some(2, tsMap(2)), 1)
          // For delta table version 5, candidate log versions are 3, 7. 3 is closer.
          checkExpectedIndexUsed(indexName, path, Some(5, tsMap(5)), 3)
          // For delta table version 6, candidate log versions are 3, 7. 7 is closer.
          checkExpectedIndexUsed(indexName, path, Some(6, tsMap(6)), 7)
          // For delta table version 7, candidate log version is 7. (optimize)
          checkExpectedIndexUsed(indexName, path, Some(7, tsMap(7)), 7)
          // For delta table version 8, candidate log version is 11. (optimize)
          checkExpectedIndexUsed(indexName, path, Some(8, tsMap(8)), 11)
          // For delta table version 9, candidate log version is 11.
          checkExpectedIndexUsed(indexName, path, Some(9, tsMap(9)), 11)
        }
      }
    }
  }

  test("Verify time travel query works well with VacuumIndex.") {
    withTempPathAsString { path =>
      import spark.implicits._
      val df = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
      df.write.format("delta").save(path)

      val tsMap = mutable.Map[Long, String]()
      tsMap.put(0, getSparkFormattedTimestamps(System.currentTimeMillis).head)

      val indexName = "deltaIndex3"
      val deltaDf = spark.read.format("delta").load(path)

      appendAndRefresh(df, path, indexName, None, tsMap) // delta version 1
      appendAndRefresh(df, path, indexName, None, tsMap) // delta version 2
      val indexConfig = IndexConfig(indexName, Seq("clicks"), Seq("Query"))
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        hyperspace.createIndex(deltaDf, indexConfig)
      }

      withIndex(indexName) {
        checkExpectedIndexUsed(indexName, path, None, 1)

        withSQLConf(TestConfig.HybridScanEnabled: _*) {
          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 3, index log version 1.
          appendAndRefresh(df, path, indexName, Some("incremental"), tsMap)
          // delta version 4, index log version 3 (refresh).

          // Without delta table version, the latest log version should be applied.
          checkExpectedIndexUsed(indexName, path, None, 3)
          // For delta table version 0, candidate log version is 1.
          checkExpectedIndexUsed(indexName, path, Some(0, tsMap(1)), 1)

          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 5, index log version 3.
          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 6, index log version 3.
          appendAndRefresh(df, path, indexName, Some("incremental"), tsMap)
          // delta version 7, index log version 5 (refresh).
          hyperspace.optimizeIndex(indexName)
          // delta version 7, index log version 7 (optimize).
          appendAndRefresh(df, path, indexName, Some("incremental"), tsMap)
          // delta version 8, index log version 9 (refresh).
          hyperspace.optimizeIndex(indexName)
          // delta version 8, index log version 11 (optimize).
          appendAndRefresh(df, path, indexName, None, tsMap)
          // delta version 9, index long version 11.

          val beforeDataFiles = listFiles(path, getFileIdTracker(systemPath, indexConfig))
          val beforeIndexDataFiles =
            listFiles(getIndexDataPath(indexName, 0), getFileIdTracker(systemPath, indexConfig))

          // Calling vacuumIndex on active index deletes outdated data and history.
          hyperspace.vacuumIndex(indexName)

          val afterDataFiles = listFiles(path, getFileIdTracker(systemPath, indexConfig))
          val afterIndexDataFiles =
            listFiles(getIndexDataPath(indexName, 0), getFileIdTracker(systemPath, indexConfig))

          // Data files should not affected by vacuum index
          assert(beforeDataFiles === afterDataFiles)
          // Two files should be deleted in the version 0 index data directory
          assert((beforeIndexDataFiles.toSet -- afterIndexDataFiles.toSet).size == 2)

          // These paths should not be deleted
          val fs = new Path("/").getFileSystem(new Configuration)
          val shouldExistPath = Seq(0, 5)
          shouldExistPath.map(idx => {
            val indexDataPath = getIndexDataPath(indexName, idx)
            assert(fs.exists(new Path(indexDataPath)))
          })

          // These path should be deleted
          val shouldDeletedPath = Seq(1, 2, 3, 4)
          shouldDeletedPath.map(idx => {
            val indexDataPath = getIndexDataPath(indexName, idx)
            assert(!fs.exists(new Path(indexDataPath)))
          })

          // Whenever index is used, since every history is also vacuumed,
          // expected index version is always 13 (the last version after vacuum outdated).
          checkExpectedIndexUsed(indexName, path, None, 13)
          checkExpectedIndexUsed(indexName, path, Some(1, tsMap(1)), 13)
          checkExpectedIndexUsed(indexName, path, Some(2, tsMap(2)), 13)
          checkExpectedIndexUsed(indexName, path, Some(5, tsMap(5)), 13)
          checkExpectedIndexUsed(indexName, path, Some(6, tsMap(6)), 13)
          checkExpectedIndexUsed(indexName, path, Some(7, tsMap(7)), 13)
          checkExpectedIndexUsed(indexName, path, Some(8, tsMap(8)), 13)
          checkExpectedIndexUsed(indexName, path, Some(9, tsMap(9)), 13)
        }
      }
    }
  }

  test("DeltaLakeRelation.closestIndex should handle indexes without delta versions.") {
    withTempPathAsString { path =>
      import spark.implicits._
      val df = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
      df.write.format("delta").save(path)
      val parquetDf = spark.read.parquet(path)
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        hyperspace.createIndex(parquetDf, IndexConfig("testIndex", Seq("Date"), Seq("clicks")))
      }
      withSQLConf(TestConfig.HybridScanEnabled: _*) {
        val deltaDf = spark.read.format("delta").load(path)
        val rel = new DeltaLakeRelation(
          spark,
          deltaDf.queryExecution.optimizedPlan.asInstanceOf[LogicalRelation])

        val entry = latestIndexLogEntry(systemPath, "testIndex")
        assert(rel.closestIndex(entry).equals(entry))
      }
    }
  }

  def checkExpectedIndexUsed(
      indexName: String,
      dataPath: String,
      timeTravelInfo: Option[(Long, String)],
      expectedIndexVersion: Int): Unit = {
    def buildQuery(
        path: String,
        versionAsOf: Option[Long] = None,
        timestampAsOf: Option[String] = None): DataFrame = {
      assert(!(versionAsOf.isDefined && timestampAsOf.isDefined))
      val deltaDf = if (versionAsOf.isDefined) {
        spark.read.format("delta").option("versionAsOf", versionAsOf.get).load(path)
      } else if (timestampAsOf.isDefined) {
        spark.read
          .format("delta")
          .option("timestampAsOf", timestampAsOf.get)
          .load(path)
      } else {
        spark.read.format("delta").load(path)
      }
      deltaDf.filter(deltaDf("clicks") <= 2000).select(deltaDf("query"))
    }

    val q = buildQuery(dataPath, timeTravelInfo.map(t => t._1), None)
    assert(
      isIndexVersionUsed(q.queryExecution.optimizedPlan, indexName, expectedIndexVersion),
      s"timeTravelVer: ${timeTravelInfo.getOrElse("N/A")}, " +
        s"expectedIndexVer: $expectedIndexVersion, " +
        s"plan: ${q.queryExecution.optimizedPlan.toString}")
    if (timeTravelInfo.isDefined) {
      val df = spark.read.format("delta").load(dataPath)
      // Check time travel query with timestampAsOf instead of versionAsOf.
      val latestVer = getDeltaLakeTableVersion(df)
      // Delta Lake throws an exception if the timestamp is larger than the latest commit.
      if (latestVer != timeTravelInfo.get._1) {
        val q = buildQuery(dataPath, None, timeTravelInfo.map(t => t._2))
        assert(
          isIndexVersionUsed(q.queryExecution.optimizedPlan, indexName, expectedIndexVersion),
          s"timeTravelVer: ${timeTravelInfo.getOrElse("N/A")}, " +
            s"expectedIndexVer: $expectedIndexVersion, " +
            s"plan: ${q.queryExecution.optimizedPlan.toString}")
      }
    }
  }

  private def appendAndRefresh(
      df: DataFrame,
      dataPath: String,
      indexName: String,
      refreshMode: Option[String],
      timestampMap: mutable.Map[Long, String]): Unit = {
    df.limit(3).write.format("delta").mode("append").save(dataPath)

    // Get the timestamp string for time travel query using "timestampAsOf" option.
    val timestampStr = getSparkFormattedTimestamps(System.currentTimeMillis).head
    // Sleep 1 second because Delta lake internally handles commit timestamp in second unit.
    // Without this sleep, tests will fail in azure pipeline.
    Thread.sleep(1000)

    if (refreshMode.isDefined) {
      hyperspace.refreshIndex(indexName, mode = refreshMode.get)
    }

    val newDf = spark.read.format("delta").load(dataPath)
    getDeltaLakeTableVersion(newDf)
    timestampMap.put(getDeltaLakeTableVersion(newDf), timestampStr)
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

  private def listFiles(path: String, fileIdTracker: FileIdTracker): Seq[FileInfo] = {
    val absolutePath = PathUtils.makeAbsolute(path)
    val fs = absolutePath.getFileSystem(new Configuration)
    fs.listStatus(absolutePath)
      .toSeq
      .filter(f => DataPathFilter.accept(f.getPath))
      .map(f => FileInfo(f, fileIdTracker.addFile(f), asFullPath = true))
  }

  private def getIndexDataPath(indexName: String, idx: Int): String = {
    val indexBasePath = spark.conf.get("spark.hyperspace.system.path")

    s"${indexBasePath}/${indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=${idx}"
  }

}
