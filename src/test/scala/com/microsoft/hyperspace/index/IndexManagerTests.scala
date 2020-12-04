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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.execution.datasources.{BucketingUtils, HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types._

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException, MockEventLogger, SampleData}
import com.microsoft.hyperspace.TestUtils.{copyWithState, latestIndexLogEntry, logManager}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.IndexConstants.{GLOBBING_PATTERN_KEY, OPTIMIZE_FILE_SIZE_THRESHOLD, REFRESH_MODE_FULL, REFRESH_MODE_INCREMENTAL}
import com.microsoft.hyperspace.telemetry.OptimizeActionEvent
import com.microsoft.hyperspace.util.{FileUtils, JsonUtils, PathUtils}

class IndexManagerTests extends HyperspaceSuite with SQLHelper {
  private val sampleParquetDataLocation = "src/test/resources/sampleparquet"
  override val systemPath = PathUtils.makeAbsolute("src/test/resources/indexLocation")
  private val indexConfig1 = IndexConfig("index1", Seq("RGUID"), Seq("Date"))
  private val indexConfig2 = IndexConfig("index2", Seq("Query"), Seq("imprs"))
  private lazy val hyperspace: Hyperspace = new Hyperspace(spark)
  private var df: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    FileUtils.delete(new Path(sampleParquetDataLocation))
    SampleData.save(
      spark,
      sampleParquetDataLocation,
      Seq("Date", "RGUID", "Query", "imprs", "clicks"))
    df = spark.read.parquet(sampleParquetDataLocation)
  }

  after {
    FileUtils.delete(systemPath, true)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(sampleParquetDataLocation))
    super.afterAll()
  }

  test("Verify that indexes() returns the correct dataframe with and without lineage.") {
    Seq(true, false).foreach { enableLineage =>
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> enableLineage.toString) {
        withIndex(indexConfig1.indexName) {
          import spark.implicits._
          hyperspace.createIndex(df, indexConfig1)
          val actual = hyperspace.indexes.as[IndexSummary].collect().head
          var expectedSchema =
            StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType)))
          if (enableLineage) {
            expectedSchema =
              expectedSchema.add(StructField(IndexConstants.DATA_FILE_NAME_ID, LongType, false))
          }
          val expected = new IndexSummary(
            indexConfig1.indexName,
            indexConfig1.indexedColumns,
            indexConfig1.includedColumns,
            200,
            expectedSchema.json,
            s"$systemPath/${indexConfig1.indexName}" +
              s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0",
            Constants.States.ACTIVE)
          assert(actual.equals(expected))
        }
      }
    }
  }

  test("Verify getIndexes()") {
    hyperspace.createIndex(df, indexConfig1)
    hyperspace.createIndex(df, indexConfig2)

    val expectedIndex1 = expectedIndex(
      indexConfig1,
      StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType))),
      df)

    val expectedIndex2 = expectedIndex(
      indexConfig2,
      StructType(Seq(StructField("Query", StringType), StructField("imprs", IntegerType))),
      df)

    // Verify if indexes returned match the actual created indexes.
    verifyIndexes(Seq(expectedIndex1, expectedIndex2))
  }

  test("Verify delete().") {
    hyperspace.createIndex(df, indexConfig1)
    hyperspace.createIndex(df, indexConfig2)

    val expectedIndex1 = expectedIndex(
      indexConfig1,
      StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType))),
      df)

    val expectedIndex2 = expectedIndex(
      indexConfig2,
      StructType(Seq(StructField("Query", StringType), StructField("imprs", IntegerType))),
      df)

    // Verify expected list of indexes before delete.
    verifyIndexes(Seq(expectedIndex1, expectedIndex2))

    hyperspace.deleteIndex("index1")
    hyperspace.deleteIndex("INDEX2")

    // Verify if delete index works and marks the index as `DELETED` in given list of indexes.
    val expectedDeletedIndex1 = copyWithState(expectedIndex1, Constants.States.DELETED)
    val expectedDeletedIndex2 = copyWithState(expectedIndex2, Constants.States.DELETED)
    verifyIndexes(Seq(expectedDeletedIndex1, expectedDeletedIndex2))
  }

  test("Verify restore().") {
    hyperspace.createIndex(df, indexConfig1)
    hyperspace.createIndex(df, indexConfig2)

    val expectedIndex1 = expectedIndex(
      indexConfig1,
      StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType))),
      df)

    val expectedIndex2 = expectedIndex(
      indexConfig2,
      StructType(Seq(StructField("Query", StringType), StructField("imprs", IntegerType))),
      df)

    // Verify expected list of indexes before delete.
    verifyIndexes(Seq(expectedIndex1, expectedIndex2))

    hyperspace.deleteIndex("index1")
    hyperspace.deleteIndex("INDEX2")

    // Verify if delete index works before restore.
    val expectedDeletedIndex1 = copyWithState(expectedIndex1, Constants.States.DELETED)
    val expectedDeletedIndex2 = copyWithState(expectedIndex2, Constants.States.DELETED)
    verifyIndexes(Seq(expectedDeletedIndex1, expectedDeletedIndex2))

    // Check if index1 got restored and `DELETED` flag is `CREATED` in the list of indexes.
    hyperspace.restoreIndex("index1")
    hyperspace.restoreIndex("INDEX2")
    verifyIndexes(Seq(expectedIndex1, expectedIndex2))
  }

  test("Verify vacuum().") {
    hyperspace.createIndex(df, indexConfig1)
    hyperspace.createIndex(df, indexConfig2)

    val expectedIndex1 = expectedIndex(
      indexConfig1,
      StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType))),
      df)

    val expectedIndex2 = expectedIndex(
      indexConfig2,
      StructType(Seq(StructField("Query", StringType), StructField("imprs", IntegerType))),
      df)

    // Verify expected list of indexes before delete.
    verifyIndexes(Seq(expectedIndex1, expectedIndex2))

    hyperspace.deleteIndex("index1")
    hyperspace.deleteIndex("INDEX2")

    // Verify if delete index works and marks the index as `DELETED` in given list of indexes.
    val expectedDeletedIndex1 = copyWithState(expectedIndex1, Constants.States.DELETED)
    val expectedDeletedIndex2 = copyWithState(expectedIndex2, Constants.States.DELETED)

    verifyIndexes(Seq(expectedDeletedIndex1, expectedDeletedIndex2))

    // Verify if after vacuum `DELETED` index are not part of the list of indexes.
    hyperspace.vacuumIndex("index1")
    hyperspace.vacuumIndex("INDEX2")
    val expectedVacuumedIndex1 = copyWithState(expectedIndex1, Constants.States.DOESNOTEXIST)
    val expectedVacuumedIndex2 = copyWithState(expectedIndex2, Constants.States.DOESNOTEXIST)
    verifyIndexes(Seq(expectedVacuumedIndex1, expectedVacuumedIndex2))
  }

  test("Verify refresh-full rebuild of index.") {
    Seq(("csv", Map("header" -> "true")), ("parquet", Map()), ("json", Map())).foreach {
      case (format, option: Map[String, String]) =>
        val refreshTestLocation = sampleParquetDataLocation + "refresh_" + format
        FileUtils.delete(new Path(refreshTestLocation))
        val indexConfig = IndexConfig(s"index_$format", Seq("RGUID"), Seq("imprs"))
        import spark.implicits._
        SampleData.testData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .limit(10)
          .write
          .options(option)
          .format(format)
          .save(refreshTestLocation)
        val df = spark.read.format(format).options(option).load(refreshTestLocation)
        hyperspace.createIndex(df, indexConfig)
        var indexCount =
          spark.read
            .parquet(s"$systemPath/index_$format" +
              s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
            .count()
        assert(indexCount == 10)

        // Change Original Data
        SampleData.testData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .limit(3)
          .write
          .mode("overwrite")
          .options(option)
          .format(format)
          .save(refreshTestLocation)
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_FULL)
        val newIndexLocation = s"$systemPath/index_$format"
        indexCount = spark.read
          .parquet(newIndexLocation +
            s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=1")
          .count()

        // Check if index got updated
        assert(indexCount == 3)

        // Check if lastest log file is updated with newly created index files
        val indexPath = PathUtils.makeAbsolute(newIndexLocation)
        val logManager = IndexLogManagerFactoryImpl.create(indexPath)
        val latestLog = logManager.getLatestLog()
        assert(latestLog.isDefined && latestLog.get.isInstanceOf[IndexLogEntry])
        val indexLog = latestLog.get.asInstanceOf[IndexLogEntry]
        val indexFiles = indexLog.content.files
        assert(indexFiles.nonEmpty)
        assert(indexFiles.forall(_.getName.startsWith("part-0")))
        assert(indexLog.state.equals("ACTIVE"))

        FileUtils.delete(new Path(refreshTestLocation))
      case _ => fail("invalid test")
    }
  }

  test("Verify incremental refresh (append-only) should index only newly appended data.") {
    withTempPathAsString { testPath =>
      // Setup. Create sample data and index.
      val indexConfig = IndexConfig("index", Seq("RGUID"), Seq("imprs"))
      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(testPath)
      val df = spark.read.parquet(testPath)
      hyperspace.createIndex(df, indexConfig)
      assert(countRecords(0) == 10)
      // Check if latest log file is updated with newly created index files.
      validateMetadata("index", Set("v__=0"))

      // Change original data.
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .mode("append")
        .parquet(testPath)
      hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

      // Check if index got updated.
      assert(countRecords(1) == 3)

      // Check if latest log file is updated with newly created index files.
      validateMetadata("index", Set("v__=0", "v__=1"))
    }
  }

  test("Verify quick optimize rebuild of index after index incremental refresh.") {
    withTempPathAsString { testPath =>
      val indexConfig = IndexConfig("index", Seq("RGUID"), Seq("imprs"))
      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(testPath)
      val df = spark.read.parquet(testPath)
      hyperspace.createIndex(df, indexConfig)
      assert(countRecords(0) == 10)

      // Change original data.
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .mode("append")
        .parquet(testPath)
      hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

      // Check if index got updated.
      assert(countRecords(1) == 3)

      hyperspace.optimizeIndex(indexConfig.indexName)

      // Check if latest log file is updated with newly created index files.
      // Since we appended 3 records for incremental refresh (v_==1) and optimized as v_==2,
      // some of index data files should be from v__=0.
      validateMetadata("index", Set("v__=0", "v__=2"))
    }
  }

  test(
    "Verify quick optimize rebuild of index after index incremental refresh with files" +
      " from multiple directories.") {
    // Test Logic:
    // 1. Create large data, such that index files are > threshold.
    // 2. Add small data. Refresh index every time. Here, index files will be smaller than
    // threshold.
    // 3. Call optimize. Check the metadata. It should not contain small index files created
    // during refresh operations.
    withTempPathAsString { testPath =>
      withSQLConf(OPTIMIZE_FILE_SIZE_THRESHOLD -> "900") {
        val indexConfig = IndexConfig("index", Seq("RGUID"), Seq("imprs"))
        import spark.implicits._
        val smallData = SampleData.testData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .limit(10)

        // Create large data. Index files will be bigger than threshold.
        val bigData = smallData.union(smallData).union(smallData).union(smallData)
        bigData.write.parquet(testPath)

        val df = spark.read.parquet(testPath)
        hyperspace.createIndex(df, indexConfig)
        validateMetadata("index", Set("v__=0"))
        assert(countRecords(0) == 40)

        // Append small amounts of new data and call incremental refresh.
        smallData.write.mode("append").parquet(testPath)
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)
        validateMetadata("index", Set("v__=0", "v__=1"))
        assert(countRecords(1) == 10)

        smallData.write.mode("append").parquet(testPath)
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)
        validateMetadata("index", Set("v__=0", "v__=1", "v__=2"))
        assert(countRecords(2) == 10)

        // Check after optimize,
        hyperspace.optimizeIndex(indexConfig.indexName)
        validateMetadata("index", Set("v__=0", "v__=3"))
        assert(countRecords(3) == 20)
      }
    }
  }

  test("Verify optimize is a no-op if no small files found.") {
    withTempPathAsString { testPath =>
      withSQLConf(OPTIMIZE_FILE_SIZE_THRESHOLD -> "1") {
        val indexConfig = IndexConfig("index", Seq("RGUID"), Seq("imprs"))
        import spark.implicits._
        SampleData.testData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .limit(10)
          .write
          .parquet(testPath)
        val df = spark.read.parquet(testPath)
        hyperspace.createIndex(df, indexConfig)

        // Change original data.
        SampleData.testData
          .toDF("Date", "RGUID", "Query", "imprs", "clicks")
          .limit(3)
          .write
          .mode("append")
          .parquet(testPath)
        hyperspace.refreshIndex(indexConfig.indexName, REFRESH_MODE_INCREMENTAL)

        // Check if latest log file is updated with newly created index files.
        val latestId = logManager(systemPath, indexConfig.indexName).getLatestId().get

        MockEventLogger.reset()
        hyperspace.optimizeIndex(indexConfig.indexName)

        val index = logManager(systemPath, indexConfig.indexName)
          .getLatestStableLog()
          .get
          .asInstanceOf[IndexLogEntry]

        // Check that no new log files were created in this operation.
        assert(latestId == index.id)

        // Check all index files are larger than the size threshold.
        assert(index.content.fileInfos.forall(_.size > 1))

        // Check emitted events.
        MockEventLogger.emittedEvents match {
          case Seq(
              OptimizeActionEvent(_, _, "Operation started."),
              OptimizeActionEvent(_, _, msg)) =>
            assert(
              msg.contains(
                "Optimize aborted as no optimizable index files smaller than 1 found."))
          case _ => fail()
        }
      }
    }
  }

  test("Verify optimize is no-op if each bucket has a single index file.") {
    withTempPathAsString { testPath =>
      val indexConfig = IndexConfig("index", Seq("RGUID"), Seq("imprs"))
      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(testPath)
      val df = spark.read.parquet(testPath)
      hyperspace.createIndex(df, indexConfig)

      // Check if latest log file is updated with newly created index files.
      val latestId = logManager(systemPath, indexConfig.indexName).getLatestId().get

      // Ensure all index files are smaller than the size threshold so that they can be
      // possible candidates for optimizeIndex. In this test, as the test data is tiny,
      // all of index files are less than the threshold.
      {
        val index = logManager(systemPath, indexConfig.indexName)
          .getLatestStableLog()
          .get
          .asInstanceOf[IndexLogEntry]
        assert(
          index.content.fileInfos
            .forall(_.size < IndexConstants.OPTIMIZE_FILE_SIZE_THRESHOLD_DEFAULT))
      }

      MockEventLogger.reset()
      hyperspace.optimizeIndex(indexConfig.indexName)

      {
        val index = logManager(systemPath, indexConfig.indexName)
          .getLatestStableLog()
          .get
          .asInstanceOf[IndexLogEntry]

        // Check that no new log files were created in this operation.
        assert(latestId == index.id)

        // Check all index files are not needed to be optimized. (i.e. non-optimizable)
        val filesPerBucket =
          index.content.files.groupBy(f => BucketingUtils.getBucketId(f.getName))
        assert(filesPerBucket.values.forall(_.size == 1))
      }

      // Check emitted events.
      MockEventLogger.emittedEvents match {
        case Seq(
            OptimizeActionEvent(_, _, "Operation started."),
            OptimizeActionEvent(_, _, msg)) =>
          assert(
            msg.contains(
              "Optimize aborted as no optimizable index files smaller than 268435456 found."))
        case _ => fail()
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
        .parquet(testPath)

      val df = spark.read.parquet(testPath)
      hyperspace.createIndex(df, IndexConfig("index", Seq("clicks"), Seq("Date")))

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
        .parquet(testPath)
      hyperspace.refreshIndex("index", "incremental")

      // Check if partition columns are correctly stored in index contents.
      val indexDf2 = spark.read.parquet(s"$systemPath/index").where("Date = '2019-10-03'")
      assert(appendData.size == indexDf2.count())
      val newEntry = latestIndexLogEntry(systemPath, "index")
      assert(newEntry.relations.head.options("basePath").equals(absoluteTestPath))
    }
  }

  test("Verify index maintenance (create, refresh) works with globbing patterns.") {
    withTempPathAsString { testPath =>
      val absoluteTestPath = PathUtils.makeAbsolute(testPath)
      val globPath = absoluteTestPath + "/*"
      val p1 = absoluteTestPath + "/1"
      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(p1)

      // Create index with globbing pattern.
      val df =
        spark.read.option(GLOBBING_PATTERN_KEY, globPath).parquet(globPath)
      val indexConfig = IndexConfig("index", Seq("RGUID"), Seq("imprs"))
      hyperspace.createIndex(df, indexConfig)

      // Check if latest log file contains data source as globbing pattern.
      var index = latestIndexLogEntry(systemPath, indexConfig.indexName)
      var relation = index.relations.head
      var indexedFiles = relation.data.properties.content.files
      assert(relation.rootPaths.head.equals(globPath))
      assert(indexedFiles.forall(_.toString.startsWith(p1)))

      // Append data to a new directory which matches the globbing pattern.
      val p2 = absoluteTestPath + "/2"
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .parquet(p2)

      // Refresh index and check if files from both directories are present.
      hyperspace.refreshIndex(indexConfig.indexName, "incremental")
      index = latestIndexLogEntry(systemPath, indexConfig.indexName)
      relation = index.relations.head
      indexedFiles = relation.data.properties.content.files
      assert(relation.rootPaths.head.equals(globPath))
      assert(indexedFiles.exists(_.toString.startsWith(p1)))
      assert(indexedFiles.exists(_.toString.startsWith(p2)))
    }
  }

  test("Verify createIndex fails for globbing patterns when config is set incorrectly.") {
    withTempPathAsString { testPath =>
      val absoluteTestPath = PathUtils.makeAbsolute(testPath)
      val globPath = absoluteTestPath + "/1/*"
      val p1 = absoluteTestPath + "/2/1"
      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(p1)

      // Create index where globbing pattern config doesn't match with actual data being indexed.
      val df =
        spark.read.option(GLOBBING_PATTERN_KEY, globPath).parquet(p1)
      val indexConfig = IndexConfig("index", Seq("RGUID"), Seq("imprs"))
      val ex = intercept[HyperspaceException] {
        hyperspace.createIndex(df, indexConfig)
      }
      assert(
        ex.msg.contains("Some glob patterns do not match with available root paths of the " +
          "source data."))
    }
  }

  test("Verify createIndex works for globbing patterns with multiple paths.") {
    withTempPathAsString { testPath =>
      val absoluteTestPath = PathUtils.makeAbsolute(testPath)
      val globPath1 = absoluteTestPath + "/1/*"
      val globPath2 = absoluteTestPath + "/2/*"
      val p1 = absoluteTestPath + "/1/1"
      val p2 = absoluteTestPath + "/2/1"

      // Store some data in p1 and p2 paths.
      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(p1)
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(p2)

      // Create index with globbing pattern.
      val df =
        spark.read
          .option(GLOBBING_PATTERN_KEY, s"$globPath1,$globPath2")
          .parquet(globPath1, globPath2)
      val indexConfig = IndexConfig("index", Seq("RGUID"), Seq("imprs"))
      hyperspace.createIndex(df, indexConfig)

      // Check if latest log file contains data source as globbing pattern.
      var index = latestIndexLogEntry(systemPath, indexConfig.indexName)
      var relation = index.relations.head
      var indexedFiles = relation.data.properties.content.files
      assert(relation.rootPaths.equals(Seq(globPath1, globPath2)))
      assert(
        indexedFiles.forall(path => path.toString.startsWith(p1) || path.toString.startsWith(p2)))

      // Append data to new directories which match the globbing pattern.
      val p3 = absoluteTestPath + "/1/2"
      val p4 = absoluteTestPath + "/2/2"
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .parquet(p3)
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .parquet(p4)

      // Refresh index and check if files from all directories are present.
      hyperspace.refreshIndex(indexConfig.indexName, "incremental")
      index = latestIndexLogEntry(systemPath, indexConfig.indexName)
      relation = index.relations.head
      indexedFiles = relation.data.properties.content.files
      assert(relation.rootPaths.equals(Seq(globPath1, globPath2)))
      assert(
        indexedFiles.forall(path =>
          path.toString.startsWith(p1) || path.toString.startsWith(p2) ||
            path.toString.startsWith(p3) || path.toString.startsWith(p4)))
      assert(indexedFiles.exists(_.toString.startsWith(p1)))
      assert(indexedFiles.exists(_.toString.startsWith(p2)))
      assert(indexedFiles.exists(_.toString.startsWith(p3)))
      assert(indexedFiles.exists(_.toString.startsWith(p4)))
    }
  }

  test("Verify createIndex works for globbing patterns with multiple levels.") {
    withTempPathAsString { testPath =>
      val absoluteTestPath = PathUtils.makeAbsolute(testPath)
      val globPath = absoluteTestPath + "/*/*"
      val p1 = absoluteTestPath + "/1/1"
      val p2 = absoluteTestPath + "/2/1"

      // Store some data in p1 and p2 paths.
      import spark.implicits._
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(p1)
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(10)
        .write
        .parquet(p2)

      // Create index with globbing pattern.
      val df =
        spark.read
          .option(GLOBBING_PATTERN_KEY, s"$globPath")
          .parquet(globPath)
      val indexConfig = IndexConfig("index", Seq("RGUID"), Seq("imprs"))
      hyperspace.createIndex(df, indexConfig)

      // Check if latest log file contains data source as globbing pattern.
      var index = latestIndexLogEntry(systemPath, indexConfig.indexName)
      var relation = index.relations.head
      var indexedFiles = relation.data.properties.content.files
      assert(relation.rootPaths.equals(Seq(globPath)))
      assert(
        indexedFiles.forall(path => path.toString.startsWith(p1) || path.toString.startsWith(p2)))

      // Append data to new directories which match the globbing pattern.
      val p3 = absoluteTestPath + "/1/2"
      val p4 = absoluteTestPath + "/2/2"
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .parquet(p3)
      SampleData.testData
        .toDF("Date", "RGUID", "Query", "imprs", "clicks")
        .limit(3)
        .write
        .parquet(p4)

      // Refresh index and check if files from all directories are present.
      hyperspace.refreshIndex(indexConfig.indexName, "incremental")
      index = latestIndexLogEntry(systemPath, indexConfig.indexName)
      relation = index.relations.head
      indexedFiles = relation.data.properties.content.files
      assert(relation.rootPaths.equals(Seq(globPath)))
      assert(
        indexedFiles.forall(path =>
          path.toString.startsWith(p1) || path.toString.startsWith(p2) ||
            path.toString.startsWith(p3) || path.toString.startsWith(p4)))
      assert(indexedFiles.exists(_.toString.startsWith(p1)))
      assert(indexedFiles.exists(_.toString.startsWith(p2)))
      assert(indexedFiles.exists(_.toString.startsWith(p3)))
      assert(indexedFiles.exists(_.toString.startsWith(p4)))
    }
  }

  private def validateMetadata(indexName: String, indexVersions: Set[String]): Unit = {
    val indexLog = latestIndexLogEntry(systemPath, indexName)
    val indexFiles = indexLog.content.files
    assert(indexFiles.nonEmpty)
    assert(indexFiles.forall(_.getName.startsWith("part-0")))
    assert(indexLog.state.equals("ACTIVE"))
    // Check all files belong to the provided index versions only.
    assert(indexFiles.map(_.getParent.getName).toSet === indexVersions)
  }

  private def expectedIndex(
      indexConfig: IndexConfig,
      schema: StructType,
      df: DataFrame,
      state: String = Constants.States.ACTIVE): IndexLogEntry = {

    val fileIdTracker = getFileIdTracker(indexConfig)
    LogicalPlanSignatureProvider.create().signature(df.queryExecution.optimizedPlan) match {
      case Some(s) =>
        val relations = df.queryExecution.optimizedPlan.collect {
          case LogicalRelation(
              HadoopFsRelation(
                location: PartitioningAwareFileIndex,
                _,
                dataSchema,
                _,
                fileFormat,
                options),
              _,
              _,
              _) =>
            val files = location.allFiles
            val sourceDataProperties =
              Hdfs.Properties(Content.fromLeafFiles(files, fileIdTracker).get)
            val fileFormatName = fileFormat match {
              case d: DataSourceRegister => d.shortName
              case other => throw HyperspaceException(s"Unsupported file format $other")
            }
            Relation(
              location.rootPaths.map(_.toString),
              Hdfs(sourceDataProperties),
              dataSchema.json,
              fileFormatName,
              options - "path")
        }
        val sourcePlanProperties = SparkPlan.Properties(
          relations,
          null,
          null,
          LogicalPlanFingerprint(
            LogicalPlanFingerprint.Properties(
              Seq(Signature(LogicalPlanSignatureProvider.create().name, s)))))

        val entry = IndexLogEntry(
          indexConfig.indexName,
          CoveringIndex(
            CoveringIndex.Properties(
              CoveringIndex.Properties
                .Columns(indexConfig.indexedColumns, indexConfig.includedColumns),
              IndexLogEntry.schemaString(schema),
              IndexConstants.INDEX_NUM_BUCKETS_DEFAULT,
              Map())),
          Content.fromDirectory(
            PathUtils.makeAbsolute(
              new Path(s"$systemPath/${indexConfig.indexName}" +
                s"/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")),
            fileIdTracker),
          Source(SparkPlan(sourcePlanProperties)),
          Map())
        entry.state = state
        entry
      case None => fail("Invalid plan for index dataFrame.")
    }
  }

  // Verify if the indexes currently stored in Hyperspace matches the given indexes.
  private def verifyIndexes(expectedIndexes: Seq[IndexLogEntry]): Unit = {
    assert(IndexCollectionManager(spark).getIndexes().toSet == expectedIndexes.toSet)
  }

  private def countRecords(version: Int): Long = {
    spark.read
      .parquet(s"$systemPath/index/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=$version")
      .count()
  }

  private def getFileIdTracker(indexConfig: IndexConfig): FileIdTracker = {
    val indexLogPath = PathUtils.makeAbsolute(
      s"$systemPath/${indexConfig.indexName}/${IndexConstants.HYPERSPACE_LOG}/latestStable")
    val indexLogJson =
      FileUtils.readContents(indexLogPath.getFileSystem(new Configuration), indexLogPath)
    JsonUtils
      .fromJson[IndexLogEntry](indexLogJson)
      .fileIdTracker
  }
}
