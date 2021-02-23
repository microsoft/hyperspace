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

import io.delta.tables.DeltaTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources._

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData, TestConfig}
import com.microsoft.hyperspace.TestUtils.latestIndexLogEntry
import com.microsoft.hyperspace.index.IndexConstants.REFRESH_MODE_QUICK
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
