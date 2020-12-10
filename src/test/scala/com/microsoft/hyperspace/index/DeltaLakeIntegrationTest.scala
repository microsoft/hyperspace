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

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData}

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

        assert(isIndexUsed(query().queryExecution.optimizedPlan, "deltaIndex", false))

        // Create a new version by deleting entries.
        val deltaTable = DeltaTable.forPath(dataPath)
        deltaTable.delete("clicks > 5000")

        withSQLConf(
          "spark.hyperspace.index.hybridscan.enabled" -> "true",
          "spark.hyperspace.index.hybridscan.delete.enabled" -> "true") {
          // The index should be applied for the updated version.
          assert(isIndexUsed(query().queryExecution.optimizedPlan, "deltaIndex", true))

          // Append data.
          dfFromSample
            .limit(3)
            .write
            .format("delta")
            .mode("append")
            .save(dataPath)

          // The index should be applied for the updated version.
          assert(isIndexUsed(query().queryExecution.optimizedPlan, "deltaIndex", true))
        }
      }
    }
  }

  def isIndexUsed(
      plan: LogicalPlan,
      indexPathSubStr: String,
      isHybridScan: Boolean = false): Boolean = {
    val rootPaths = plan.collect {
      case LogicalRelation(
          HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        location.rootPaths
    }.flatten
    if (!isHybridScan) {
      rootPaths.nonEmpty && rootPaths.forall(_.toString.contains(indexPathSubStr))
    } else {
      rootPaths.nonEmpty && rootPaths.exists(_.toString.contains(indexPathSubStr))
    }
  }
}
