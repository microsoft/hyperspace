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

import scala.util.Try

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.Union
import org.apache.spark.sql.execution.{FileSourceScanExec, InputAdapter, ProjectExec, UnionExec, WholeStageCodegenExec}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec, ShuffleExchangeExec}

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData}
import com.microsoft.hyperspace.index.execution.BucketUnionExec
import com.microsoft.hyperspace.index.plans.logical.BucketUnion
import com.microsoft.hyperspace.index.rules.{FilterIndexRule, JoinIndexRule}
import com.microsoft.hyperspace.util.FileUtils

class HybridScanTest extends QueryTest with HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/hybridScanTest")

  private val sampleData = SampleData.testData
  private val sampleParquetDataLocation = "src/test/resources/sampleparquet2"
  private val sampleJsonDataLocation = "src/test/resources/samplejson"
  private var df: DataFrame = _
  private var hyperspace: Hyperspace = _
  private val indexConfig1 = IndexConfig("index1", Seq("clicks"), Seq("query"))
  private val indexConfig2 = IndexConfig("index2", Seq("clicks"), Seq("query"))

  private def copyFirstFile(df: DataFrame): Unit = {
    val files = df.queryExecution.optimizedPlan.collect {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        location.allFiles
    }.flatten
    val sourcePath = files.head.getPath
    val destPath = new Path(files.head.getPath + ".copy")
    sourcePath.getFileSystem(new Configuration).copyToLocalFile(sourcePath, destPath)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    val sparkSession = spark
    import sparkSession.implicits._
    hyperspace = new Hyperspace(spark)
    FileUtils.delete(new Path(sampleParquetDataLocation))
    FileUtils.delete(new Path(sampleJsonDataLocation))
    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    dfFromSample.write.parquet(sampleParquetDataLocation)
    dfFromSample.write.json(sampleJsonDataLocation)

    df = spark.read.parquet(sampleParquetDataLocation)
    hyperspace.createIndex(df, indexConfig1)
    copyFirstFile(df)

    df = spark.read.json(sampleJsonDataLocation)
    hyperspace.createIndex(df, indexConfig2)
    copyFirstFile(df)
  }

  after {
    spark.disableHyperspace()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.delete(new Path(sampleParquetDataLocation))
    FileUtils.delete(new Path(sampleJsonDataLocation))
  }

  test("Append-only: filter index & parquet format, " +
    "index relation should include appended file paths") {
    df = spark.read.parquet(sampleParquetDataLocation)
    val query = df.filter(df("clicks") <= 2000).select(df("query"))

    withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "false") {
      val transformed = FilterIndexRule(query.queryExecution.optimizedPlan)
      assert(transformed.equals(query.queryExecution.optimizedPlan), "No plan transformation.")
    }

    withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "true") {
      val planWithHybridScan = FilterIndexRule(query.queryExecution.optimizedPlan)
      assert(
        !planWithHybridScan.equals(query.queryExecution.optimizedPlan),
        "Plan should be transformed.")

      // check appended file is added to relation node or not
      val nodes = planWithHybridScan collect {
        case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
          assert(fsRelation.location.inputFiles.count(_.contains(".copy")) === 1)
          assert(fsRelation.location.inputFiles.count(_.contains("index1")) === 4)
          p
      }
      assert(nodes.length === 1)

      spark.enableHyperspace()
      val query2 = df.filter(df("clicks") <= 2000).select(df("query"))

      checkAnswer(query2, query)
    }
  }

  test("Append-only: join index, appended data should be shuffled with indexed columns " +
    "and merged by BucketUnion") {
    df = spark.read.parquet(sampleParquetDataLocation)
    val query = df.filter(df("clicks") >= 2000).select(df("clicks"), df("query"))
    val query2 = df.filter(df("clicks") <= 4000).select(df("clicks"), df("query"))
    val join = query.join(query2, "clicks")

    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "false") {
        val transformed = JoinIndexRule(join.queryExecution.optimizedPlan)
        assert(transformed.equals(join.queryExecution.optimizedPlan), "No plan transformation.")
      }

      withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "true") {
        val planWithHybridScan = JoinIndexRule(join.queryExecution.optimizedPlan)
        assert(
          !planWithHybridScan.equals(join.queryExecution.optimizedPlan),
          "Plan should be transformed.")

        // check appended file is added to relation node or not
        val nodes = planWithHybridScan collect {
          case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
            val appendedFileCnt = fsRelation.location.inputFiles.count(_.contains(".copy"))
            val indexFileCnt = fsRelation.location.inputFiles.count(_.contains("index1"))
            assert(appendedFileCnt === 1 || indexFileCnt === 4)
            assert(appendedFileCnt * indexFileCnt === 0)
            p
          case p @ BucketUnion(_, _) => p
        }
        assert(nodes.count(p => p.getClass.toString.contains("LogicalRelation")) === 4)
        assert(nodes.count(p => p.getClass.toString.contains("BucketUnion")) === 2)

        spark.enableHyperspace()
        val execPlan = spark.sessionState.executePlan(planWithHybridScan).executedPlan
        val execNodes = execPlan collect {
          case p @ BucketUnionExec(children, bucketSpec) =>
            assert(children.size === 2)
            // head is always the index plan
            assert(Try(children.head.asInstanceOf[WholeStageCodegenExec]).isSuccess)
            assert(
              Try(children.last.asInstanceOf[ShuffleExchangeExec]).isSuccess || Try(
                children.last.asInstanceOf[ReusedExchangeExec]).isSuccess)
            assert(bucketSpec.numBuckets === 200)
            p
          case p @ FileSourceScanExec(_, _, _, _, _, dataFilters, _) =>
            // check filter pushed down properly
            assert(
              dataFilters.toString.contains(" >= 2000)") && dataFilters.toString.contains(
                " <= 4000)"))
            p
        }
        assert(execNodes.count(p => p.getClass.toString.contains("BucketUnionExec")) === 2)
        // 2 of index, 1 of appended file (1 is reused)
        assert(execNodes.count(p => p.getClass.toString.contains("FileSourceScanExec")) === 3)

        val join2 = query.join(query2, "clicks")
        checkAnswer(join2, join)
      }
    }
  }

  test("Append-only: filter rule & json format, " +
    "appended data should be shuffled and merged by BucketUnion") {
    df = spark.read.json(sampleJsonDataLocation)
    val query = df.filter(df("clicks") <= 2000).select(df("query"))

    withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "false") {
      val transformed = FilterIndexRule(query.queryExecution.optimizedPlan)
      assert(transformed.equals(query.queryExecution.optimizedPlan), "No plan transformation.")
    }

    withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "true") {
      val planWithHybridScan = FilterIndexRule(query.queryExecution.optimizedPlan)
      assert(
        !planWithHybridScan.equals(query.queryExecution.optimizedPlan),
        "Plan should be transformed.")

      // check appended file is added to relation node or not
      val nodes = planWithHybridScan collect {
        case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
          val appendedFileCnt = fsRelation.location.inputFiles.count(_.contains(".copy"))
          val indexFileCnt = fsRelation.location.inputFiles.count(_.contains("index2"))
          assert(appendedFileCnt === 1 || indexFileCnt === 4)
          assert(appendedFileCnt * indexFileCnt === 0)
          p
        case p @ Union(_) => p
      }
      assert(nodes.count(p => p.getClass.toString.contains("LogicalRelation")) === 2)
      assert(nodes.count(p => p.getClass.toString.contains("Union")) === 1)
      planWithHybridScan.foreach(f => assert(Try(f.asInstanceOf[Exchange]).isFailure))

      spark.enableHyperspace()
      val execPlan = spark.sessionState.executePlan(planWithHybridScan).executedPlan

      val execNodes = execPlan collect {
        case p @ UnionExec(children) =>
          assert(children.size === 2)
          // head is always the index plan
          assert(Try(children.head.asInstanceOf[WholeStageCodegenExec]).isSuccess)
          assert(Try(children.last.asInstanceOf[WholeStageCodegenExec]).isSuccess)
          p
        case p @ FileSourceScanExec(_, _, _, _, _, dataFilters, _) =>
          // check filter pushed down properly
          assert(dataFilters.toString.contains(" <= 2000)"))
          p
      }

      execPlan.foreach(p => assert(Try(p.asInstanceOf[ShuffleExchangeExec]).isFailure))
      assert(execNodes.count(p => p.getClass.toString.contains("UnionExec")) === 1)
      // 1 of index, 1 of appended file
      assert(execNodes.count(p => p.getClass.toString.contains("FileSourceScanExec")) === 2)

      val query2 = df.filter(df("clicks") <= 2000).select(df("query"))
      checkAnswer(query2, query)
    }
  }
}
