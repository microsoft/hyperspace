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
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project, RepartitionByExpression, Union}
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec, UnionExec}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.DataSourceRegister

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData}
import com.microsoft.hyperspace.index.execution.BucketUnionExec
import com.microsoft.hyperspace.index.plans.logical.BucketUnion
import com.microsoft.hyperspace.util.FileUtils

class HybridScanTest extends QueryTest with HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/hybridScanTest")

  private val sampleData = SampleData.testData
  private val sampleDataRoot = "src/test/resources/data/"
  private val sampleParquetDataLocation = sampleDataRoot + "sampleparquet2"
  private val sampleParquetDataLocation2 = sampleDataRoot + "sampleparquet3"
  private val sampleJsonDataLocation = sampleDataRoot + "samplejson"
  private var hyperspace: Hyperspace = _
  private val indexConfig1 = IndexConfig("index1", Seq("clicks"), Seq("query"))
  private val indexConfig2 = IndexConfig("index2", Seq("clicks"), Seq("query"))
  private val indexConfig3 = IndexConfig("index3", Seq("clicks"), Seq("Date"))

  private def copyFirstFile(df: DataFrame): Unit = {
    val file = df.inputFiles.head
    val sourcePath = new Path(file)
    val destPath = new Path(file + ".copy")
    systemPath.getFileSystem(new Configuration).copyToLocalFile(sourcePath, destPath)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._
    hyperspace = new Hyperspace(spark)
    FileUtils.delete(new Path(sampleParquetDataLocation))
    FileUtils.delete(new Path(sampleJsonDataLocation))
    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    dfFromSample.write.parquet(sampleParquetDataLocation)
    dfFromSample.write.parquet(sampleParquetDataLocation2)
    dfFromSample.write.json(sampleJsonDataLocation)

    {
      val df = spark.read.parquet(sampleParquetDataLocation)
      hyperspace.createIndex(df, indexConfig1)
      copyFirstFile(df)
    }

    {
      val df = spark.read.parquet(sampleParquetDataLocation2)
      hyperspace.createIndex(df, indexConfig3)
      copyFirstFile(df)
    }

    {
      val df = spark.read.json(sampleJsonDataLocation)
      hyperspace.createIndex(df, indexConfig2)
      copyFirstFile(df)
    }
  }

  before {
    spark.enableHyperspace()
  }

  after {
    spark.disableHyperspace()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.delete(new Path(sampleDataRoot))
  }

  test(
    "Append-only: filter index & parquet format, " +
      "index relation should include appended file paths") {
    val df = spark.read.parquet(sampleParquetDataLocation)
    def filterQuery: DataFrame =
      df.filter(df("clicks") <= 2000).select(df("query"))
    val baseQuery = filterQuery

    withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "false") {
      val filter = filterQuery
      assert(baseQuery.queryExecution.optimizedPlan.equals(filter.queryExecution.optimizedPlan))
    }

    withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "true") {
      val filter = filterQuery
      val planWithHybridScan = filter.queryExecution.optimizedPlan
      assert(!baseQuery.queryExecution.optimizedPlan.equals(planWithHybridScan))

      // Check appended file is added to relation node or not.
      val nodes = planWithHybridScan collect {
        case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
          // Verify appended file is included or not.
          assert(fsRelation.location.inputFiles.count(_.contains(".copy")) === 1)
          // Verify number of index data files.
          assert(fsRelation.location.inputFiles.count(_.contains("index1")) === 4)
          p
      }
      // Filter Index and Parquet format source file can be handled with 1 LogicalRelation
      assert(nodes.length === 1)
      checkAnswer(baseQuery, filter)
    }
  }

  test(
    "Append-only: join index, appended data should be shuffled with indexed columns " +
      "and merged by BucketUnion") {
    val df1 = spark.read.parquet(sampleParquetDataLocation)
    val df2 = spark.read.parquet(sampleParquetDataLocation2)
    def joinQuery(): DataFrame = {
      val query = df1.filter(df1("clicks") >= 2000).select(df1("clicks"), df1("query"))
      val query2 = df2.filter(df2("clicks") <= 4000).select(df2("clicks"), df2("Date"))
      query.join(query2, "clicks")
    }
    val baseQuery = joinQuery()

    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "false") {
        val join = joinQuery()
        assert(join.queryExecution.optimizedPlan.equals(baseQuery.queryExecution.optimizedPlan))
      }

      withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "true") {
        val join = joinQuery()
        val planWithHybridScan = join.queryExecution.optimizedPlan
        assert(!baseQuery.queryExecution.optimizedPlan.equals(planWithHybridScan))

        // Check appended file is added to relation node or not.
        val nodes = planWithHybridScan collect {
          case b @ BucketUnion(children, bucketSpec) =>
            assert(bucketSpec.numBuckets === 200)
            assert(
              bucketSpec.bucketColumnNames.size == 1 && bucketSpec.bucketColumnNames.head
                .equals("clicks"))

            val childNodes = children collect {
              case r @ RepartitionByExpression(
                    _,
                    Project(_, Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))),
                    numBucket: Int) =>
                // Check 1 appended file.
                assert(fsRelation.location.inputFiles.count(_.contains(".copy")) === 1)
                assert(numBucket === 200)
                r
              case p @ Project(
                    _,
                    Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))) =>
                // Check 4 of index data files.
                assert(fsRelation.location.inputFiles.count(_.contains("index")) === 4)
                p
            }

            // BucketUnion has 2 children.
            assert(childNodes.size === 2)
            assert(childNodes.count(_.isInstanceOf[Project]) == 1)
            assert(childNodes.count(_.isInstanceOf[RepartitionByExpression]) == 1)
            b
        }
        // 2 BucketUnion in Join Rule v1.
        assert(nodes.count(_.isInstanceOf[BucketUnion]) === 2)

        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          val execPlan = spark.sessionState.executePlan(planWithHybridScan).executedPlan
          val execNodes = execPlan collect {
            case p @ BucketUnionExec(children, bucketSpec) =>
              assert(children.size === 2)
              // children.head is always the index plan.
              assert(children.head.isInstanceOf[ProjectExec])
              assert(children.last.isInstanceOf[ShuffleExchangeExec])
              assert(bucketSpec.numBuckets === 200)
              p
            case p @ FileSourceScanExec(_, _, _, _, _, dataFilters, _) =>
              // Check filter pushed down properly.
              assert(
                dataFilters.toString.contains(" >= 2000)") && dataFilters.toString.contains(
                  " <= 4000)"))
              p
          }
          assert(execNodes.count(_.isInstanceOf[BucketUnionExec]) === 2)
          // 2 of index, 2 of appended file
          assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 4)

          checkAnswer(join, baseQuery)
        }
      }
    }
  }

  test(
    "Append-only: filter rule & json format, " +
      "appended data should be shuffled and merged by Union") {
    val df = spark.read.json(sampleJsonDataLocation)
    def filterQuery: DataFrame = df.filter(df("clicks") <= 2000).select(df("query"))
    val baseQuery = filterQuery

    withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "false") {
      val filter = filterQuery
      assert(baseQuery.queryExecution.optimizedPlan.equals(filter.queryExecution.optimizedPlan))
    }

    withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "true") {
      val filter = filterQuery
      val planWithHybridScan = filter.queryExecution.optimizedPlan
      assert(!baseQuery.queryExecution.optimizedPlan.equals(planWithHybridScan))

      // Check appended file is added to relation node or not.
      val nodes = planWithHybridScan collect {
        case u @ Union(children) =>
          val formats = children collect {
            case Project(_, Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))) =>
              val fileFormatName = fsRelation.fileFormat match {
                case d: DataSourceRegister => d.shortName
                case _ => fail("Unexpected file format")
              }
              fileFormatName match {
                case "parquet" =>
                  // Check 4 of index data files.
                  assert(fsRelation.location.inputFiles.count(_.contains("index")) === 4)
                case "json" =>
                  // Check 1 appended file.
                  assert(fsRelation.location.inputFiles.count(_.contains(".copy")) === 1)
                case _ => fail("Unexpected file format")
              }
              fileFormatName
          }

          // Union has 2 children.
          assert(formats.size === 2)
          assert(formats.contains("parquet") && formats.contains("json"))
          u
      }
      assert(nodes.count(_.isInstanceOf[Union]) === 1)

      // Make sure there is no shuffle.
      planWithHybridScan.foreach(p => assert(!p.isInstanceOf[RepartitionByExpression]))

      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val execPlan = spark.sessionState.executePlan(planWithHybridScan).executedPlan
        val execNodes = execPlan collect {
          case p @ UnionExec(children) =>
            assert(children.size === 2)
            assert(children.head.isInstanceOf[ProjectExec]) // index data
            assert(children.last.isInstanceOf[ProjectExec]) // appended data
            p
          case p @ FileSourceScanExec(_, _, _, _, _, dataFilters, _) =>
            // Check filter pushed down properly.
            assert(dataFilters.toString.contains(" <= 2000)"))
            p
        }
        assert(execNodes.count(_.isInstanceOf[UnionExec]) === 1)
        // 1 of index, 1 of appended file
        assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 2)
        // Make sure there is no shuffle.
        execPlan.foreach(p => assert(!p.isInstanceOf[ShuffleExchangeExec]))

        checkAnswer(baseQuery, filter)
      }
    }
  }
}
