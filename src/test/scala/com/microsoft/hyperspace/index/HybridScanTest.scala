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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, In, Literal, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Project, RepartitionByExpression, Union}
import org.apache.spark.sql.execution.{FileSourceScanExec, InputAdapter, ProjectExec, UnionExec, WholeStageCodegenExec}
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
  private val sampleDataLocationRoot = "src/test/resources/data/"
  private val sampleParquetDataLocationAppend = sampleDataLocationRoot + "sampleparquet0"
  private val sampleParquetDataLocationAppend2 = sampleDataLocationRoot + "sampleparquet1"
  private val sampleParquetDataLocationDelete = sampleDataLocationRoot + "sampleparquet2"
  private val sampleParquetDataLocationDelete2 = sampleDataLocationRoot + "sampleparquet3"
  private val sampleParquetDataLocationDelete3 = sampleDataLocationRoot + "sampleparquet4"
  private val sampleParquetDataLocationBoth = sampleDataLocationRoot + "sampleparquet5"
  private val sampleJsonDataLocationAppend = sampleDataLocationRoot + "samplejson1"
  private val sampleJsonDataLocationDelete = sampleDataLocationRoot + "samplejson2"
  private var hyperspace: Hyperspace = _

  // Creates an index with given 'df' and 'indexConfig' and copies the first 'appendCnt'
  // number of input files from 'df'.
  def setupIndexAndChangeData(
      df: DataFrame,
      indexConfig: IndexConfig,
      appendCnt: Int,
      deleteCnt: Int): Unit = {
    hyperspace.createIndex(df, indexConfig)
    val inputFiles = df.inputFiles
    assert(appendCnt + deleteCnt < inputFiles.length)

    val fs = systemPath.getFileSystem(new Configuration)
    for (i <- 0 until appendCnt) {
      val sourcePath = new Path(inputFiles(i))
      val destPath = new Path(inputFiles(i) + ".copy")
      fs.copyToLocalFile(sourcePath, destPath)
    }
    for (i <- 1 to deleteCnt) {
      fs.delete(new Path(inputFiles(inputFiles.length - i)))
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    import spark.implicits._
    hyperspace = new Hyperspace(spark)
    FileUtils.delete(new Path(sampleDataLocationRoot))
    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    dfFromSample.write.parquet(sampleParquetDataLocationAppend)
    dfFromSample.write.parquet(sampleParquetDataLocationAppend2)
    dfFromSample.write.parquet(sampleParquetDataLocationDelete)
    dfFromSample.write.parquet(sampleParquetDataLocationDelete2)
    dfFromSample.write.parquet(sampleParquetDataLocationDelete3)
    dfFromSample.write.parquet(sampleParquetDataLocationBoth)
    dfFromSample.write.json(sampleJsonDataLocationAppend)
    dfFromSample.write.json(sampleJsonDataLocationDelete)

    val indexConfig1 = IndexConfig("index1", Seq("clicks"), Seq("query"))
    val indexConfig2 = IndexConfig("index11", Seq("clicks"), Seq("Date"))

    setupIndexAndChangeData(
      spark.read.parquet(sampleParquetDataLocationAppend),
      indexConfig1,
      appendCnt = 1,
      deleteCnt = 0)
    setupIndexAndChangeData(
      spark.read.parquet(sampleParquetDataLocationAppend2),
      indexConfig2,
      appendCnt = 1,
      deleteCnt = 0)
    setupIndexAndChangeData(
      spark.read.json(sampleJsonDataLocationAppend),
      indexConfig1.copy(indexName = "index4"),
      appendCnt = 1,
      deleteCnt = 0)

    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      setupIndexAndChangeData(
        spark.read.parquet(sampleParquetDataLocationDelete),
        indexConfig1.copy(indexName = "index2"),
        appendCnt = 0,
        deleteCnt = 2)
      setupIndexAndChangeData(
        spark.read.parquet(sampleParquetDataLocationDelete3),
        indexConfig2.copy(indexName = "index6"),
        appendCnt = 0,
        deleteCnt = 2)
      setupIndexAndChangeData(
        spark.read.parquet(sampleParquetDataLocationBoth),
        indexConfig1.copy(indexName = "index3"),
        appendCnt = 1,
        deleteCnt = 1)
      setupIndexAndChangeData(
        spark.read.json(sampleJsonDataLocationDelete),
        indexConfig1.copy(indexName = "index5"),
        appendCnt = 0,
        deleteCnt = 2)
    }
    setupIndexAndChangeData(
      spark.read.parquet(sampleParquetDataLocationDelete2),
      indexConfig1.copy(indexName = "index22"),
      appendCnt = 0,
      deleteCnt = 2)
  }

  before {
    spark.enableHyperspace()
  }

  after {
    spark.disableHyperspace()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    FileUtils.delete(new Path(sampleDataLocationRoot))
  }

  test(
    "Append-only: filter index & parquet format, " +
      "index relation should include appended file paths") {
    val df = spark.read.parquet(sampleParquetDataLocationAppend)
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
      val nodes = planWithHybridScan.collect {
        case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
          // Verify appended file is included or not.
          assert(fsRelation.location.inputFiles.count(_.contains(".copy")) === 1)
          // Verify number of index data files.
          assert(fsRelation.location.inputFiles.count(_.contains("index1")) === 4)
          assert(fsRelation.location.inputFiles.length === 5)
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
    val df1 = spark.read.parquet(sampleParquetDataLocationAppend)
    val df2 = spark.read.parquet(sampleParquetDataLocationAppend2)
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
        val nodes = planWithHybridScan.collect {
          case b @ BucketUnion(children, bucketSpec) =>
            assert(bucketSpec.numBuckets === 200)
            assert(
              bucketSpec.bucketColumnNames.size == 1 && bucketSpec.bucketColumnNames.head
                .equals("clicks"))

            val childNodes = children.collect {
              case r @ RepartitionByExpression(
                    attrs,
                    Project(_, Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))),
                    numBucket) =>
                assert(attrs.size == 1)
                assert(attrs.head.asInstanceOf[Attribute].name.contains("clicks"))
                // Check 1 appended file.
                assert(fsRelation.location.inputFiles.forall(_.contains(".copy")))
                assert(fsRelation.location.inputFiles.length === 1)
                assert(numBucket === 200)
                r
              case p @ Project(
                    _,
                    Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))) =>
                // Check 4 of index data files.
                assert(fsRelation.location.inputFiles.forall(_.contains("index")))
                assert(fsRelation.location.inputFiles.length === 4)
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
          val execNodes = execPlan.collect {
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
    val df = spark.read.json(sampleJsonDataLocationAppend)
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
      val nodes = planWithHybridScan.collect {
        case u @ Union(children) =>
          val formats = children.collect {
            case Project(_, Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))) =>
              val fileFormatName = fsRelation.fileFormat match {
                case d: DataSourceRegister => d.shortName
                case _ => fail("Unexpected file format")
              }
              fileFormatName match {
                case "parquet" =>
                  // Check 4 of index data files.
                  assert(fsRelation.location.inputFiles.forall(_.contains("index")))
                  assert(fsRelation.location.inputFiles.length === 4)
                case "json" =>
                  // Check 1 appended file.
                  assert(fsRelation.location.inputFiles.forall(_.contains(".copy")))
                  assert(fsRelation.location.inputFiles.length === 1)
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
        val execNodes = execPlan.collect {
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

  test(
    "Delete-only: filter index & parquet format, " +
      "Hybrid Scan for delete support doesn't work without linage column") {
    val df = spark.read.parquet(sampleParquetDataLocationDelete2)
    def filterQuery: DataFrame =
      df.filter(df("clicks") <= 2000).select(df("query"))
    val baseQuery = filterQuery

    withSQLConf(
      "spark.hyperspace.index.hybridscan.enabled" -> "true",
      "spark.hyperspace.index.hybridscan.delete.enabled" -> "false") {
      val filter = filterQuery
      assert(baseQuery.queryExecution.optimizedPlan.equals(filter.queryExecution.optimizedPlan))
    }

    withSQLConf(
      "spark.hyperspace.index.hybridscan.enabled" -> "true",
      "spark.hyperspace.index.hybridscan.delete.enabled" -> "true") {
      val filter = filterQuery
      assert(baseQuery.queryExecution.optimizedPlan.equals(filter.queryExecution.optimizedPlan))
    }
  }

  test(
    "Delete-only: filter index & parquet, json format, " +
      "index relation should have additional filter for deleted files") {

    Seq(
      (sampleParquetDataLocationDelete, "index2", "parquet"),
      (sampleJsonDataLocationDelete, "index5", "json")) foreach {
      case (dataPath, indexName, dataFormat) =>
        val df = spark.read.format(dataFormat).load(dataPath)
        def filterQuery: DataFrame =
          df.filter(df("clicks") <= 2000).select(df("query"))
        val baseQuery = filterQuery

        withSQLConf(
          "spark.hyperspace.index.hybridscan.enabled" -> "true",
          "spark.hyperspace.index.hybridscan.delete.enabled" -> "false") {
          val filter = filterQuery
          assert(
            baseQuery.queryExecution.optimizedPlan.equals(filter.queryExecution.optimizedPlan))
        }

        withSQLConf(
          "spark.hyperspace.index.hybridscan.enabled" -> "true",
          "spark.hyperspace.index.hybridscan.delete.enabled" -> "true") {
          val filter = filterQuery
          val planWithHybridScan = filter.queryExecution.optimizedPlan
          assert(!baseQuery.queryExecution.optimizedPlan.equals(planWithHybridScan))

          val deletedFilesList = planWithHybridScan collect {
            case Filter(
                Not(In(attr, deletedFileNames)),
                LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
              // Check new filter condition on linage column.
              assert(attr.toString.contains(IndexConstants.DATA_FILE_NAME_COLUMN))
              val deleted = deletedFileNames.map(_.toString)
              assert(deleted.length == 2)
              assert(deleted.distinct.length == deleted.length)
              assert(deleted.forall(f => !df.inputFiles.contains(f)))
              // Check the location is replaced with index data files properly.
              assert(fsRelation.location.inputFiles.forall(_.contains(indexName)))
              assert(fsRelation.location.inputFiles.length === 4)
              deleted
          }
          assert(deletedFilesList.length === 1)
          val deletedFiles = deletedFilesList.flatten
          assert(deletedFiles.length === 2)

          val execPlan = spark.sessionState.executePlan(planWithHybridScan).executedPlan
          val execNodes = execPlan collect {
            case p @ FileSourceScanExec(_, _, _, _, _, dataFilters, _) =>
              // Check filter pushed down properly.
              val filterStr = dataFilters.toString
              assert(filterStr.contains(" <= 2000)"))
              // Check deleted files.
              assert(deletedFiles.forall(filterStr.contains))
              p
          }
          assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 1)

          checkAnswer(baseQuery, filter)
        }
    }
  }

  test("Delete-only: join index, deleted files should be excluded from each index relation.") {
    val df1 = spark.read.parquet(sampleParquetDataLocationDelete)
    val df2 = spark.read.parquet(sampleParquetDataLocationDelete3)
    def joinQuery(): DataFrame = {
      val query = df1.filter(df1("clicks") >= 2000).select(df1("clicks"), df1("query"))
      val query2 = df2.filter(df2("clicks") <= 4000).select(df2("clicks"), df2("Date"))
      query.join(query2, "clicks")
    }
    val baseQuery = joinQuery

    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "false") {
        val join = joinQuery()
        assert(join.queryExecution.optimizedPlan.equals(baseQuery.queryExecution.optimizedPlan))
      }

      withSQLConf(
        "spark.hyperspace.index.hybridscan.enabled" -> "true",
        "spark.hyperspace.index.hybridscan.delete.enabled" -> "true") {
        val join = joinQuery()
        val planWithHybridScan = join.queryExecution.optimizedPlan
        assert(!baseQuery.queryExecution.optimizedPlan.equals(planWithHybridScan))

        val deletedFilesList = planWithHybridScan collect {
          case Filter(
              Not(In(attr, deletedFileNames)),
              LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
            // Check new filter condition on linage column.
            assert(attr.toString.contains(IndexConstants.DATA_FILE_NAME_COLUMN))
            val deleted = deletedFileNames.map(_.toString)
            assert(deleted.length == 2)
            assert(deleted.distinct.length == deleted.length)
            assert(deleted.forall(f => !df1.inputFiles.contains(f)))
            assert(deleted.forall(f => !df2.inputFiles.contains(f)))
            assert(fsRelation.location.inputFiles.forall(_.contains("index")))
            assert(fsRelation.location.inputFiles.length === 4)
            deleted
        }
        assert(deletedFilesList.length === 2)
        assert(deletedFilesList.flatten.length === 4)

        var deletedFiles = deletedFilesList.flatten

        val execPlan = spark.sessionState.executePlan(planWithHybridScan).executedPlan
        val execNodes = execPlan collect {
          case p @ FileSourceScanExec(_, _, _, _, _, dataFilters, _) =>
            // Check filter pushed down properly.
            val filterStr = dataFilters.toString
            assert(filterStr.contains(" >= 2000)") && filterStr.contains(" <= 4000)"))
            // Check deleted files in the push down condition and remove from the list.
            deletedFiles = deletedFiles.filterNot(filterStr.contains(_))
            p
        }
        assert(deletedFiles.isEmpty)
        assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 2)

        checkAnswer(join, baseQuery)
      }
    }
  }

  test(
    "Append+Delete: filter index & parquet format, " +
      "appended files should be handled with additional plan and merged by Union.") {
    val df = spark.read.parquet(sampleParquetDataLocationBoth)
    def filterQuery: DataFrame =
      df.filter(df("clicks") <= 2000).select(df("query"))
    val baseQuery = filterQuery

    withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "false") {
      val filter = filterQuery
      assert(baseQuery.queryExecution.optimizedPlan.equals(filter.queryExecution.optimizedPlan))
    }

    withSQLConf(
      "spark.hyperspace.index.hybridscan.enabled" -> "true",
      "spark.hyperspace.index.hybridscan.delete.enabled" -> "true") {
      val filter = filterQuery
      val planWithHybridScan = filter.queryExecution.optimizedPlan
      assert(!baseQuery.queryExecution.optimizedPlan.equals(planWithHybridScan))

      var deletedFilesList: Seq[Seq[String]] = Nil
      val nodes = planWithHybridScan collect {
        case p @ Filter(
              Not(In(attr, deletedFileNames)),
              LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
          // Check new filter condition on linage column.
          assert(attr.toString.contains(IndexConstants.DATA_FILE_NAME_COLUMN))
          val deleted = deletedFileNames.map(_.toString)
          assert(deleted.length == 1)
          assert(deleted.distinct.length == deleted.length)
          assert(deleted.forall(f => !df.inputFiles.contains(f)))
          assert(fsRelation.location.inputFiles.forall(_.contains("index3")))
          assert(fsRelation.location.inputFiles.length === 4)
          deletedFilesList = deletedFilesList :+ deleted
          p
        case p @ Union(_) =>
          p
        case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
          if (fsRelation.location.inputFiles.exists(_.contains(".copy"))) {
            assert(fsRelation.location.inputFiles.length === 1)
          } else {
            assert(fsRelation.location.inputFiles.forall(_.contains("index3")))
            assert(fsRelation.location.inputFiles.length === 4)
          }
          p
      }
      assert(nodes.count(_.isInstanceOf[Filter]) === 1)
      assert(nodes.count(_.isInstanceOf[Union]) === 1)
      assert(nodes.count(_.isInstanceOf[LogicalRelation]) === 2)

      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val execPlan = spark.sessionState.executePlan(planWithHybridScan).executedPlan
        var deletePushDownFilterFound = false
        val execNodes = execPlan collect {
          case p @ UnionExec(children) =>
            assert(children.size === 2)
            assert(children.head.isInstanceOf[ProjectExec]) // index data
            assert(children.last.isInstanceOf[ProjectExec]) // appended data
            p
          case p @ FileSourceScanExec(_, _, _, _, _, dataFilters, _) =>
            // Check filter pushed down properly.
            val filterStr = dataFilters.toString
            assert(filterStr.contains(" <= 2000)"))
            if (filterStr.contains(IndexConstants.DATA_FILE_NAME_COLUMN)) {
              assert(deletedFilesList.flatten.forall(filterStr.contains(_)))
              deletePushDownFilterFound = true
            }
            p
        }

        // Make sure there is no shuffle.
        execPlan.foreach(p => assert(!p.isInstanceOf[ShuffleExchangeExec]))
        assert(execNodes.count(_.isInstanceOf[UnionExec]) === 1)
        // 1 of index, 1 of appended file
        assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 2)
        assert(deletePushDownFilterFound)

        checkAnswer(baseQuery, filter)
      }
    }
  }

  test(
    "Append+Delete: join index, appended data should be shuffled with indexed columns " +
      "and merged by BucketUnion and deleted files are handled with index data.") {
    val df1 = spark.read.parquet(sampleParquetDataLocationBoth)
    val df2 = spark.read.parquet(sampleParquetDataLocationDelete3)
    def joinQuery(): DataFrame = {
      val query = df1.filter(df1("clicks") >= 2000).select(df1("clicks"), df1("query"))
      val query2 = df2.filter(df2("clicks") <= 4000).select(df2("clicks"), df2("Date"))
      query.join(query2, "clicks")
    }
    val baseQuery = joinQuery

    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withSQLConf("spark.hyperspace.index.hybridscan.enabled" -> "false") {
        val join = joinQuery()
        assert(join.queryExecution.optimizedPlan.equals(baseQuery.queryExecution.optimizedPlan))
      }

      withSQLConf(
        "spark.hyperspace.index.hybridscan.enabled" -> "true",
        "spark.hyperspace.index.hybridscan.delete.enabled" -> "true") {
        val join = joinQuery()
        val planWithHybridScan = join.queryExecution.optimizedPlan
        assert(!baseQuery.queryExecution.optimizedPlan.equals(planWithHybridScan))

        var deletedFilesList: Seq[Seq[String]] = Nil
        val nodes = planWithHybridScan collect {
          case p @ Filter(
                Not(In(attr, deletedFileNames)),
                LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
            // Check new filter condition on linage column.
            assert(attr.toString.contains(IndexConstants.DATA_FILE_NAME_COLUMN))
            assert(deletedFileNames.length == 1 || deletedFileNames.length == 2)
            val deleted = deletedFileNames.map(_.toString)
            assert(deleted.distinct.length == deleted.length)
            assert(deleted.forall(f => !df1.inputFiles.contains(f)))
            assert(deleted.forall(f => !df2.inputFiles.contains(f)))
            assert(fsRelation.location.inputFiles.forall(_.contains("index")))
            assert(fsRelation.location.inputFiles.length === 4)
            deletedFilesList = deletedFilesList :+ deleted
            p
          case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
            val appendedFileCnt = fsRelation.location.inputFiles.count(_.contains(".copy"))
            val indexFileCnt = fsRelation.location.inputFiles.count(_.contains("index"))
            if (appendedFileCnt > 0) {
              assert(appendedFileCnt === 1)
              assert(fsRelation.location.inputFiles.length === 1)
            } else {
              assert(indexFileCnt === 4)
              assert(fsRelation.location.inputFiles.length === 4)
            }
            p
          case p @ BucketUnion(_, _) => p
        }
        assert(nodes.count(_.isInstanceOf[LogicalRelation]) === 3)
        assert(nodes.count(_.isInstanceOf[BucketUnion]) === 1)
        assert(nodes.count(_.isInstanceOf[Filter]) === 2)
        assert(deletedFilesList.length === 2)
        var deletedFiles = deletedFilesList.flatten
        assert(deletedFiles.length === 3)

        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
          val execPlan = spark.sessionState.executePlan(planWithHybridScan).executedPlan
          var deleteFilesPushDownFilterCnt = 0
          val execNodes = execPlan collect {
            case p @ BucketUnionExec(children, bucketSpec) =>
              assert(children.size === 2)
              // children.head is always the index plan.
              assert(children.head.isInstanceOf[ProjectExec]) // index data
              assert(children.last.isInstanceOf[ShuffleExchangeExec]) // appended data
              assert(bucketSpec.bucketColumnNames.length === 1)
              assert(bucketSpec.bucketColumnNames.head.equals("clicks"))
              assert(bucketSpec.numBuckets === 200)
              p
            case p @ FileSourceScanExec(_, _, _, _, _, dataFilters, _) =>
              // Check filter pushed down properly.
              val filterStr = dataFilters.toString
              assert(filterStr.contains(" >= 2000)") && filterStr.contains(" <= 4000)"))
              // Check deleted files.
              if (filterStr.contains(IndexConstants.DATA_FILE_NAME_COLUMN)) {
                deletedFiles = deletedFiles.filterNot(filterStr.contains(_))
                deleteFilesPushDownFilterCnt += 1
              }
              p
          }
          assert(deletedFiles.isEmpty)
          assert(execNodes.count(_.isInstanceOf[BucketUnionExec]) === 1)
          // 2 of index, 1 of appended file
          assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 3)
          assert(deleteFilesPushDownFilterCnt == 2)

          checkAnswer(baseQuery, join)
        }
      }
    }
  }
}
