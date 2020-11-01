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
import org.apache.spark.sql.catalyst.expressions.{Attribute, EqualTo, In, InSet, Literal, Not}
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

trait HybridScanTestSuite extends QueryTest with HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/hybridScanTest")

  val sampleData = SampleData.testData
  protected val sampleDataLocationRoot = "src/test/resources/data/"
  protected val sampleDataFormatAppend = sampleDataLocationRoot + "sample0"
  protected val sampleDataFormatAppend2 = sampleDataLocationRoot + "sample1"
  protected val sampleDataFormatDelete = sampleDataLocationRoot + "sample2"
  protected val sampleDataFormatDelete2 = sampleDataLocationRoot + "sample3"
  protected val sampleDataFormatDelete3 = sampleDataLocationRoot + "sample4"
  protected val sampleDataFormatDelete4 = sampleDataLocationRoot + "sample5"
  protected val sampleDataFormatBoth = sampleDataLocationRoot + "sample6"
  protected val sampleDataFormat2Append = sampleDataLocationRoot + "sample7"
  protected val sampleDataFormat2Delete = sampleDataLocationRoot + "sample8"
  var hyperspace: Hyperspace = _

  // Introduce fileFormat to support "delta" format soon.
  val fileFormat = "parquet"
  val fileFormat2 = "json"

  // Construct a df with `sourceFileFormat` and `sourcePath` and create an index with the df and
  // given 'indexConfig'. Then copies the first 'appendCnt' number of input files from 'df' and
  // deletes the last 'deleteCnt' of the input files.
  def setupIndexAndChangeData(
      sourceFileFormat: String,
      sourcePath: String,
      indexConfig: IndexConfig,
      appendCnt: Int,
      deleteCnt: Int): Unit = {
    val df = spark.read.format(sourceFileFormat).load(sourcePath)
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
      fs.delete(new Path(inputFiles(inputFiles.length - i)), false)
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
    FileUtils.delete(new Path(sampleDataLocationRoot))
  }

  test(
    "Append-only: join rule, appended data should be shuffled with indexed columns " +
      "and merged by BucketUnion") {
    val df1 = spark.read.format(fileFormat).load(sampleDataFormatAppend)
    val df2 = spark.read.format(fileFormat).load(sampleDataFormatAppend2)
    def joinQuery(): DataFrame = {
      val query = df1.filter(df1("clicks") >= 2000).select(df1("clicks"), df1("query"))
      val query2 = df2.filter(df2("clicks") <= 4000).select(df2("clicks"), df2("Date"))
      query.join(query2, "clicks")
    }
    val baseQuery = joinQuery()
    val basePlan = baseQuery.queryExecution.optimizedPlan

    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val join = joinQuery()
        assert(basePlan.equals(join.queryExecution.optimizedPlan))
      }

      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
        val join = joinQuery()
        val planWithHybridScan = join.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHybridScan))

        // Check appended file is added to relation node or not.
        val nodes = planWithHybridScan.collect {
          case b @ BucketUnion(children, bucketSpec) =>
            assert(bucketSpec.numBuckets === 200)
            assert(
              bucketSpec.bucketColumnNames.size === 1 &&
                bucketSpec.bucketColumnNames.head === "clicks")

            val childNodes = children.collect {
              case r @ RepartitionByExpression(
                    attrs,
                    Project(_, Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))),
                    numBucket) =>
                assert(attrs.size === 1)
                assert(attrs.head.asInstanceOf[Attribute].name.contains("clicks"))
                // Check 1 appended file.
                assert(fsRelation.location.inputFiles.forall(f =>
                  f.contains(sampleDataFormatAppend) || f.contains(sampleDataFormatAppend2)))
                assert(fsRelation.location.inputFiles.length === 1)
                assert(numBucket === 200)
                r
              case p @ Project(
                    _,
                    Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))) =>
                // Check 4 of index data files.
                assert(
                  fsRelation.location.inputFiles.forall(_.contains("index_ParquetAppend"))
                    || fsRelation.location.inputFiles
                      .forall(_.contains("indexType2_ParquetAppend2")))
                assert(fsRelation.location.inputFiles.length === 4)
                p
            }

            // BucketUnion has 2 children.
            assert(childNodes.size === 2)
            assert(childNodes.count(_.isInstanceOf[Project]) === 1)
            assert(childNodes.count(_.isInstanceOf[RepartitionByExpression]) === 1)
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
            case p @ FileSourceScanExec(_, _, _, partitionFilters, _, dataFilters, _) =>
              // Check filter pushed down properly.
              if (partitionFilters.nonEmpty) {
                assert(
                  partitionFilters.toString.contains(" >= 2000)") && partitionFilters.toString
                    .contains(" <= 4000)"))
              } else {
                assert(
                  dataFilters.toString.contains(" >= 2000)") && dataFilters.toString.contains(
                    " <= 4000)"))
              }
              p
          }
          assert(execNodes.count(_.isInstanceOf[BucketUnionExec]) === 2)
          // 2 of index, 2 of appended file
          assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 4)
        }
        checkAnswer(join, baseQuery)
      }
    }
  }

  test(
    "Append-only: filter rule and non-parquet format," +
      "appended data should be shuffled and merged by Union") {
    val df = spark.read.format(fileFormat2).load(sampleDataFormat2Append)
    def filterQuery: DataFrame = df.filter(df("clicks") <= 2000).select(df("query"))
    val baseQuery = filterQuery
    val basePlan = baseQuery.queryExecution.optimizedPlan

    withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
      val filter = filterQuery
      assert(basePlan.equals(filter.queryExecution.optimizedPlan))
    }

    withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
      val filter = filterQuery
      val planWithHybridScan = filter.queryExecution.optimizedPlan
      assert(!basePlan.equals(planWithHybridScan))

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
                  assert(fsRelation.location.inputFiles.forall(_.contains("index_JsonAppend")))
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
          case p @ FileSourceScanExec(_, _, _, partitionFilters, _, dataFilters, _) =>
            // Check filter pushed down properly.
            assert(
              dataFilters.toString.contains(" <= 2000)") ||
                partitionFilters.toString.contains(" <= 2000)"))
            p
        }
        assert(execNodes.count(_.isInstanceOf[UnionExec]) === 1)
        // 1 of index, 1 of appended file
        assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 2)
        // Make sure there is no shuffle.
        execPlan.foreach(p => assert(!p.isInstanceOf[ShuffleExchangeExec]))
      }
      checkAnswer(baseQuery, filter)
    }
  }

  test("Delete-only: index relation should have additional filter for deleted files") {
    // Test for both file format
    Seq(
      (sampleDataFormatDelete, "index_ParquetDelete", "parquet"),
      (sampleDataFormat2Delete, "index_JsonDelete", "json")) foreach {
      case (dataPath, indexName, dataFormat) =>
        val df = spark.read.format(dataFormat).load(dataPath)
        def filterQuery: DataFrame =
          df.filter(df("clicks") <= 2000).select(df("query"))
        val baseQuery = filterQuery
        val basePlan = baseQuery.queryExecution.optimizedPlan

        withSQLConf(
          IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
          IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED -> "false") {
          val filter = filterQuery
          assert(basePlan.equals(filter.queryExecution.optimizedPlan))
        }

        withSQLConf(
          IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
          IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED -> "true") {
          val filter = filterQuery
          val planWithHybridScan = filter.queryExecution.optimizedPlan
          assert(!basePlan.equals(planWithHybridScan))

          val deletedFilesList = planWithHybridScan collect {
            case Filter(
                Not(In(attr, deletedFileNames)),
                LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
              // Check new filter condition on lineage column.
              assert(attr.toString.contains(IndexConstants.DATA_FILE_NAME_COLUMN))
              val deleted = deletedFileNames.map(_.toString)
              assert(deleted.length === 2)
              assert(deleted.distinct.length === deleted.length)
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
          assert(execNodes.length === 1)

          checkAnswer(baseQuery, filter)
        }
    }
  }

  test(
    "Delete-only: join rule, deleted files should be excluded from each index data" +
      " relation.") {
    val df1 = spark.read.format(fileFormat).load(sampleDataFormatDelete)
    val df2 = spark.read.format(fileFormat).load(sampleDataFormatDelete3)
    def joinQuery(): DataFrame = {
      val query = df1.filter(df1("clicks") >= 2000).select(df1("clicks"), df1("query"))
      val query2 = df2.filter(df2("clicks") <= 4000).select(df2("clicks"), df2("Date"))
      query.join(query2, "clicks")
    }
    val baseQuery = joinQuery()
    val basePlan = baseQuery.queryExecution.optimizedPlan

    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val join = joinQuery()
        assert(basePlan.equals(join.queryExecution.optimizedPlan))
      }

      withSQLConf(
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
        IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED -> "true") {
        val join = joinQuery()
        val planWithHybridScan = join.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHybridScan))

        val deletedFilesList = planWithHybridScan collect {
          case Filter(
              Not(In(attr, deletedFileNames)),
              LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
            // Check new filter condition on lineage column.
            assert(attr.toString.contains(IndexConstants.DATA_FILE_NAME_COLUMN))
            val deleted = deletedFileNames.map(_.toString)
            assert(deleted.length === 2)
            assert(deleted.distinct.length === deleted.length)
            assert(deleted.forall(f => !df1.inputFiles.contains(f)))
            assert(deleted.forall(f => !df2.inputFiles.contains(f)))
            assert(
              fsRelation.location.inputFiles.forall(_.contains("index_ParquetDelete")) ||
                fsRelation.location.inputFiles.forall(_.contains("indexType2_ParquetDelete3")))
            assert(fsRelation.location.inputFiles.length === 4)
            deleted
        }
        assert(deletedFilesList.length === 2)
        var deletedFiles = deletedFilesList.flatten
        assert(deletedFiles.length === 4)

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

  test("Delete-only: filter rule, number of delete files threshold") {
    val indexConfig = IndexConfig("index_ParquetDelete2", Seq("clicks"), Seq("query"))
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      setupIndexAndChangeData(
        fileFormat,
        sampleDataFormatDelete4,
        indexConfig.copy(indexName = "IndexDeleteCntTest"),
        appendCnt = 0,
        deleteCnt = 2)
    }

    val df = spark.read.format(fileFormat).load(sampleDataFormatDelete4)
    def filterQuery: DataFrame =
      df.filter(df("clicks") <= 2000).select(df("query"))
    val baseQuery = filterQuery
    val basePlan = baseQuery.queryExecution.optimizedPlan

    withSQLConf(
      IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
      IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED -> "true") {
      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_DELETE_MAX_NUM_FILES -> "2") {
        val filter = filterQuery
        // Since number of deletedFiles = 2, index can be applied with Hybrid scan.
        assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
      }
      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_DELETE_MAX_NUM_FILES -> "1") {
        val filter = filterQuery
        // Since number of deletedFiles = 2, index should not be applied even with Hybrid scan.
        assert(basePlan.equals(filter.queryExecution.optimizedPlan))
      }
    }
  }

  test(
    "Append+Delete: filter rule, appended files should be handled " +
      "with additional plan and merged by Union.") {
    val df = spark.read.format(fileFormat).load(sampleDataFormatBoth)
    def filterQuery: DataFrame =
      df.filter(df("clicks") <= 2000).select(df("query"))
    val baseQuery = filterQuery
    val basePlan = baseQuery.queryExecution.optimizedPlan

    withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
      val filter = filterQuery
      assert(basePlan.equals(filter.queryExecution.optimizedPlan))
    }

    withSQLConf(
      IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
      IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED -> "true") {
      val filter = filterQuery
      val planWithHybridScan = filter.queryExecution.optimizedPlan
      assert(!basePlan.equals(planWithHybridScan))

      var deletedFilesList: Seq[Seq[String]] = Nil
      val nodes = planWithHybridScan collect {
        // OptimizeIn would change `IN` to `EqualTo` if there is only one element in `In`.
        case p @ Filter(
              Not(EqualTo(left: Attribute, right: Literal)),
              LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
          // Check new filter condition on lineage column.
          val colName = left.toString
          val deletedFile = right.toString
          assert(colName.contains(IndexConstants.DATA_FILE_NAME_COLUMN))
          assert(!df.inputFiles.contains(deletedFile))
          assert(fsRelation.location.inputFiles.forall(_.contains("index_ParquetBoth")))
          assert(fsRelation.location.inputFiles.length === 4)
          deletedFilesList = deletedFilesList :+ Seq(deletedFile)
          p
        case p: Union =>
          p
        case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
          if (fsRelation.location.inputFiles.exists(_.contains(".copy"))) {
            // Check input files for appended files.
            assert(fsRelation.location.inputFiles.length === 1)
          } else {
            // Check input files for index data files.
            assert(fsRelation.location.inputFiles.forall(_.contains("index_ParquetBoth")))
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
              assert(!deletePushDownFilterFound)
              deletePushDownFilterFound = true
            }
            p
          case _: ShuffleExchangeExec =>
            // Make sure there is no shuffle.
            fail("ShuffleExchangeExec node found")
        }

        assert(execNodes.count(_.isInstanceOf[UnionExec]) === 1)
        // 1 of index, 1 of appended file
        assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 2)
        assert(deletePushDownFilterFound)

        checkAnswer(baseQuery, filter)
      }
    }
  }

  test(
    "Append+Delete: join rule, appended data should be shuffled with indexed columns " +
      "and merged by BucketUnion and deleted files are handled with index data.") {
    // One relation has both deleted & appended files and the other one has only deleted files.
    val df1 = spark.read.format(fileFormat).load(sampleDataFormatBoth)
    val df2 = spark.read.format(fileFormat).load(sampleDataFormatDelete3)
    def joinQuery(): DataFrame = {
      val query = df1.filter(df1("clicks") >= 2000).select(df1("clicks"), df1("query"))
      val query2 = df2.filter(df2("clicks") <= 4000).select(df2("clicks"), df2("Date"))
      query.join(query2, "clicks")
    }
    val baseQuery = joinQuery()
    val basePlan = baseQuery.queryExecution.optimizedPlan

    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val join = joinQuery()
        assert(basePlan.equals(join.queryExecution.optimizedPlan))
      }

      withSQLConf(
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
        IndexConstants.INDEX_HYBRID_SCAN_DELETE_ENABLED -> "true",
        "spark.sql.optimizer.inSetConversionThreshold" -> "1") {
        // Changed inSetConversionThreshould to check InSet optimization.
        val join = joinQuery()
        val planWithHybridScan = join.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHybridScan))

        var deletedFilesList: Seq[Seq[String]] = Nil
        val nodes = planWithHybridScan collect {
          case p @ Filter(
                Not(InSet(attr, deletedFileNames)),
                LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
            // Check new filter condition on lineage column.
            assert(attr.toString.contains(IndexConstants.DATA_FILE_NAME_COLUMN))
            // This node should be df2 - Filter-Not-InSet with 2 deleted files.
            assert(deletedFileNames.size === 2)
            val deleted = deletedFileNames.map(_.toString).toSeq
            assert(deleted.forall(f => !df2.inputFiles.contains(f)))
            assert(
              fsRelation.location.inputFiles.forall(_.contains("index_ParquetBoth")) ||
                fsRelation.location.inputFiles.forall(_.contains("indexType2_ParquetDelete3")))
            assert(fsRelation.location.inputFiles.length === 4)
            deletedFilesList = deletedFilesList :+ deleted
            p
          // OptimizeIn would change `IN` to `EqualTo` if there is only one element in `In`.
          case p @ Filter(
                Not(EqualTo(left: Attribute, right: Literal)),
                LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
            // Check new filter condition on lineage column.
            val colName = left.toString
            val deletedFile = right.toString
            // This node should be df1 - Filter-Not with 1 deleted files.
            assert(colName.contains(IndexConstants.DATA_FILE_NAME_COLUMN))
            assert(!df1.inputFiles.contains(deletedFile))
            assert(fsRelation.location.inputFiles.forall(_.contains("index_ParquetBoth")))
            assert(fsRelation.location.inputFiles.length === 4)
            deletedFilesList = deletedFilesList :+ Seq(deletedFile)
            p
          case p @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
            val appendedFileCnt = fsRelation.location.inputFiles.count(_.contains(".copy"))
            val indexFileCnt = fsRelation.location.inputFiles.count(_.contains("index"))
            if (appendedFileCnt > 0) {
              assert(appendedFileCnt === 1)
              assert(fsRelation.location.inputFiles.length === 1)
            } else {
              assert(
                fsRelation.location.inputFiles.forall(_.contains("index_ParquetBoth")) ||
                  fsRelation.location.inputFiles.forall(_.contains("indexType2_ParquetDelete3")))
              assert(indexFileCnt === 4)
              assert(fsRelation.location.inputFiles.length === 4)
            }
            p
          case p: BucketUnion => p
        }
        // 2 LogicalRelation for index with append+delete, 1 for index with delete.
        assert(nodes.count(_.isInstanceOf[LogicalRelation]) === 3)
        // 1 BucketUnion for index with append+delete.
        assert(nodes.count(_.isInstanceOf[BucketUnion]) === 1)
        // 2 Filter-Not-In nodes for index with delete.
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
          // Check all deleted files are present in Push-down filter condition.
          assert(deletedFiles.isEmpty)
          assert(execNodes.count(_.isInstanceOf[BucketUnionExec]) === 1)
          // 2 of index data, 1 of appended file.
          assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 3)
          assert(deleteFilesPushDownFilterCnt === 2)

          checkAnswer(baseQuery, join)
        }
      }
    }
  }
}
