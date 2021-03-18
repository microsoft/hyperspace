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
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project, RepartitionByExpression, Union}
import org.apache.spark.sql.execution.{FileSourceScanExec, ProjectExec, UnionExec}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import com.microsoft.hyperspace._
import com.microsoft.hyperspace.{Hyperspace, SampleNestedData, TestConfig}
import com.microsoft.hyperspace.TestUtils.{latestIndexLogEntry, logManager}
import com.microsoft.hyperspace.index.execution.BucketUnionExec
import com.microsoft.hyperspace.index.plans.logical.BucketUnion
import com.microsoft.hyperspace.util.{FileUtils, SchemaUtils}

// Hybrid Scan tests for non partitioned source data. Test cases of HybridScanSuite are also
// executed with non partitioned source data.
class HybridScanForNestedFieldsTest extends QueryTest with HyperspaceSuite {
  override val systemPath = new Path("src/test/resources/hybridScanTestNestedFields")
  import spark.implicits._

  val sampleNestedData = SampleNestedData.testData
  val fileFormat = "parquet"
  val fileFormat2 = "json"
  val nestedDf = sampleNestedData.toDF("Date", "RGUID", "Query", "imprs", "clicks", "nested")

  val indexConfig1 =
    IndexConfig("index1", Seq("nested.leaf.cnt"), Seq("query", "nested.leaf.id"))
  val indexConfig2 =
    IndexConfig("index2", Seq("nested.leaf.cnt"), Seq("Date", "nested.leaf.id"))
  var hyperspace: Hyperspace = _

  def normalizePaths(in: Seq[String]): Seq[String] = {
    in.map(_.replace("file:///", "file:/"))
  }
  def equalNormalizedPaths(a: Seq[String], b: Seq[String]): Boolean = {
    normalizePaths(a).toSet === normalizePaths(b).toSet
  }

  def setupIndexAndChangeData(
      sourceFileFormat: String,
      sourcePath: String,
      indexConfig: IndexConfig,
      appendCnt: Int,
      deleteCnt: Int): (Seq[String], Seq[String]) = {
    nestedDf.write.format(sourceFileFormat).save(sourcePath)
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

    val df2 = spark.read.format(sourceFileFormat).load(sourcePath)
    (df2.inputFiles diff inputFiles, inputFiles diff df2.inputFiles)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    hyperspace = new Hyperspace(spark)
  }

  before {
    spark.conf.set(IndexConstants.INDEX_LINEAGE_ENABLED, "true")
    spark.enableHyperspace()
  }

  after {
    FileUtils.delete(systemPath)
    spark.disableHyperspace()
  }

  def checkDeletedFiles(
      plan: LogicalPlan,
      indexName: String,
      expectedDeletedFileNames: Seq[String]): Unit = {

    val fileNameToId = getLatestStableLog(indexName).fileIdTracker.getFileToIdMap.toSeq.map {
      kv =>
        (kv._1._1, kv._2)
    }.toMap

    val expectedDeletedFiles =
      expectedDeletedFileNames.map(f => fileNameToId(f.replace("file:///", "file:/")).toString)

    if (expectedDeletedFiles.nonEmpty) {
      log
      val inputFiles = plan.collect {
        case LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
          fsRelation.inputFiles.toSeq
      }.flatten
      val deletedFilesList = plan collect {
        case Filter(
            Not(EqualTo(left: Attribute, right: Literal)),
            LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
          // Check new filter condition on lineage column.
          val colName = left.toString
          val deletedFile = right.toString
          assert(colName.toString.contains(IndexConstants.DATA_FILE_NAME_ID))
          val deleted = Seq(deletedFile)
          assert(expectedDeletedFiles.length == 1)
          // Check the location is replaced with index data files properly.
          val files = fsRelation.location.inputFiles
          assert(files.nonEmpty && files.forall(_.contains(indexName)))
          deleted
        case Filter(
            Not(InSet(attr, deletedFileIds)),
            LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
          // Check new filter condition on lineage column.
          assert(attr.toString.contains(IndexConstants.DATA_FILE_NAME_ID))
          val deleted = deletedFileIds.map(_.toString).toSeq
          assert(
            expectedDeletedFiles.length > spark.conf
              .get("spark.sql.optimizer.inSetConversionThreshold")
              .toLong)
          // Check the location is replaced with index data files properly.
          val files = fsRelation.location.inputFiles
          assert(files.nonEmpty && files.forall(_.contains(indexName)))
          deleted
        case Filter(
            Not(In(attr, deletedFileIds)),
            LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
          // Check new filter condition on lineage column.
          assert(attr.toString.contains(IndexConstants.DATA_FILE_NAME_ID))
          val deleted = deletedFileIds.map(_.toString)
          assert(
            expectedDeletedFiles.length <= spark.conf
              .get("spark.sql.optimizer.inSetConversionThreshold")
              .toLong)
          // Check the location is replaced with index data files properly.
          val files = fsRelation.location.inputFiles
          assert(files.nonEmpty && files.forall(_.contains(indexName)))
          deleted
      }
      assert(deletedFilesList.length === 1)
      val deletedFiles = deletedFilesList.flatten
      assert(deletedFiles.length === expectedDeletedFiles.size)
      assert(deletedFiles.distinct.length === deletedFiles.length)
      assert(deletedFiles.forall(f => !inputFiles.contains(f)))
      assert(equalNormalizedPaths(deletedFiles, expectedDeletedFiles))

      val execPlan = spark.sessionState.executePlan(plan).executedPlan
      val execNodes = execPlan collect {
        case p @ FileSourceScanExec(_, _, _, _, _, dataFilters, _) =>
          // Check deleted files.
          assert(deletedFiles.forall(dataFilters.toString.contains))
          p
      }
      assert(execNodes.length === 1)
    }
  }

  def checkJoinIndexHybridScan(
      plan: LogicalPlan,
      leftIndexName: String,
      leftAppended: Seq[String],
      leftDeleted: Seq[String],
      rightIndexName: String,
      rightAppended: Seq[String],
      rightDeleted: Seq[String],
      filterConditions: Seq[String] = Nil): Unit = {
    // Project - Join - children
    val left = plan.children.head.children.head
    val right = plan.children.head.children.last

    // Check deleted files with the first child of each left and right child.
    checkDeletedFiles(left.children.head, leftIndexName, leftDeleted)
    checkDeletedFiles(right.children.head, rightIndexName, rightDeleted)

    val leftNodes = left.collect {
      case b @ BucketUnion(children, bucketSpec) =>
        assert(bucketSpec.numBuckets === 200)
        assert(
          bucketSpec.bucketColumnNames.size === 1 &&
            bucketSpec.bucketColumnNames.head === "cnt")

        val childNodes = children.collect {
          case r @ RepartitionByExpression(
                attrs,
                Project(_, Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))),
                numBucket) =>
            assert(attrs.size === 1)
            assert(attrs.head.asInstanceOf[Attribute].name.contains("cnt"))

            // Check appended file.
            val files = fsRelation.location.inputFiles
            assert(equalNormalizedPaths(files, leftAppended))
            assert(files.length === leftAppended.length)
            assert(numBucket === 200)
            r
          case p @ Project(_, Filter(_, _)) =>
            val files = p collect {
              case LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
                fsRelation.location.inputFiles
            }
            assert(files.nonEmpty && files.flatten.forall(_.contains(leftIndexName)))
            p
        }

        // BucketUnion has 2 children.
        assert(childNodes.size === 2)
        assert(childNodes.count(_.isInstanceOf[Project]) === 1)
        assert(childNodes.count(_.isInstanceOf[RepartitionByExpression]) === 1)
        b
    }

    val rightNodes = right.collect {
      case b @ BucketUnion(children, bucketSpec) =>
        assert(bucketSpec.numBuckets === 200)
        assert(
          bucketSpec.bucketColumnNames.size === 1 &&
            bucketSpec.bucketColumnNames.head === "cnt")

        val childNodes = children.collect {
          case r @ RepartitionByExpression(
                attrs,
                Project(_, Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))),
                numBucket) =>
            assert(attrs.size === 1)
            assert(attrs.head.asInstanceOf[Attribute].name.contains("cnt"))

            // Check appended files.
            val files = fsRelation.location.inputFiles
            assert(equalNormalizedPaths(files, rightAppended))
            assert(files.length === rightAppended.length)
            assert(numBucket === 200)
            r
          case p @ Project(
                _,
                Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))) =>
            // Check index data files.
            val files = fsRelation.location.inputFiles
            assert(files.nonEmpty && files.forall(_.contains(rightIndexName)))
            p
        }

        // BucketUnion has 2 children.
        assert(childNodes.size === 2)
        assert(childNodes.count(_.isInstanceOf[Project]) === 1)
        assert(childNodes.count(_.isInstanceOf[RepartitionByExpression]) === 1)
        b
    }

    // Check BucketUnion node if needed.
    assert(leftAppended.isEmpty || leftNodes.count(_.isInstanceOf[BucketUnion]) === 1)
    assert(rightAppended.isEmpty || rightNodes.count(_.isInstanceOf[BucketUnion]) === 1)

    withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
      val execPlan = spark.sessionState.executePlan(plan).executedPlan
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
            assert(filterConditions.forall(partitionFilters.toString.contains))
          } else {
            assert(filterConditions.forall(dataFilters.toString.contains))
          }
          p
      }
      var requiredBucketUnion = 0
      if (leftAppended.nonEmpty) requiredBucketUnion += 1
      if (rightAppended.nonEmpty) requiredBucketUnion += 1
      assert(execNodes.count(_.isInstanceOf[BucketUnionExec]) === requiredBucketUnion)
      // 2 of index data and number of indexes with appended files.
      assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 2 + requiredBucketUnion)
    }
  }

  def checkFilterIndexHybridScanUnion(
      plan: LogicalPlan,
      indexName: String,
      expectedAppendedFiles: Seq[String] = Nil,
      expectedDeletedFiles: Seq[String] = Nil,
      filterConditions: Seq[String] = Nil): Unit = {
    // The first child should be the index data scan; thus check if the deleted files are handled
    // properly with the first child plan.
    checkDeletedFiles(plan.children.head, indexName, expectedDeletedFiles)

    if (expectedAppendedFiles.nonEmpty) {
      val nodes = plan.collect {
        case u @ Union(children) =>
          val indexChild = children.head
          indexChild collect {
            case LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
              assert(fsRelation.location.inputFiles.forall(_.contains(indexName)))
          }

          assert(children.tail.size === 1)
          val appendChild = children.last
          appendChild collect {
            case LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
              val files = fsRelation.location.inputFiles
              assert(equalNormalizedPaths(files, expectedAppendedFiles))
              assert(files.length === expectedAppendedFiles.size)
          }
          u
      }
      assert(nodes.count(_.isInstanceOf[Union]) === 1)

      // Make sure there is no shuffle.
      plan.foreach(p => assert(!p.isInstanceOf[RepartitionByExpression]))

      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val execPlan = spark.sessionState.executePlan(plan).executedPlan
        val execNodes = execPlan.collect {
          case p @ UnionExec(children) =>
            assert(children.size === 2)
            assert(children.head.isInstanceOf[ProjectExec]) // index data
            assert(children.last.isInstanceOf[ProjectExec]) // appended data
            p
          case p @ FileSourceScanExec(_, _, _, partitionFilters, _, dataFilters, _) =>
            // Check filter pushed down properly.
            if (partitionFilters.nonEmpty) {
              assert(filterConditions.forall(partitionFilters.toString.contains))
            } else {
              assert(filterConditions.forall(dataFilters.toString.contains))
            }
            p
        }
        assert(execNodes.count(_.isInstanceOf[UnionExec]) === 1)
        // 1 of index, 1 of appended file
        assert(execNodes.count(_.isInstanceOf[FileSourceScanExec]) === 2)
        // Make sure there is no shuffle.
        execPlan.foreach(p => assert(!p.isInstanceOf[ShuffleExchangeExec]))
      }
    }
  }

  private def getLatestStableLog(indexName: String): IndexLogEntry = {
    val entry = logManager(systemPath, indexName).getLatestStableLog()
    assert(entry.isDefined)
    assert(entry.get.isInstanceOf[IndexLogEntry])
    entry.get.asInstanceOf[IndexLogEntry]
  }

  test(
    "Append-only: union over index and new files " +
      "due to field names being different: `nested__leaf__cnt` + `nested.leaf.cnt`.") {
    // This flag is for testing plan transformation if appended files could be load with index
    // data scan node. Currently, it's applied for a very specific case: FilterIndexRule,
    // Parquet source format, no partitioning, no deleted files.
    withTempPathAsString { testPath =>
      val (appendedFiles, _) = setupIndexAndChangeData(
        "parquet",
        testPath,
        indexConfig1.copy(indexName = "index_Append"),
        appendCnt = 1,
        deleteCnt = 0)

      val df = spark.read.format("parquet").load(testPath)
      def filterQuery: DataFrame =
        df.filter(df("nested.leaf.cnt") <= 20).select(df("query"))

      val baseQuery = filterQuery
      val basePlan = baseQuery.queryExecution.optimizedPlan

      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val filter = filterQuery
        assert(basePlan.equals(filter.queryExecution.optimizedPlan))
      }

      withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
        val filter = filterQuery
        val planWithHybridScan = filter.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHybridScan))

        // Check appended file is added to relation node or not.
        val nodes = planWithHybridScan.collect {
          case u @ Union(children) =>
            val indexChild = children.head
            indexChild collect {
              case LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
                assert(fsRelation.location.inputFiles.forall(_.contains("index_Append")))
            }

            assert(children.tail.size === 1)
            val appendChild = children.last
            appendChild collect {
              case LogicalRelation(fsRelation: HadoopFsRelation, _, _, _) =>
                val files = fsRelation.location.inputFiles
                assert(files.toSeq == appendedFiles)
                assert(files.length === appendedFiles.size)
            }
            u
        }

        // Filter Index and Parquet format source file can be handled with 1 LogicalRelation
        assert(nodes.length === 1)
        val left = baseQuery.collect().map(_.getString(0))
        val right = filter.collect().map(_.getString(0))
        assert(left.diff(right).isEmpty)
        assert(right.diff(left).isEmpty)
      }
    }
  }

  test("Delete-only: Hybrid Scan for delete support doesn't work without lineage column.") {
    val indexConfig = IndexConfig("index_ParquetDelete2", Seq("nested.leaf.cnt"), Seq("query"))
    Seq(("indexWithoutLineage", "false", false), ("indexWithLineage", "true", true)) foreach {
      case (indexName, lineageColumnConfig, transformationExpected) =>
        withTempPathAsString { testPath =>
          withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> lineageColumnConfig) {
            setupIndexAndChangeData(
              fileFormat,
              testPath,
              indexConfig.copy(indexName = indexName),
              appendCnt = 0,
              deleteCnt = 1)

            val df = spark.read.format(fileFormat).load(testPath)

            def filterQuery: DataFrame =
              df.filter(df("nested.leaf.cnt") <= 20).select(df("query"))

            val baseQuery = filterQuery
            val basePlan = baseQuery.queryExecution.optimizedPlan
            withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
              val filter = filterQuery
              assert(basePlan.equals(filter.queryExecution.optimizedPlan))
            }
            withSQLConf(TestConfig.HybridScanEnabled: _*) {
              val filter = filterQuery
              assert(
                basePlan
                  .equals(filter.queryExecution.optimizedPlan)
                  .equals(!transformationExpected))
            }
          }
        }
    }
  }

  test("Delete-only: filter rule, number of delete files threshold.") {
    withTempPathAsString { testPath =>
      val indexName = "IndexDeleteCntTest"
      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        setupIndexAndChangeData(
          fileFormat,
          testPath,
          indexConfig1.copy(indexName = indexName),
          appendCnt = 0,
          deleteCnt = 2)
      }

      val df = spark.read.format(fileFormat).load(testPath)
      def filterQuery: DataFrame =
        df.filter(df("nested.leaf.cnt") <= 20).select(df("query"))
      val baseQuery = filterQuery
      val basePlan = baseQuery.queryExecution.optimizedPlan
      val sourceSize = latestIndexLogEntry(systemPath, indexName).sourceFilesSizeInBytes

      val afterDeleteSize = FileUtils.getDirectorySize(new Path(testPath))
      val deletedRatio = 1 - (afterDeleteSize / sourceSize.toFloat)

      withSQLConf(TestConfig.HybridScanEnabled: _*) {
        withSQLConf(
          IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD ->
            (deletedRatio + 0.1).toString) {
          val filter = filterQuery
          // As deletedRatio is less than the threshold, the index can be applied.
          assert(!basePlan.equals(filter.queryExecution.optimizedPlan))
        }
        withSQLConf(
          IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD ->
            (deletedRatio - 0.1).toString) {
          val filter = filterQuery
          // As deletedRatio is greater than the threshold, the index shouldn't be applied.
          assert(basePlan.equals(filter.queryExecution.optimizedPlan))
        }
      }
    }
  }

  test(
    "Append-only: join rule, appended data should be shuffled with indexed columns " +
      "and merged by BucketUnion.") {
    withTempPathAsString { testPath =>
      val appendPath1 = testPath + "/append1"
      val appendPath2 = testPath + "/append2"
      val leftIndexName = "index_Append"
      val rightIndexName = "indexType2_Append"
      val (leftAppended, leftDeleted) = setupIndexAndChangeData(
        fileFormat,
        appendPath1,
        indexConfig1.copy(indexName = leftIndexName),
        appendCnt = 1,
        deleteCnt = 0)
      val (rightAppended, rightDeleted) = setupIndexAndChangeData(
        fileFormat,
        appendPath2,
        indexConfig2.copy(indexName = rightIndexName),
        appendCnt = 1,
        deleteCnt = 0)

      val df1 = spark.read.format(fileFormat).load(appendPath1)
      val df2 = spark.read.format(fileFormat).load(appendPath2)
      def joinQuery(): DataFrame = {
        val query2 = df1
          .filter(df1("nested.leaf.cnt") >= 20)
          .select(df1("nested.leaf.cnt"), df1("query"), df1("nested.leaf.id"))
        val query = df2
          .filter(df2("nested.leaf.cnt") <= 40)
          .select(df2("nested.leaf.cnt"), df2("Date"), df2("nested.leaf.id"))
        query2.join(query, "cnt")
      }
      val baseQuery = joinQuery()
      val basePlan = baseQuery.queryExecution.optimizedPlan

      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
          val join = joinQuery()
          checkAnswer(join, baseQuery)
        }

        withSQLConf(TestConfig.HybridScanEnabled: _*) {
          val join = joinQuery()
          val planWithHybridScan = join.queryExecution.optimizedPlan
          assert(!basePlan.equals(planWithHybridScan))
          checkJoinIndexHybridScan(
            planWithHybridScan,
            leftIndexName,
            leftAppended,
            leftDeleted,
            rightIndexName,
            rightAppended,
            rightDeleted,
            Seq(">= 20", "<= 40"))
          checkAnswer(join, baseQuery)
        }
      }
    }
  }

  test(
    "Append-only: filter rule and non-parquet format," +
      "appended data should be shuffled and merged by Union.") {
    // Note: for delta lake, this test is also eligible as the dataset is partitioned.
    withTempPathAsString { testPath =>
      val (appendedFiles, deletedFiles) = setupIndexAndChangeData(
        fileFormat2,
        testPath,
        indexConfig1.copy(indexName = "index_Format2Append"),
        appendCnt = 1,
        deleteCnt = 0)

      val df = spark.read.format(fileFormat2).load(testPath)
      def filterQuery: DataFrame = df.filter(df("nested.leaf.cnt") <= 20).select(df("query"))

      val baseQuery = filterQuery
      val basePlan = baseQuery.queryExecution.optimizedPlan

      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val filter = filterQuery
        assert(basePlan.equals(filter.queryExecution.optimizedPlan))
      }

      withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
        val filter = filterQuery
        val planWithHybridScan = filter.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHybridScan))

        checkFilterIndexHybridScanUnion(
          planWithHybridScan,
          "index_Format2Append",
          appendedFiles,
          deletedFiles,
          Seq(" <= 20"))

        // Check bucketSpec is not used.
        val bucketSpec = planWithHybridScan collect {
          case LogicalRelation(HadoopFsRelation(_, _, _, bucketSpec, _, _), _, _, _) =>
            bucketSpec
        }
        assert(bucketSpec.length == 2)

        // bucketSpec.head is for the index plan, bucketSpec.last is for the plan
        // for appended files.
        assert(bucketSpec.head.isEmpty && bucketSpec.last.isEmpty)

        checkAnswer(baseQuery, filter)
      }
    }
  }

  test(
    "Append-only: filter rule and non-parquet format," +
      "appended data should be shuffled and merged by Union even with bucketSpec.") {
    withTempPathAsString { testPath =>
      val (appendedFiles, deletedFiles) = setupIndexAndChangeData(
        fileFormat2,
        testPath,
        indexConfig1.copy(indexName = "index_Format2Append"),
        appendCnt = 1,
        deleteCnt = 0)

      val df = spark.read.format(fileFormat2).load(testPath)
      def filterQuery: DataFrame = df.filter(df("nested.leaf.cnt") <= 20).select(df("query"))

      val baseQuery = filterQuery
      val basePlan = baseQuery.queryExecution.optimizedPlan

      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val filter = filterQuery
        assert(basePlan.equals(filter.queryExecution.optimizedPlan))
      }

      withSQLConf(
        TestConfig.HybridScanEnabledAppendOnly :+
          IndexConstants.INDEX_FILTER_RULE_USE_BUCKET_SPEC -> "true": _*) {
        val filter = filterQuery
        val planWithHybridScan = filter.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHybridScan))

        checkFilterIndexHybridScanUnion(
          planWithHybridScan,
          "index_Format2Append",
          appendedFiles,
          deletedFiles,
          Seq(" <= 20"))

        // Check bucketSpec is used.
        val bucketSpec = planWithHybridScan collect {
          case LogicalRelation(HadoopFsRelation(_, _, _, bucketSpec, _, _), _, _, _) =>
            bucketSpec
        }
        assert(bucketSpec.length == 2)
        // bucketSpec.head is for the index plan, bucketSpec.last is for the plan
        // for appended files.
        assert(bucketSpec.head.isDefined && bucketSpec.last.isEmpty)
        assert(
          bucketSpec.head.get.bucketColumnNames.toSet === indexConfig1.indexedColumns.toSet
            .map(SchemaUtils.escapeFieldName))

        checkAnswer(baseQuery, filter)
      }
    }
  }

  test("Delete-only: index relation should have additional filter for deleted files.") {
    val testSet = Seq(("index_ParquetDelete", fileFormat), ("index_JsonDelete", fileFormat2))
    testSet foreach {
      case (indexName, dataFormat) =>
        withTempPathAsString { dataPath =>
          val (appendedFiles, deletedFiles) = setupIndexAndChangeData(
            dataFormat,
            dataPath,
            indexConfig1.copy(indexName = indexName),
            appendCnt = 0,
            deleteCnt = 1)

          val df = spark.read.format(dataFormat).load(dataPath)
          def filterQuery: DataFrame =
            df.filter(df("nested.leaf.cnt") <= 20).select(df("query"))

          val baseQuery = filterQuery
          val basePlan = baseQuery.queryExecution.optimizedPlan

          withSQLConf(TestConfig.HybridScanEnabledAppendOnly: _*) {
            val filter = filterQuery
            assert(basePlan.equals(filter.queryExecution.optimizedPlan))
          }

          withSQLConf(TestConfig.HybridScanEnabled: _*) {
            val filter = filterQuery
            val planWithHybridScan = filter.queryExecution.optimizedPlan
            assert(!basePlan.equals(planWithHybridScan))
            checkFilterIndexHybridScanUnion(
              planWithHybridScan,
              indexName,
              appendedFiles,
              deletedFiles,
              Seq(" <= 20"))
            checkAnswer(baseQuery, filter)
          }
        }
    }
  }

  test("Delete-only: join rule, deleted files should be excluded from each index data relation.") {
    withTempPathAsString { testPath =>
      val deletePath1 = testPath + "/delete1"
      val deletePath2 = testPath + "/delete2"
      val leftIndexName = "index_Delete"
      val rightIndexName = "indexType2_Delete2"
      val (leftAppended, leftDeleted) = setupIndexAndChangeData(
        fileFormat,
        deletePath1,
        indexConfig1.copy(indexName = leftIndexName),
        appendCnt = 0,
        deleteCnt = 2)
      val (rightAppended, rightDeleted) = setupIndexAndChangeData(
        fileFormat,
        deletePath2,
        indexConfig2.copy(indexName = rightIndexName),
        appendCnt = 0,
        deleteCnt = 2)

      val df1 = spark.read.format(fileFormat).load(deletePath1)
      val df2 = spark.read.format(fileFormat).load(deletePath2)

      def joinQuery(): DataFrame = {
        val query =
          df1.filter(df1("nested.leaf.cnt") >= 20).select(df1("nested.leaf.cnt"), df1("query"))
        val query2 =
          df2.filter(df2("nested.leaf.cnt") <= 40).select(df2("nested.leaf.cnt"), df2("Date"))
        query.join(query2, "cnt")
      }

      val baseQuery = joinQuery()
      val basePlan = baseQuery.queryExecution.optimizedPlan

      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
        withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
          val join = joinQuery()
          checkAnswer(baseQuery, join)
        }

        withSQLConf(TestConfig.HybridScanEnabled: _*) {
          val join = joinQuery()
          val planWithHybridScan = join.queryExecution.optimizedPlan
          assert(!basePlan.equals(planWithHybridScan))
          checkJoinIndexHybridScan(
            planWithHybridScan,
            leftIndexName,
            leftAppended,
            leftDeleted,
            rightIndexName,
            rightAppended,
            rightDeleted,
            Seq(" >= 20", " <= 40"))
          checkAnswer(join, baseQuery)
        }
      }
    }
  }

  test(
    "Append+Delete: filter rule, appended files should be handled " +
      "with additional plan and merged by Union.") {

    withTempPathAsString { testPath =>
      val (appendedFiles, deletedFiles) = setupIndexAndChangeData(
        fileFormat,
        testPath,
        indexConfig1.copy(indexName = "index_appendAndDelete"),
        appendCnt = 1,
        deleteCnt = 1)

      val df = spark.read.format(fileFormat).load(testPath)

      def filterQuery: DataFrame =
        df.filter(df("nested.leaf.cnt") <= 20).select(df("query"))

      val baseQuery = filterQuery
      val basePlan = baseQuery.queryExecution.optimizedPlan

      withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
        val filter = filterQuery
        assert(basePlan.equals(filter.queryExecution.optimizedPlan))
      }

      withSQLConf(TestConfig.HybridScanEnabled: _*) {
        val filter = filterQuery
        val planWithHybridScan = filter.queryExecution.optimizedPlan
        assert(!basePlan.equals(planWithHybridScan))

        checkFilterIndexHybridScanUnion(
          planWithHybridScan,
          "index_appendAndDelete",
          appendedFiles,
          deletedFiles,
          Seq(" <= 20"))
        checkAnswer(baseQuery, filter)
      }
    }
  }

  test(
    "Append+Delete: join rule, appended data should be shuffled with indexed columns " +
      "and merged by BucketUnion and deleted files are handled with index data.") {
    // One relation has both deleted & appended files and the other one has only deleted files.
    withTempPathAsString { testPath =>
      val leftDataPath = testPath + "/leftPath"
      val rightDataPath = testPath + "/rightPath"
      val leftIndexName = "index_Both"
      val rightIndexName = "indexType2_Delete"
      val (leftAppended, leftDeleted) = setupIndexAndChangeData(
        fileFormat,
        leftDataPath,
        indexConfig1.copy(indexName = leftIndexName),
        appendCnt = 1,
        deleteCnt = 1)
      val (rightAppended, rightDeleted) = setupIndexAndChangeData(
        fileFormat,
        rightDataPath,
        indexConfig2.copy(indexName = rightIndexName),
        appendCnt = 0,
        deleteCnt = 2)

      val df1 = spark.read.format(fileFormat).load(leftDataPath)
      val df2 = spark.read.format(fileFormat).load(rightDataPath)
      def joinQuery(): DataFrame = {
        val query =
          df1.filter(df1("nested.leaf.cnt") >= 20).select(df1("nested.leaf.cnt"), df1("query"))
        val query2 =
          df2.filter(df2("nested.leaf.cnt") <= 40).select(df2("nested.leaf.cnt"), df2("Date"))
        query.join(query2, "cnt")
      }
      val baseQuery = joinQuery()
      val basePlan = baseQuery.queryExecution.optimizedPlan

      withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
        withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
          val join = joinQuery()
          checkAnswer(baseQuery, join)
        }

        withSQLConf(
          TestConfig.HybridScanEnabled :+
            "spark.sql.optimizer.inSetConversionThreshold" -> "1": _*) {
          // Changed inSetConversionThreshold to check InSet optimization.
          val join = joinQuery()
          val planWithHybridScan = join.queryExecution.optimizedPlan
          assert(!basePlan.equals(planWithHybridScan))
          checkJoinIndexHybridScan(
            planWithHybridScan,
            leftIndexName,
            leftAppended,
            leftDeleted,
            rightIndexName,
            rightAppended,
            rightDeleted,
            Seq(" >= 20)", " <= 40)"))
          checkAnswer(baseQuery, join)
        }
      }
    }
  }
}
