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
import org.apache.spark.sql.execution.{FileSourceScanExec, FilterExec, ProjectExec, UnionExec}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.execution.exchange.ShuffleExchangeExec
import org.apache.spark.sql.internal.SQLConf

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData, TestConfig}
import com.microsoft.hyperspace.TestUtils.logManager
import com.microsoft.hyperspace.index.execution.BucketUnionExec
import com.microsoft.hyperspace.index.plans.logical.BucketUnion
import com.microsoft.hyperspace.shim.{ExtractFileSourceScanExecFilters, ExtractUnionChildren, RepartitionByExpressionWithOptionalNumPartitions}
import com.microsoft.hyperspace.util.FileUtils

trait HybridScanSuite extends QueryTest with HyperspaceSuite {
  override val indexLocationDirName = "hybridScanTest"

  val sampleData = SampleData.testData
  var hyperspace: Hyperspace = _

  import spark.implicits._
  val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
  val indexConfig1 = IndexConfig("indexType1", Seq("clicks"), Seq("query"))
  val indexConfig2 = IndexConfig("indexType2", Seq("clicks"), Seq("Date"))

  protected val fileFormat = "parquet"
  protected val fileFormat2 = "json"

  def normalizePaths(in: Seq[String]): Seq[String] = {
    in.map(_.replace("file:///", "file:/"))
  }
  def equalNormalizedPaths(a: Seq[String], b: Seq[String]): Boolean = {
    normalizePaths(a).toSet === normalizePaths(b).toSet
  }

  // Create source data and create an index with given 'sourcePath', 'sourceFileFormat' and
  // 'indexConfig'. Then copies the first 'appendCnt' number of input files from 'df' and
  // deletes the last 'deleteCnt' of the input files. Return appended and deleted file lists.
  def setupIndexAndChangeData(
      sourceFileFormat: String,
      sourcePath: String,
      indexConfig: IndexConfigTrait,
      appendCnt: Int,
      deleteCnt: Int): (Seq[String], Seq[String]) = {
    dfFromSample.write.format(sourceFileFormat).save(sourcePath)
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

  before {
    spark.conf.set(IndexConstants.INDEX_LINEAGE_ENABLED, "true")

    // Dynamic pruning creates a dynamic filter, with different ids every time
    // thus making basePlan.equals(join) fail
    spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "false")

    spark.enableHyperspace()
  }

  after {
    FileUtils.delete(systemPath)
    spark.disableHyperspace()
  }

  private def getLatestStableLog(indexName: String): IndexLogEntry = {
    val entry = logManager(systemPath, indexName).getLatestStableLog()
    assert(entry.isDefined)
    assert(entry.get.isInstanceOf[IndexLogEntry])
    entry.get.asInstanceOf[IndexLogEntry]
  }

  // Check if the given plan is transformed correctly to handle deleted files. The plan
  // should include only one LogicalRelation node which is transformed with the given index.
  def checkDeletedFiles(
      plan: LogicalPlan,
      indexName: String,
      expectedDeletedFileNames: Seq[String]): Unit = {

    val fileNameToId = getLatestStableLog(indexName).fileIdTracker.getFileToIdMapping.toSeq.map {
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
        case p @ ExtractFileSourceScanExecFilters(_, dataFilters) =>
          // Check deleted files.
          assert(deletedFiles.forall(dataFilters.toString.contains))
          p
      }
      assert(execNodes.length === 1)
    }
  }

  // Check if the given plan is transformed correctly for Join index application.
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
            bucketSpec.bucketColumnNames.head === "clicks")

        val childNodes = children.collect {
          case r @ RepartitionByExpressionWithOptionalNumPartitions(
                attrs,
                Project(_, Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))),
                Some(numBucket)) =>
            assert(attrs.size === 1)
            assert(attrs.head.asInstanceOf[Attribute].name.contains("clicks"))

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
            bucketSpec.bucketColumnNames.head === "clicks")

        val childNodes = children.collect {
          case r @ RepartitionByExpressionWithOptionalNumPartitions(
                attrs,
                Project(_, Filter(_, LogicalRelation(fsRelation: HadoopFsRelation, _, _, _))),
                Some(numBucket)) =>
            assert(attrs.size === 1)
            assert(attrs.head.asInstanceOf[Attribute].name.contains("clicks"))

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
          assert(
            children.head.isInstanceOf[ProjectExec] || children.head.isInstanceOf[FilterExec])
          assert(children.last.isInstanceOf[ShuffleExchangeExec])
          assert(bucketSpec.numBuckets === 200)
          p
        case p @ ExtractFileSourceScanExecFilters(partitionFilters, dataFilters) =>
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

  // Check if the given plan is transformed correctly for Filter index application and appended
  // files are handled with Union operation if appended files is not empty.
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
        case u @ ExtractUnionChildren(children) =>
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
          case p @ ExtractFileSourceScanExecFilters(partitionFilters, dataFilters) =>
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
        val query2 = df1.filter(df1("clicks") >= 2000).select(df1("clicks"), df1("query"))
        val query = df2.filter(df2("clicks") <= 4000).select(df2("clicks"), df2("Date"))
        query2.join(query, "clicks")
      }
      val baseQuery = joinQuery()
      val basePlan = baseQuery.queryExecution.optimizedPlan

      // RemoveRedundantProjects rule causes HybridScanForIcebergTest to fail
      // in Spark 3.1. Either a bug of Spark 3.1, or Iceberg needs to be
      // updated. Either way, it will take some time to be fixed, so let's
      // temporarily disable the rule here.
      withSQLConf(
        "spark.sql.autoBroadcastJoinThreshold" -> "-1",
        "spark.sql.execution.removeRedundantProjects" -> "false") {
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false",
          IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "false") {
          val join = joinQuery()
          checkAnswer(join, baseQuery)
          assert(basePlan.equals(join.queryExecution.optimizedPlan))
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
            Seq(">= 2000", "<= 4000"))
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
      def filterQuery: DataFrame = df.filter(df("clicks") <= 2000).select(df("query"))

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
          Seq(" <= 2000"))

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
      def filterQuery: DataFrame = df.filter(df("clicks") <= 2000).select(df("query"))

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
          Seq(" <= 2000"))

        // Check bucketSpec is used.
        val bucketSpec = planWithHybridScan collect {
          case LogicalRelation(HadoopFsRelation(_, _, _, bucketSpec, _, _), _, _, _) =>
            bucketSpec
        }
        assert(bucketSpec.length == 2)
        // bucketSpec.head is for the index plan, bucketSpec.last is for the plan
        // for appended files.
        assert(bucketSpec.head.isDefined && bucketSpec.last.isEmpty)
        assert(bucketSpec.head.get.bucketColumnNames.toSet === indexConfig1.indexedColumns.toSet)

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
            df.filter(df("clicks") <= 2000).select(df("query"))

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
              Seq(" <= 2000"))
            checkAnswer(baseQuery, filter)
          }
        }
    }
  }

  test(
    "Delete-only: join rule, deleted files should be excluded from each index data relation.") {
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
            Seq(" >= 2000", " <= 4000"))
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
        df.filter(df("clicks") <= 2000).select(df("query"))

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
          Seq(" <= 2000"))
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
          TestConfig.HybridScanEnabled :+
            "spark.sql.optimizer.inSetConversionThreshold" -> "1": _*) {
          // Changed inSetConversionThreshould to check InSet optimization.
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
            Seq(" >= 2000)", " <= 4000)"))
          checkAnswer(baseQuery, join)
        }
      }
    }
  }
}
