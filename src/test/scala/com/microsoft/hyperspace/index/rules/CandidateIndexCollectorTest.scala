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

package com.microsoft.hyperspace.index.rules

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, IsNotNull}
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation, NoopCache}
import org.apache.spark.sql.types.{IntegerType, StringType}

import com.microsoft.hyperspace.TestUtils.latestIndexLogEntry
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{HyperspaceRuleSuite, IndexCollectionManager, IndexConfig, IndexConstants, IndexLogEntryTags}
import com.microsoft.hyperspace.util.FileUtils

class CandidateIndexCollectorTest extends HyperspaceRuleSuite with SQLHelper {
  override val indexLocationDirName = "candidateIndexCollectorTest"

  val t1c1 = AttributeReference("t1c1", IntegerType)()
  val t1c2 = AttributeReference("t1c2", StringType)()
  val t1c3 = AttributeReference("t1c3", IntegerType)()
  val t1c4 = AttributeReference("t1c4", StringType)()
  val t2c1 = AttributeReference("t2c1", IntegerType)()
  val t2c2 = AttributeReference("t2c2", StringType)()
  val t2c3 = AttributeReference("t2c3", IntegerType)()
  val t2c4 = AttributeReference("t2c4", StringType)()

  val t1Schema = schemaFromAttributes(t1c1, t1c2, t1c3, t1c4)
  val t2Schema = schemaFromAttributes(t2c1, t2c2, t2c3, t2c4)

  var t1Relation: HadoopFsRelation = _
  var t2Relation: HadoopFsRelation = _
  var t1ScanNode: LogicalRelation = _
  var t2ScanNode: LogicalRelation = _
  var t1FilterNode: Filter = _
  var t2FilterNode: Filter = _
  var t1ProjectNode: Project = _
  var t2ProjectNode: Project = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val t1Location =
      new InMemoryFileIndex(spark, Seq(new Path("t1")), Map.empty, Some(t1Schema), NoopCache)
    val t2Location =
      new InMemoryFileIndex(spark, Seq(new Path("t2")), Map.empty, Some(t2Schema), NoopCache)

    t1Relation = baseRelation(t1Location, t1Schema)
    t2Relation = baseRelation(t2Location, t2Schema)

    t1ScanNode = LogicalRelation(t1Relation, Seq(t1c1, t1c2, t1c3, t1c4), None, false)
    t2ScanNode = LogicalRelation(t2Relation, Seq(t2c1, t2c2, t2c3, t2c4), None, false)

    t1FilterNode = Filter(IsNotNull(t1c1), t1ScanNode)
    t2FilterNode = Filter(IsNotNull(t2c1), t2ScanNode)

    t1ProjectNode = Project(Seq(t1c1, t1c3), t1FilterNode)
    // Project [t1c1#0, t1c3#2]
    //  +- Filter isnotnull(t1c1#0)
    //   +- Relation[t1c1#0,t1c2#1,t1c3#2,t1c4#3] parquet

    t2ProjectNode = Project(Seq(t2c1, t2c3), t2FilterNode)
    // Project [t2c1#4, t2c3#6]
    //  +- Filter isnotnull(t2c1#4)
    //   +- Relation[t2c1#4,t2c2#5,t2c3#6,t2c4#7] parquet

    createIndexLogEntry("t1i1", Seq(t1c1), Seq(t1c3), t1ProjectNode)
    createIndexLogEntry("t1i2", Seq(t1c1, t1c2), Seq(t1c3), t1ProjectNode)
    createIndexLogEntry("t1i3", Seq(t1c2), Seq(t1c3), t1ProjectNode)
    createIndexLogEntry("t2i1", Seq(t2c1), Seq(t2c3), t2ProjectNode)
    createIndexLogEntry("t2i2", Seq(t2c1, t2c2), Seq(t2c3), t2ProjectNode)
  }

  test("Verify indexes are matched by signature correctly.") {
    val indexManager = IndexCollectionManager(spark)
    val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    val t1Result = CandidateIndexCollector(t1ProjectNode, allIndexes)
    val t2Result = CandidateIndexCollector(t2ProjectNode, allIndexes)

    CandidateIndexCollector(t1ProjectNode, allIndexes)

    assert(t1Result.head._2.length === 3)
    assert(t2Result.head._2.length === 2)

    // Delete an index for t1ProjectNode.
    indexManager.delete("t1i1")
    val allIndexes2 = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

    val t1Result2 = CandidateIndexCollector(t1ProjectNode, allIndexes2)
    assert(t1Result2.head._2.length === 2)
  }

  test("Verify CandidateIndexCollector for hybrid scan.") {
    withTempPath { tempPath =>
      val indexManager = IndexCollectionManager(spark)
      val df = spark.range(1, 5).toDF("id")
      val dataPath = tempPath.getAbsolutePath
      df.write.parquet(dataPath)

      val indexNameWithLineage = "index1"
      val indexNameWithoutLineage = "index2"
      withIndex(indexNameWithLineage, indexNameWithoutLineage) {
        val readDf = spark.read.parquet(dataPath)
        val expectedCommonSourceBytes = FileUtils.getDirectorySize(new Path(dataPath))
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
          indexManager.create(readDf, IndexConfig(indexNameWithLineage, Seq("id")))
        }
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "false") {
          indexManager.create(readDf, IndexConfig(indexNameWithoutLineage, Seq("id")))
        }
        val allIndexes = indexManager.getIndexes(Seq(Constants.States.ACTIVE))

        def verify(
            plan: LogicalPlan,
            hybridScanEnabled: Boolean,
            hybridScanDeleteEnabled: Boolean,
            expectedCandidateIndexes: Seq[String],
            expectedHybridScanTag: Option[Boolean],
            expectedCommonSourceBytes: Option[Long]): Unit = {
          withSQLConf(
            IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> hybridScanEnabled.toString,
            IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "0.99",
            IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD ->
              (if (hybridScanDeleteEnabled) "0.99" else "0")) {
            val indexes = CandidateIndexCollector(plan, allIndexes).headOption.map(_._2)
            if (expectedCandidateIndexes.nonEmpty) {
              assert(indexes.isDefined)
              assert(indexes.get.length === expectedCandidateIndexes.length)
              assert(indexes.get.map(_.name).toSet.equals(expectedCandidateIndexes.toSet))
              indexes.get.foreach { index =>
                assert(
                  index.getTagValue(plan, IndexLogEntryTags.HYBRIDSCAN_REQUIRED)
                    === expectedHybridScanTag)
                assert(
                  index.getTagValue(plan, IndexLogEntryTags.COMMON_SOURCE_SIZE_IN_BYTES)
                    === expectedCommonSourceBytes)
              }
            } else {
              assert(indexes.isEmpty)
            }
          }
        }

        // Verify that a candidate index is returned with the unmodified data files whether
        // hybrid scan is enabled or not.
        {
          val optimizedPlan = spark.read.parquet(dataPath).queryExecution.optimizedPlan
          verify(
            optimizedPlan,
            hybridScanEnabled = false,
            hybridScanDeleteEnabled = false,
            expectedCandidateIndexes = Seq(indexNameWithLineage, indexNameWithoutLineage),
            expectedHybridScanTag = None,
            expectedCommonSourceBytes = None)
          verify(
            optimizedPlan,
            hybridScanEnabled = true,
            hybridScanDeleteEnabled = false,
            expectedCandidateIndexes = Seq(indexNameWithLineage, indexNameWithoutLineage),
            expectedHybridScanTag = Some(false),
            expectedCommonSourceBytes = Some(expectedCommonSourceBytes))
        }

        // Scenario #1: Append new files.
        df.write.mode("append").parquet(dataPath)

        {
          val optimizedPlan = spark.read.parquet(dataPath).queryExecution.optimizedPlan
          verify(
            optimizedPlan,
            hybridScanEnabled = false,
            hybridScanDeleteEnabled = false,
            expectedCandidateIndexes = Seq(),
            expectedHybridScanTag = None,
            expectedCommonSourceBytes = None)
          verify(
            optimizedPlan,
            hybridScanEnabled = true,
            hybridScanDeleteEnabled = false,
            expectedCandidateIndexes = Seq(indexNameWithLineage, indexNameWithoutLineage),
            expectedHybridScanTag = Some(true),
            expectedCommonSourceBytes = Some(expectedCommonSourceBytes))
        }

        // Scenario #2: Delete 1 file.
        val deleteFilePath = new Path(readDf.inputFiles.head)
        val updatedExpectedCommonSourceBytes = expectedCommonSourceBytes - FileUtils
          .getDirectorySize(deleteFilePath)
        FileUtils.delete(deleteFilePath)

        {
          val optimizedPlan = spark.read.parquet(dataPath).queryExecution.optimizedPlan
          verify(
            optimizedPlan,
            hybridScanEnabled = false,
            hybridScanDeleteEnabled = false,
            expectedCandidateIndexes = Seq(),
            expectedHybridScanTag = None,
            expectedCommonSourceBytes = None)
          verify(
            optimizedPlan,
            hybridScanEnabled = true,
            hybridScanDeleteEnabled = false,
            expectedCandidateIndexes = Seq(),
            expectedHybridScanTag = None,
            expectedCommonSourceBytes = None)
          verify(
            optimizedPlan,
            hybridScanEnabled = true,
            hybridScanDeleteEnabled = true,
            expectedCandidateIndexes = Seq(indexNameWithLineage),
            expectedHybridScanTag = Some(true),
            expectedCommonSourceBytes = Some(updatedExpectedCommonSourceBytes))
        }

        // Scenario #3: Replace all files.
        df.write.mode("overwrite").parquet(dataPath)

        {
          val optimizedPlan = spark.read.parquet(dataPath).queryExecution.optimizedPlan
          verify(
            optimizedPlan,
            hybridScanEnabled = false,
            hybridScanDeleteEnabled = false,
            expectedCandidateIndexes = Seq(),
            expectedHybridScanTag = None,
            expectedCommonSourceBytes = None)
          verify(
            optimizedPlan,
            hybridScanEnabled = true,
            hybridScanDeleteEnabled = true,
            expectedCandidateIndexes = Seq(),
            expectedHybridScanTag = None,
            expectedCommonSourceBytes = None)
        }
      }
    }
  }

  test("Verify Hybrid Scan candidate tags work as expected.") {
    withTempPath { tempPath =>
      val indexManager = IndexCollectionManager(spark)
      val df = spark.range(1, 10).toDF("id")
      val dataPath = tempPath.getAbsolutePath
      df.write.parquet(dataPath)

      withIndex("index1") {
        val readDf = spark.read.parquet(dataPath)
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
          indexManager.create(readDf, IndexConfig("index1", Seq("id")))
        }

        val allIndexes = Seq(latestIndexLogEntry(systemPath, "index1"))
        df.limit(5).write.mode("append").parquet(dataPath)
        val optimizedPlan = spark.read.parquet(dataPath).queryExecution.optimizedPlan
        val relation = RuleUtils.getRelation(spark, optimizedPlan).get

        withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "0.99") {
            val indexes = CandidateIndexCollector(optimizedPlan, allIndexes).head._2
            assert(
              indexes.head
                .getTagValue(relation.plan, IndexLogEntryTags.IS_HYBRIDSCAN_CANDIDATE)
                .get)
            assert(
              indexes.head
                .getTagValue(relation.plan, IndexLogEntryTags.HYBRIDSCAN_RELATED_CONFIGS)
                .get == Seq("0.99", "0.2"))
          }

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "0.2") {
            val indexes = CandidateIndexCollector(optimizedPlan, allIndexes).headOption.map(_._2)
            assert(indexes.isEmpty)
            assert(
              !allIndexes.head
                .getTagValue(relation.plan, IndexLogEntryTags.IS_HYBRIDSCAN_CANDIDATE)
                .get)
            assert(
              allIndexes.head
                .getTagValue(relation.plan, IndexLogEntryTags.HYBRIDSCAN_RELATED_CONFIGS)
                .get == Seq("0.2", "0.2"))
          }
        }
      }
    }
  }

  test("Verify reason string tag is set properly.") {
    withTempPath { tempPath =>
      val indexManager = IndexCollectionManager(spark)
      import spark.implicits._
      val df = ('a' to 'z').map(c => (Char.char2int(c), c.toString)).toDF("id", "name")
      val dataPath = tempPath.getAbsolutePath
      df.write.parquet(dataPath)
      df.write.parquet(s"${dataPath}_")
      val df2 = ('a' to 'z').map(c => (Char.char2int(c), c.toString)).toDF("id", "name2")
      df2.write.parquet(s"${dataPath}__")

      val indexList =
        Seq("index_ok", "index_noLineage", "index_differentColumn", "index_noCommonFiles")

      withIndex(indexList: _*) {
        val readDf = spark.read.parquet(dataPath)
        val readDf2 = spark.read.parquet(s"${dataPath}_")
        val readDf3 = spark.read.parquet(s"${dataPath}__")
        indexManager.create(readDf, IndexConfig("index_noLineage", Seq("id", "name")))
        withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
          indexManager.create(
            readDf3,
            IndexConfig("index_differentColumn", Seq("id"), Seq("name2")))
          indexManager.create(readDf, IndexConfig("index_ok", Seq("id", "name")))
          indexManager.create(readDf2, IndexConfig("index_noCommonFiles", Seq("id", "name")))
        }

        val allIndexes = indexList.map(indexName => latestIndexLogEntry(systemPath, indexName))
        allIndexes.foreach(_.setTagValue(IndexLogEntryTags.INDEX_PLAN_ANALYSIS_ENABLED, true))

        val plan1 =
          spark.read.parquet(dataPath).select("id", "name").queryExecution.optimizedPlan
        val indexes = CandidateIndexCollector(plan1, allIndexes).head._2

        val filtered = indexes.map(_.name)
        assert(filtered.length == 2)
        assert(filtered.toSet.equals(Set("index_ok", "index_noLineage")))

        allIndexes.foreach { entry =>
          val reasons = entry.getTagValue(plan1, IndexLogEntryTags.FILTER_REASONS)
          if (filtered.contains(entry.name)) {
            assert(reasons.isEmpty)
          } else {
            assert(reasons.isDefined)
            val msg = reasons.get.map(_.verboseStr)
            entry.name match {
              case "index_differentColumn" =>
                assert(msg.exists(_.contains("Column Schema does not match")))
              case "index_noCommonFiles" =>
                assert(msg.exists(_.contains("Index signature does not match")))
            }
          }

          // Unset for next test though it will use a different plan.
          entry.unsetTagValue(plan1, IndexLogEntryTags.FILTER_REASONS)
        }

        // Hybrid Scan candidate test
        withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true") {
          df.limit(20).write.mode("append").parquet(dataPath)
          val plan2 = spark.read.parquet(dataPath).queryExecution.optimizedPlan

          val indexes = CandidateIndexCollector(plan2, allIndexes)
          assert(indexes.isEmpty)

          allIndexes.foreach { entry =>
            val reasons = entry.getTagValue(plan2, IndexLogEntryTags.FILTER_REASONS)
            assert(reasons.isDefined)
            val msg = reasons.get.map(_.verboseStr)
            entry.name match {
              case "index_differentColumn" =>
                assert(msg.exists(_.contains("Column Schema does not match")))
              case "index_noCommonFiles" =>
                assert(msg.exists(_.contains("No common files.")))
              case _ =>
                assert(msg.exists(_.contains("Appended bytes ratio")))
            }

            // Unset for next test.
            entry.unsetTagValue(plan2, IndexLogEntryTags.FILTER_REASONS)
          }

          withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "0.99") {
            {
              val indexes = CandidateIndexCollector(plan2, allIndexes).head._2
              val filtered = indexes.map(_.name)
              assert(filtered.length == 2)
              assert(filtered.toSet.equals(Set("index_ok", "index_noLineage")))

              allIndexes.foreach { entry =>
                val reasons = entry.getTagValue(plan2, IndexLogEntryTags.FILTER_REASONS)
                if (filtered.contains(entry.name)) {
                  assert(reasons.isEmpty)
                } else {
                  assert(reasons.isDefined)
                  val msg = reasons.get.map(_.verboseStr)
                  entry.name match {
                    case "index_differentColumn" =>
                      assert(msg.exists(_.contains("Column Schema does not match")))
                    case "index_noCommonFiles" =>
                      assert(msg.exists(_.contains("No common files.")))
                  }
                }

                // Unset for next test.
                entry.unsetTagValue(plan2, IndexLogEntryTags.FILTER_REASONS)
              }
            }

            // Delete threshold test
            {
              val deleteFileList = readDf.inputFiles.tail
              deleteFileList.foreach { f =>
                FileUtils.delete(new Path(f))
              }
              val plan3 = spark.read.parquet(dataPath).queryExecution.optimizedPlan

              {
                val indexes = CandidateIndexCollector(plan3, allIndexes)
                assert(indexes.isEmpty)

                allIndexes.foreach { entry =>
                  val reasons = entry.getTagValue(plan3, IndexLogEntryTags.FILTER_REASONS)
                  assert(reasons.isDefined)
                  val msg = reasons.get.map(_.verboseStr)

                  entry.name match {
                    case "index_differentColumn" =>
                      assert(msg.exists(_.contains("Column Schema does not match")))
                    case "index_noCommonFiles" =>
                      assert(msg.exists(_.contains("No common files.")))
                    case "index_noLineage" =>
                      assert(msg.exists(_.contains("Index doesn't support deleted files.")))
                    case "index_ok" =>
                      assert(msg.exists(_.contains("Deleted bytes ratio")))
                  }

                  entry.unsetTagValue(plan3, IndexLogEntryTags.FILTER_REASONS)
                }
              }

              withSQLConf(IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD -> "0.99") {
                val indexes = CandidateIndexCollector(plan3, allIndexes).head._2
                val filtered = indexes.map(_.name)
                assert(filtered.length == 1)
                assert(filtered.toSet.equals(Set("index_ok")))
              }
            }
          }
        }
      }
    }
  }
}
