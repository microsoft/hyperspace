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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, QueryTest}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex, LogicalRelation}

import com.microsoft.hyperspace.{Hyperspace, Implicits, SampleData, TestConfig}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.covering.{FilterIndexRule, JoinIndexRule}

class ScoreBasedIndexPlanOptimizerTest extends QueryTest with HyperspaceSuite {
  private val testDir = inTempDir("scoreBasedIndexPlanOptimizerTest")
  private val fileSystem = new Path(testDir).getFileSystem(new Configuration)
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
    hyperspace = new Hyperspace(spark)
    fileSystem.delete(new Path(testDir), true)
  }

  before {
    // Clear index cache so a new test does not see stale indexes from previous ones.
    clearCache()
  }

  override def afterAll(): Unit = {
    fileSystem.delete(new Path(testDir), true)
    super.afterAll()
  }

  after {
    fileSystem.delete(systemPath, true)
    spark.disableHyperspace()
  }

  test(
    "Verify filter index pair with high score should be prior to " +
      "join index pair with lower score.") {
    withTempPathAsString { testPath =>
      SampleData.save(spark, testPath, Seq("c1", "c2", "c3", "c4", "c5"))

      withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
        {
          val leftDf = spark.read.parquet(testPath)
          val leftDfJoinIndexConfig = IndexConfig("leftDfJoinIndex", Seq("c3"), Seq("c4"))
          hyperspace.createIndex(leftDf, leftDfJoinIndexConfig)

          val rightDf = spark.read.parquet(testPath)
          val rightDfJoinIndexConfig = IndexConfig("rightDfJoinIndex", Seq("c3"), Seq("c5"))
          hyperspace.createIndex(rightDf, rightDfJoinIndexConfig)

          // Append data to the same path.
          leftDf.write.mode("append").parquet(testPath)
        }

        val leftDf = spark.read.parquet(testPath)
        val leftDfFilterIndexConfig = IndexConfig("leftDfFilterIndex", Seq("c4"), Seq("c3"))
        hyperspace.createIndex(leftDf, leftDfFilterIndexConfig)

        val rightDf = spark.read.parquet(testPath)
        val rightDfFilterIndexConfig = IndexConfig("rightDfFilterIndex", Seq("c5"), Seq("c3"))
        hyperspace.createIndex(rightDf, rightDfFilterIndexConfig)

        def query(left: DataFrame, right: DataFrame): () => DataFrame = { () =>
          left
            .filter("c4 == 2")
            .select("c4", "c3")
            .join(right.filter("c5 == 3000").select("c5", "c3"), left("c3") === right("c3"))
        }

        withSQLConf(TestConfig.HybridScanEnabled: _*) {
          // For join index pair, the score is 70 * 0.5(left) + 70 * 0.5(right) = 70
          // because of the appended data. For filter index pair, the score is 50 + 50 = 100
          val plan = query(leftDf, rightDf)().queryExecution.optimizedPlan
          val allIndexes = IndexCollectionManager(spark).getIndexes(Seq(Constants.States.ACTIVE))
          val candidateIndexes = CandidateIndexCollector.apply(plan, allIndexes)
          val (_, score) = JoinIndexRule.apply(plan, candidateIndexes)
          assert(score == 70)

          val (leftChildPlan, leftChildScore) =
            FilterIndexRule.apply(plan.children.head, candidateIndexes)
          val (rightChildPlan, rightChildScore) =
            FilterIndexRule.apply(plan.children.last, candidateIndexes)
          assert(leftChildScore == 50)
          assert(!leftChildPlan.equals(plan.children.head))
          assert(rightChildScore == 50)
          assert(!rightChildPlan.equals(plan.children.last))

          verifyIndexUsage(
            query(leftDf, rightDf),
            getIndexFilesPath(leftDfFilterIndexConfig.indexName) ++
              getIndexFilesPath(rightDfFilterIndexConfig.indexName))

          def normalize(str: String): String = {
            // Expression ids are removed before comparison since they can be different.
            str.replaceAll("""#(\d+)|subquery(\d+)""", "#")
          }

          // Verify whyNot result.
          hyperspace.whyNot(query(leftDf, rightDf)()) { o =>
            val expectedOutput = getExpectedResult("whyNot_allIndex.txt")
              .replace(System.lineSeparator(), "\n")
            val actual = normalize(o.replace(System.lineSeparator(), "\n"))
            assert(actual.equals(expectedOutput), actual)
          }

          hyperspace.whyNot(query(leftDf, rightDf)(), "leftDfJoinIndex", extended = true) { o =>
            val expectedOutput = getExpectedResult("whyNot_indexName.txt")
              .replace(System.lineSeparator(), "\n")
            val actual = normalize(o.replace(System.lineSeparator(), "\n"))
            assert(actual.equals(expectedOutput), actual)
          }
        }
      }
    }
  }

  /**
   * Verify that the query plan has the expected root paths.
   *
   * @param optimizedPlan Query plan
   * @param expectedPaths Expected root paths
   */
  private def verifyQueryPlanHasExpectedRootPaths(
      optimizedPlan: LogicalPlan,
      expectedPaths: Seq[Path]): Unit = {
    assert(getAllRootPaths(optimizedPlan).sortBy(_.getName) === expectedPaths.sortBy(_.getName))
  }

  /**
   * Get all rootPaths from a query plan.
   *
   * @param optimizedPlan Query plan
   * @return A sequence of [[Path]]
   */
  private def getAllRootPaths(optimizedPlan: LogicalPlan): Seq[Path] = {
    optimizedPlan.collect {
      case LogicalRelation(
            HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _),
            _,
            _,
            _) =>
        location.rootPaths
    }.flatten
  }

  private def getIndexFilesPath(indexName: String, versions: Seq[Int] = Seq(0)): Seq[Path] = {
    versions.flatMap { v =>
      Content
        .fromDirectory(
          new Path(systemPath, s"$indexName/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=$v"),
          new FileIdTracker,
          new Configuration)
        .files
    }
  }

  private def verifyIndexUsage(f: () => DataFrame, expectedRootPaths: Seq[Path]): Unit = {
    spark.disableHyperspace()
    val dfWithHyperspaceDisabled = f()
    val schemaWithHyperspaceDisabled = dfWithHyperspaceDisabled.schema

    spark.enableHyperspace()
    val dfWithHyperspaceEnabled = f()

    verifyQueryPlanHasExpectedRootPaths(
      dfWithHyperspaceEnabled.queryExecution.optimizedPlan,
      expectedRootPaths)

    assert(schemaWithHyperspaceDisabled.equals(dfWithHyperspaceEnabled.schema))
  }
}
