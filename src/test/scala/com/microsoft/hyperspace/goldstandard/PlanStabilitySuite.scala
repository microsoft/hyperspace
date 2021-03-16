// scalastyle:off

/**
 * This trait was built upon: https://github.com/apache/spark/blob/v3.1.1/sql/core/src/test/scala/org/apache/spark/sql/PlanStabilitySuite.scala.
 *
 * The below license was copied from: https://github.com/FasterXML/jackson-module-scala/blob/2.10/src/main/resources/META-INF/LICENSE
 */

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.microsoft.hyperspace.goldstandard

import java.io.File
import java.nio.charset.StandardCharsets

import scala.collection.mutable

import org.apache.commons.io.FileUtils
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.AttributeSet
import org.apache.spark.sql.catalyst.plans.SQLHelper
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.command.ExplainCommand
import org.apache.spark.sql.execution.exchange.{Exchange, ReusedExchangeExec}

// scalastyle:off filelinelengthchecker
/**
 * Check that TPC-DS SparkPlans don't change.
 * If there are plan differences, the error message looks like this:
 *   Plans did not match:
 *   last approved simplified plan: /path/to/tpcds-plan-stability/approved-plans-xxx/q1/simplified.txt
 *   last approved explain plan: /path/to/tpcds-plan-stability/approved-plans-xxx/q1/explain.txt
 *   [last approved simplified plan]
 *
 *   actual simplified plan: /path/to/tmp/q1.actual.simplified.txt
 *   actual explain plan: /path/to/tmp/q1.actual.explain.txt
 *   [actual simplified plan]
 *
 * The explain files are saved to help debug later, they are not checked. Only the simplified
 * plans are checked (by string comparison).
 *
 *
 * To run the entire test suite:
 * {{{
 *   sbt "test:testOnly *PlanStabilitySuite"
 * }}}
 *
 * To run a single test file upon change:
 * {{{
 *  sbt "test:testOnly *PlanStabilitySuite -- -z (tpcds-v1.4/q49)"
 * }}}
 *
 * To re-generate golden files for entire suite, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 sbt "test:testOnly *PlanStabilitySuite"
 * }}}
 *
 * To re-generate golden file for a single test, run:
 * {{{
 *   SPARK_GENERATE_GOLDEN_FILES=1 sbt "test:testOnly *PlanStabilitySuite -- -z (tpcds-v1.4/q49)"
 * }}}
 */
// scalastyle:on filelinelengthchecker

trait PlanStabilitySuite extends TPCDSBase with SQLHelper with Logging {
  private val regenerateGoldenFiles: Boolean = System.getenv("SPARK_GENERATE_GOLDEN_FILES") == "1"

  protected val baseResourcePath = {
    // use the same way as `SQLQueryTestSuite` to get the resource path
    java.nio.file.Paths.get("src", "test", "resources", "tpcds").toFile
  }

  private val referenceRegex = "#\\d+".r
  private val normalizeRegex = "#\\d+L?".r

  private val clsName = this.getClass.getCanonicalName

  def goldenFilePath: String

  private def getDirForTest(name: String): File = {
    new File(goldenFilePath, name)
  }

  private def isApproved(dir: File, actualSimplifiedPlan: String): Boolean = {
    val file = new File(dir, "simplified.txt")
    val expected = FileUtils.readFileToString(file, StandardCharsets.UTF_8)
    expected == actualSimplifiedPlan
  }

  /**
   * Serialize and save this SparkPlan.
   * The resulting file is used by [[checkWithApproved]] to check stability.
   *
   * @param plan    the SparkPlan
   * @param name    the name of the query
   * @param explain the full explain output; this is saved to help debug later as the simplified
   *                plan is not too useful for debugging
   */
  private def generateGoldenFile(plan: SparkPlan, name: String, explain: String): Unit = {
    val dir = getDirForTest(name)
    val simplified = getSimplifiedPlan(plan)
    val foundMatch = dir.exists() && isApproved(dir, simplified)

    if (!foundMatch) {
      FileUtils.deleteDirectory(dir)
      assert(dir.mkdirs())

      val file = new File(dir, "simplified.txt")
      FileUtils.writeStringToFile(file, simplified, StandardCharsets.UTF_8)
      val fileOriginalPlan = new File(dir, "explain.txt")
      FileUtils.writeStringToFile(fileOriginalPlan, explain, StandardCharsets.UTF_8)
      logDebug(s"APPROVED: $file $fileOriginalPlan")
    }
  }

  private def checkWithApproved(plan: SparkPlan, name: String, explain: String): Unit = {
    val dir = getDirForTest(name)
    val tempDir = FileUtils.getTempDirectory
    val actualSimplified = getSimplifiedPlan(plan)
    val foundMatch = isApproved(dir, actualSimplified)

    if (!foundMatch) {
      // show diff with last approved
      val approvedSimplifiedFile = new File(dir, "simplified.txt")
      val approvedExplainFile = new File(dir, "explain.txt")

      val actualSimplifiedFile = new File(tempDir, s"$name.actual.simplified.txt")
      val actualExplainFile = new File(tempDir, s"$name.actual.explain.txt")

      val approvedSimplified =
        FileUtils.readFileToString(approvedSimplifiedFile, StandardCharsets.UTF_8)
      // write out for debugging
      FileUtils.writeStringToFile(actualSimplifiedFile, actualSimplified, StandardCharsets.UTF_8)
      FileUtils.writeStringToFile(actualExplainFile, explain, StandardCharsets.UTF_8)

      fail(s"""
          |Plans did not match:
          |last approved simplified plan: ${approvedSimplifiedFile.getAbsolutePath}
          |last approved explain plan: ${approvedExplainFile.getAbsolutePath}
          |
          |$approvedSimplified
          |
          |actual simplified plan: ${actualSimplifiedFile.getAbsolutePath}
          |actual explain plan: ${actualExplainFile.getAbsolutePath}
          |
          |$actualSimplified
        """.stripMargin)
    }
  }

  /**
   * Get the simplified plan for a specific SparkPlan. In the simplified plan, the node only has
   * its name and all the sorted reference and produced attributes names(without ExprId) and its
   * simplified children as well. And we'll only identify the performance sensitive nodes, e.g.,
   * Exchange, Subquery, in the simplified plan. Given such a identical but simplified plan, we'd
   * expect to avoid frequent plan changing and catch the possible meaningful regression.
   */
  private def getSimplifiedPlan(plan: SparkPlan): String = {
    val exchangeIdMap = new mutable.HashMap[SparkPlan, Int]()
    val subqueriesMap = new mutable.HashMap[SparkPlan, Int]()

    def getId(plan: SparkPlan): Int = plan match {
      case exchange: Exchange => exchangeIdMap.getOrElseUpdate(exchange, exchangeIdMap.size + 1)
      case ReusedExchangeExec(_, exchange) =>
        exchangeIdMap.getOrElseUpdate(exchange, exchangeIdMap.size + 1)
      case subquery: SubqueryExec =>
        subqueriesMap.getOrElseUpdate(subquery, subqueriesMap.size + 1)
      case _ => -1
    }

    /**
     * Some expression names have ExprId in them due to using things such as
     * "sum(sr_return_amt#14)", so we remove all of these using regex
     */
    def cleanUpReferences(references: AttributeSet): String = {
      referenceRegex.replaceAllIn(references.toSeq.map(_.name).sorted.mkString(","), "")
    }

    /**
     * Generate a simplified plan as a string
     * Example output:
     * TakeOrderedAndProject [c_customer_id]
     *   WholeStageCodegen
     *     Project [c_customer_id]
     */
    def simplifyNode(node: SparkPlan, depth: Int): String = {
      val padding = "  " * depth
      var thisNode = node.nodeName
      if (node.references.nonEmpty) {
        thisNode += s" [${cleanUpReferences(node.references)}]"
      }
      if (node.producedAttributes.nonEmpty) {
        thisNode += s" [${cleanUpReferences(node.producedAttributes)}]"
      }
      val id = getId(node)
      if (id > 0) {
        thisNode += s" #$id"
      }
      val childrenSimplified = node.children.map(simplifyNode(_, depth + 1))
      val subqueriesSimplified = node.subqueries.map(simplifyNode(_, depth + 1))
      s"$padding$thisNode\n${subqueriesSimplified.mkString("")}${childrenSimplified.mkString("")}"
    }

    simplifyNode(plan, 0)
  }

  private def normalizeIds(plan: String): String = {
    val map = new mutable.HashMap[String, String]()
    normalizeRegex
      .findAllMatchIn(plan)
      .map(_.toString)
      .foreach(map.getOrElseUpdate(_, (map.size + 1).toString))
    normalizeRegex.replaceAllIn(plan, regexMatch => s"#${map(regexMatch.toString)}")
  }

  private def normalizeLocation(plan: String): String = {
    plan.replaceAll(
      s"Location.*spark-warehouse/",
      "Location [not included in comparison]/{warehouse_dir}/")
  }

  /**
   * Test a TPC-DS query. Depending on the settings this test will either check if the plan matches
   * a golden file or it will create a new golden file.
   */
  protected def testQuery(tpcdsGroup: String, query: String, suffix: String = ""): Unit = {
    val queryString = resourceToString(
      s"$tpcdsGroup/$query.sql",
      classLoader = Thread.currentThread().getContextClassLoader)
    val qe = spark.sql(queryString).queryExecution
    val plan = qe.executedPlan
    val explain = normalizeLocation(normalizeIds(explainString(qe)))

    if (regenerateGoldenFiles) {
      generateGoldenFile(plan, query + suffix, explain)
    } else {
      checkWithApproved(plan, query + suffix, explain)
    }
  }

  def explainString(queryExecution: QueryExecution): String = {
    val explain = ExplainCommand(queryExecution.logical, extended = false)
    spark.sessionState
      .executePlan(explain)
      .executedPlan
      .executeCollect()
      .map(_.getString(0))
      .mkString("\n")
  }
}

/**
 * Spark Only Suite.
 */
class TPCDSV1_4_SparkPlanStabilitySuite extends PlanStabilitySuite {
  override val goldenFilePath: String =
    new File(baseResourcePath, "spark-2.4/approved-plans-v1_4").getAbsolutePath

  // Enable cross join because some queries fail during query optimization phase.
  withSQLConf("spark.sql.crossJoin.enabled" -> "true") {
    tpcdsQueries.foreach { q =>
      test(s"check simplified (tpcds-v1.4/$q)") {
        testQuery("tpcds/queries", q)
      }
    }
  }
}
