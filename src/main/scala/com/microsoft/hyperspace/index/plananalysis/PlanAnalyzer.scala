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

package com.microsoft.hyperspace.index.plananalysis

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.hyperspace.utils.DataFrameUtils
import org.apache.spark.sql.hyperspace.utils.logicalPlanToDataFrame

import com.microsoft.hyperspace.{HyperspaceException, Implicits}
import com.microsoft.hyperspace.index.IndexConstants

/**
 * Provides helper methods for explain API.
 */
object PlanAnalyzer {

  /**
   * Constructs string that explains how indexes will be applied to the given dataframe.
   *
   * @param originalDf DataFrame query.
   * @param spark SparkSession.
   * @param indexes DataFrame with list of all indexes available.
   * @param verbose Flag to enable verbose mode.
   * @return Explain string.
   */
  def explainString(
      originalDf: DataFrame,
      spark: SparkSession,
      indexes: DataFrame,
      verbose: Boolean): String = {
    val displayMode = getDisplayMode(spark)
    // Create a new df from the original df's logical plan. This ensures a deterministic optimized
    // plan creation when running Hyperspace rules.
    val df = logicalPlanToDataFrame(spark, originalDf.queryExecution.logical)
    val withoutHyperspaceContext = initializeContext(spark, df, false, displayMode)
    val withHyperspaceContext = initializeContext(spark, df, true, displayMode)

    // TODO: Use recursive approach for tree comparison instead iterative.
    // The loop compares nodes of both plans from top down manner using queues.
    while (withHyperspaceContext.nonEmpty && withoutHyperspaceContext.nonEmpty) {
      // If nodes are not equal, then there are two possible cases:
      //
      // (1) FileSourceScanExec node pointing to original data table is replaced
      // by other FileSourceScanExec with index. For example below, FileSource is replaced with
      // FileSourceWithIndex and only those two nodes need to be highlighted.
      //
      //          Node1                                           Node1
      //       //     \\             Applying indexes             // \\
      //    Node2     FileSource    ===================>       Node2  FileSourceWithIndex
      //
      // (2) A subtree in the original plan is replaced with other subtree.
      // For example, in below case subtree starting from Node3 on left is replaced with subtree
      // with NodeA which is of different height and also has FileSource node with index at the
      // bottom instead of original node pointing to source table.
      //
      //          Node1                                           Node1
      //       //     \\             Applying indexes             // \\
      //    Node2        Node3     ===================>        Node2  NodeA
      //     //         //  \\                                 //      \\
      //    //         //     \\                              //        \\
      // FileSource   Node4  Node5                      FileSource   FileSourceWithIndex
      //                       \\
      //                     FileSource
      //
      // In both cases we highlight all nodes in the different subtrees beginning from the
      // first different nodes found based on below equality check.
      val nodeFromWithIndexPlan = withHyperspaceContext.curPlan
      val nodeFromWithoutIndexPlan = withoutHyperspaceContext.curPlan
      if (!areEqual(nodeFromWithIndexPlan, nodeFromWithoutIndexPlan)) {
        withHyperspaceContext.moveNextSubtree { (stream, plan) =>
          stream.highlight(plan).writeLine()
        }
        withoutHyperspaceContext.moveNextSubtree { (stream, plan) =>
          stream.highlight(plan).writeLine()
        }
      } else {
        // Here if compared nodes are equal, we print out node plans without highlight option.
        withHyperspaceContext.moveNext { (stream, plan) =>
          stream.writeLine(plan)
        }
        withoutHyperspaceContext.moveNext { (stream, plan) =>
          stream.writeLine(plan)
        }
      }
    }

    val outputStream = BufferStream(displayMode)

    buildHeader(outputStream, "Plan with indexes:")
    outputStream.writeLine(withHyperspaceContext.toString)

    buildHeader(outputStream, "Plan without indexes:")
    outputStream.writeLine(withoutHyperspaceContext.toString)

    buildHeader(outputStream, "Indexes used:")
    writeUsedIndexes(withHyperspaceContext.originalPlan, spark, indexes, outputStream)
    outputStream.writeLine()

    if (verbose) {
      buildHeader(outputStream, "Physical operator stats:")
      writePhysicalOperatorStats(
        withHyperspaceContext,
        withoutHyperspaceContext,
        outputStream,
        spark)
      outputStream.writeLine()
    }

    outputStream.withTag()
  }

  /**
   * Gets paths of all file source nodes in given plan.
   *
   * @param sparkPlan spark plan.
   * @return paths of all file source nodes in given plan.
   */
  private def getPaths(sparkPlan: SparkPlan): Seq[String] = {
    val usedPaths = new ListBuffer[String]
    sparkPlan.foreach {
      case FileSourceScanExec(
          HadoopFsRelation(location: InMemoryFileIndex, _, _, _, _, _),
          _,
          _,
          _,
          _,
          _,
          _) =>
        usedPaths += location.rootPaths.head.getParent.toString
      case other =>
        other.subqueries.foreach { subQuery =>
          getPaths(subQuery).flatMap(path => usedPaths += path)
        }
    }
    usedPaths
  }

  /**
   * Initializes context for analyzing plan for explain API.
   *
   * @param spark sparkSession.
   * @param df dataFrame.
   * @param hyperspaceState Hyperspace state.
   * @param displayMode display mode.
   * @return context.
   */
  private def initializeContext(
      spark: SparkSession,
      df: DataFrame,
      hyperspaceState: Boolean,
      displayMode: DisplayMode): PlanContext = {
    val (originalPlan, planQueue) = withHyperspaceState(spark, hyperspaceState) {
      val nodes = new mutable.Queue[SparkPlan]
      val originalPlan = spark.sessionState
        .executePlan(df.queryExecution.optimizedPlan)
        .executedPlan
      constructSparkPlanQueue(originalPlan, nodes)
      (originalPlan, nodes)
    }

    new PlanContext(originalPlan, planQueue, displayMode)
  }

  /**
   * Compares given spark plan nodes. Note that spark plan nodes are trees and if we use equals
   * function it compares all the children nodes as well. In this case, we need to compare
   * the class type of the given nodes and not all the children.
   *
   * @param sparkPlan1 spark plan node 1.
   * @param sparkPlan2 spark plan node 2.
   * @return true if given spark plans equivalent or false otherwise.
   */
  private def areEqual(sparkPlan1: SparkPlan, sparkPlan2: SparkPlan): Boolean =
    (sparkPlan1, sparkPlan2) match {
      case (s1: FileSourceScanExec, s2: FileSourceScanExec) =>
        // If given nodes are of type FileSourceScanExec, then equality comparison depends
        // on the root path comparison of the nodes, because root path may be either pointing
        // to base table or indexed table.
        s1.relation.location.rootPaths.head
          .equals(s2.relation.location.rootPaths.head)
      case (s1: SparkPlan, s2: SparkPlan) =>
        // For all other node types we just compares types of the nodes for equality comparison.
        s1.getClass.equals(s2.getClass)
    }

  /**
   * Write used indexes (if any) to bufferStream.
   *
   * @param plan sparkPlan with indexes.
   * @param spark sparkSession.
   * @param indexes all available indexes for the use.
   */
  private def writeUsedIndexes(
      plan: SparkPlan,
      spark: SparkSession,
      indexes: DataFrame,
      bufferStream: BufferStream): Unit = {
    val usedIndexes = indexes.filter(indexes("indexLocation").isin(getPaths(plan): _*))
    usedIndexes.collect().foreach { row =>
      bufferStream
        .write(row.getAs("name").toString)
        .write(":")
        .writeLine(row.getAs("indexLocation").toString)
    }
  }

  /**
   * Write stats on physical operators to bufferStream.
   *
   * @param withHyperspaceContext `PlanContext` where Hyperspace is enabled.
   * @param withoutHyperspaceContext `PlanContext` where Hyperspace is disabled.
   * @param bufferStream buffer stream to write to.
   * @param spark spark session.
   */
  private def writePhysicalOperatorStats(
      withHyperspaceContext: PlanContext,
      withoutHyperspaceContext: PlanContext,
      bufferStream: BufferStream,
      spark: SparkSession): Unit = {
    import spark.implicits._
    val stats = new PhysicalOperatorAnalyzer()
      .analyze(withoutHyperspaceContext.originalPlan, withHyperspaceContext.originalPlan)
      .comparisonStats

    val operatorColName = "Physical Operator"
    val hyperspaceDisabledColName = "Hyperspace Disabled"
    val hyperspaceEnabledColName = "Hyperspace Enabled"
    val differenceColName = "Difference"

    val df = stats
      .map { stat =>
        if (stat.numOccurrencesInPlan1 == stat.numOccurrencesInPlan2) {
          stat
        } else {
          // If the number of occurrences are different, prefix "*" to the operator name
          // for a visual cue and for the sort ordering.
          PhysicalOperatorComparison(
            s"*${stat.name}",
            stat.numOccurrencesInPlan1,
            stat.numOccurrencesInPlan2)
        }
      }
      .toDF(operatorColName, hyperspaceDisabledColName, hyperspaceEnabledColName)
      .withColumn(
        differenceColName,
        col(hyperspaceEnabledColName) - col(hyperspaceDisabledColName))
      .sort(operatorColName)

    // +1 for "*" prefixed for the operator names.
    val maxNameLength = stats.maxBy(_.name.length).name.length + 1
    // '\n' is used in `showString` as a delimiter for newlines.
    df.showString(stats.size, maxNameLength).split('\n').foreach(bufferStream.writeLine)
  }

  /**
   * Builds and appends header string to the buffer.
   *
   * @param bufferStream BufferStream.
   * @param s header string.
   */
  private def buildHeader(bufferStream: BufferStream, s: String): Unit = {
    bufferStream
      .writeLine("=============================================================")
      .writeLine(s)
      .writeLine("=============================================================")
  }

  /**
   * Constructs spark plan queue with all nodes using Pre-order traversal
   * on given spark plan. queue helps to compare spark plans node by node.
   *
   * @param sparkPlan spark plan.
   * @param resultQueue result queue to be updated using spark plan nodes.
   */
  private def constructSparkPlanQueue(
      sparkPlan: SparkPlan,
      resultQueue: mutable.Queue[SparkPlan]): Unit = {
    sparkPlan.foreach { node =>
      // The below nodes are skipped because they override treeString in such a way that
      // these nodes are not available in treeString output of the root node and this affects our
      // comparison in a way that we are not able to highlight correct parts of explain string.
      node match {
        case _: WholeStageCodegenExec =>
        case _: InputAdapter =>
        case _ => resultQueue += node
      }

      // Goes through all possible nested subqueries for every node,
      // as they are skipped by foreach loop on the parent node.
      // Example: In the following query, nodes from subquery in filter condition won't
      // populate without below foreach on subqueries:
      // `select ColX from T1 where ColX == (select ColY from T2 where ColZ==A)`
      node.subqueries.foreach { subquery =>
        constructSparkPlanQueue(subquery, resultQueue)
      }
    }
  }

  private def getDisplayMode(sparkSession: SparkSession): DisplayMode = {
    val displayMode =
      sparkSession.conf
        .get(IndexConstants.DISPLAY_MODE, IndexConstants.DisplayMode.PLAIN_TEXT)
    val highlightTags = Map(
      IndexConstants.HIGHLIGHT_BEGIN_TAG -> sparkSession.conf
        .get(IndexConstants.HIGHLIGHT_BEGIN_TAG, ""),
      IndexConstants.HIGHLIGHT_END_TAG -> sparkSession.conf
        .get(IndexConstants.HIGHLIGHT_END_TAG, ""))
    displayMode match {
      case IndexConstants.DisplayMode.PLAIN_TEXT => new PlainTextMode(highlightTags)
      case IndexConstants.DisplayMode.HTML => new HTMLMode(highlightTags)
      case IndexConstants.DisplayMode.CONSOLE => new ConsoleMode(highlightTags)
      case _ =>
        throw HyperspaceException(s"Display mode: $displayMode not supported.")
    }
  }

  /**
   * Executes given body function in expected Hyperspace state for given spark session and restores
   * back to original state.
   *
   * @param spark spark session.
   * @param desiredState desired Hyperspace state.
   * @param f function to execute in the given Hyperspace state.
   */
  private def withHyperspaceState[T](spark: SparkSession, desiredState: Boolean)(f: => T): T = {
    // Figure outs initial Hyperspace state and enable/disable Hyperspace if needed.
    val isHyperspaceEnabled = spark.isHyperspaceEnabled()
    if (desiredState) {
      spark.enableHyperspace()
    } else {
      spark.disableHyperspace()
    }

    val result = f

    // Restores initial Hyperspace state on given spark session.
    if (isHyperspaceEnabled) {
      spark.enableHyperspace()
    } else {
      spark.disableHyperspace()
    }

    result
  }

  /**
   * Context for keeping track of given Spark Plan queue.
   *
   * @param nodes SparkPlan queue.
   * @param displayMode display mode for explain.
   */
  private class PlanContext(
      val originalPlan: SparkPlan,
      nodes: mutable.Queue[SparkPlan],
      displayMode: DisplayMode) {
    // Note: Spark plan node's treeString API uses "\n" character as a line separator
    // for string output which we have to use to breakdown treeString's output.
    private val treeStringNewLine = "\n"

    // Each plan in the queue maps to one string in planStrings in the same order.
    private val planStrings: Seq[String] = nodes.head.treeString.split(treeStringNewLine)

    // Keeps track of position of string representation of given spark plan node.
    private var curPos = 0

    // Buffer stream to store spark plan string output.
    private val bufferStream = BufferStream(displayMode)

    private def curPlanString: String = planStrings(curPos)

    override def toString: String = bufferStream.toString

    def curPlan: SparkPlan = nodes.head

    def nonEmpty: Boolean = nodes.nonEmpty

    // Applies the given `f` to each node in the current tree and advance.
    def moveNextSubtree(f: (BufferStream, String) => Unit): Unit = {
      // Here since we don't know the height of subtree of the node we are processing,
      // we first find out height by treeString height.
      val nodeHeight = nodes.head.treeString.split(treeStringNewLine).length
      for (_ <- 0 until nodeHeight) {
        moveNext(f)
      }
    }

    // Applies the given `f` to the current node and advance.
    def moveNext(f: (BufferStream, String) => Unit): Unit = {
      f(bufferStream, curPlanString)
      curPos += 1
      nodes.dequeue
    }
  }
}
