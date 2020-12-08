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

import scala.collection.mutable.ListBuffer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, InMemoryFileIndex}

import com.microsoft.hyperspace.{HyperspaceException, Implicits}
import com.microsoft.hyperspace.index.IndexConstants

object PlanAnalyzerUtils {

  /**
   * Gets paths of all file source nodes in given plan.
   *
   * @param sparkPlan spark plan.
   * @return paths of all file source nodes in given plan.
   */
  private[hyperspace] def getPaths(sparkPlan: SparkPlan): Seq[String] = {
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

  private[hyperspace] def getDisplayMode(sparkSession: SparkSession): DisplayMode = {
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
  private[hyperspace] def withHyperspaceState[T](spark: SparkSession, desiredState: Boolean)(
      f: => T): T = {
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

}
