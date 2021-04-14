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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

import com.microsoft.hyperspace.ActiveSparkSession
import com.microsoft.hyperspace.index.IndexLogEntry

trait HyperspaceCheck {
  def reason: String
  /*
  var whyNotEnabled: Boolean = false
  var whyNotTargetIndex: Option[String] = None
  var whyNotReasons: mutable.Seq[String] = mutable.Seq[String]()

  def withWhyNotCheck(indexes: Seq[IndexLogEntry], msg: String)(
    f: => Seq[IndexLogEntry]): Seq[IndexLogEntry] = {
    if (whyNotEnabled) {
      val names = indexes.map(_.name).toSet
      val targetFound = if (whyNotTargetIndex.isDefined) {
        names.contains(whyNotTargetIndex.get)
      } else {
        names.nonEmpty
      }

      val res = f
      val namesAfter = res.map(_.name).toSet

      if (targetFound) {
        whyNotTargetIndex
          .map { idx =>
            if (!namesAfter.contains(idx)) {
              whyNotReasons += s"$msg, index: ${idx}"
            }
            idx
          }
          .getOrElse {
            if (namesAfter.size == 0) {
              whyNotReasons += msg
            }
          }
      }
      res
    } else {
      f
    }
  }

  def withWhyNotCheck(indexes: Map[LogicalPlan, Seq[IndexLogEntry]], msg: String)(
    f: => Map[LogicalPlan, Seq[IndexLogEntry]]): Map[LogicalPlan, Seq[IndexLogEntry]] = {
    if (whyNotEnabled) {
      val names = indexes.values.flatten.map(_.name).toSet
      val targetFound = if (whyNotTargetIndex.isDefined) {
        names.contains(whyNotTargetIndex.get)
      } else {
        names.nonEmpty
      }

      val res = f
      val namesAfter = res.values.flatten.map(_.name).toSet

      if (targetFound) {
        whyNotTargetIndex
          .map { idx =>
            if (!namesAfter.contains(idx)) {
              whyNotReasons += s"$msg, index: ${idx}"
            }
            idx
          }
          .getOrElse {
            if (namesAfter.size == 0) {
              whyNotReasons += msg
            }
          }
      }
      res
    } else {
      f
    }
  }
   */
}

trait HyperspaceIndexCheck extends HyperspaceCheck with ActiveSparkSession {
  def apply(plan: LogicalPlan, indexes: Seq[IndexLogEntry])
    : Seq[IndexLogEntry]
}

trait HyperspacePlanCheck extends HyperspaceCheck with ActiveSparkSession {
  def apply(
      plan: LogicalPlan,
      indexes: Map[LogicalPlan, Seq[IndexLogEntry]]): Map[LogicalPlan, Seq[IndexLogEntry]]
}
