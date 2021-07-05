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
import com.microsoft.hyperspace.index.{IndexLogEntry, IndexLogEntryTags}

trait IndexFilter extends ActiveSparkSession {

  /**
   * Append a given reason string to FILTER_REASONS tag of the index if the condition is false and
   * FILTER_REASONS_ENABLED tag is set to the index.
   *
   * @param condition Flag to append reason string
   * @param plan Query plan to tag
   * @param index Index to tag
   * @param reasonString Informational message in case condition is false.
   */
  protected def setFilterReasonTag(
      condition: Boolean,
      plan: LogicalPlan,
      index: IndexLogEntry,
      reasonString: => String): Unit = {
    if (!condition && index
        .getTagValue(IndexLogEntryTags.FILTER_REASONS_ENABLED)
        .getOrElse(false)) {
      val prevReason =
        index.getTagValue(plan, IndexLogEntryTags.FILTER_REASONS).getOrElse(Nil)
      index.setTagValue(plan, IndexLogEntryTags.FILTER_REASONS, prevReason :+ reasonString)
    }
  }

  /**
   * Append the reason string to FILTER_REASONS tag for the given index
   * if the result of the function is false and FILTER_REASONS tag is set to the index.
   *
   * @param reasonString Informational message in case condition is false.
   * @param plan Query plan to tag
   * @param index Index to tag
   * @param f Function for a condition
   * @return Result of the given function
   */
  protected def withFilterReasonTag(
      plan: LogicalPlan,
      index: IndexLogEntry,
      reasonString: => String)(f: => Boolean): Boolean = {
    val ret = f
    setFilterReasonTag(ret, plan, index, reasonString)
    ret
  }

  /**
   * Append the reason string to FILTER_REASONS tag for the given list of indexes
   * if the result of the function is false and FILTER_REASONS_ENABLED tag is set to the index.
   *
   * @param plan Query plan to tag
   * @param indexes Indexes to tag
   * @param reasonString Informational message in case condition is false.
   * @param f Function for a condition
   * @return Result of the given function
   */
  protected def withFilterReasonTag(
      plan: LogicalPlan,
      indexes: Seq[IndexLogEntry],
      reasonString: => String)(f: => Boolean): Boolean = {
    val ret = f
    indexes.foreach { index =>
      setFilterReasonTag(ret, plan, index, reasonString)
    }
    ret
  }

  /**
   * Append the reason string to FILTER_REASONS tag for the given list of indexes
   * if FILTER_REASONS_ENABLED tag is set to the indexes.
   *
   * @param reasonString Informational message in case condition is false.
   * @param plan Query plan to tag
   * @param indexes Indexes to tag
   * @return Result of the given function
   */
  protected def setFilterReasonTag(
      plan: LogicalPlan,
      indexes: Seq[IndexLogEntry],
      reasonString: => String): Unit = {
    indexes.foreach { index =>
      setFilterReasonTag(false, plan, index, reasonString)
    }
  }
}
