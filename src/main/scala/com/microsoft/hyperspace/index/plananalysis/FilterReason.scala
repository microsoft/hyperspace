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

import com.microsoft.hyperspace.index.plananalysis.FilterReasonCode._

class FilterReason(
    val code: String,
    argStrings: => Seq[(String, String)],
    verboseString: => String) {

  def argStr: String = {
    // key1=[value1], key2=[value2]
    argStrings.map(kv => s"${kv._1}=[${kv._2}]").mkString(",")
  }

  def verboseStr: String = {
    verboseString
  }
}

object FilterReasons {
  def apply(code: FilterReasonCode, argStrings: => Seq[(String, String)]): FilterReason = {
    code match {
      case COL_SCHEMA_MISMATCH =>
        new FilterReason(
          code.toString,
          argStrings,
          "Column Schema does not match. Source data columns: [" + argStrings(0)._2 +
            "], Index columns: [" + argStrings(1)._2)
      case SOURCE_DATA_CHANGE =>
        new FilterReason(code.toString, argStrings, "Index signature does not match.")
      case NO_DELETE_SUPPORT =>
        new FilterReason(code.toString, argStrings, "Index doesn't support deleted files.")
      case NO_COMMON_FILES =>
        new FilterReason(code.toString, argStrings, "No common files.")
      case TOO_MUCH_APPENDED =>
        new FilterReason(
          code.toString,
          argStrings,
          s"Appended bytes ratio (${argStrings(0)._2}) is larger than " +
            s"threshold config ${argStrings(1)._2}). ")
      case TOO_MUCH_DELETED =>
        new FilterReason(
          code.toString,
          argStrings,
          s"Deleted bytes ratio (${argStrings(0)._2}) is larger than " +
            s"threshold config ${argStrings(1)._2}). ")
      case NO_FIRST_INDEXED_COL_COND =>
        new FilterReason(
          code.toString,
          argStrings,
          "The first indexed column should be used in filter conditions. " +
            s"The first indexed column: ${argStrings(0)._2}, " +
            s"Columns in filter condition: [${argStrings(1)._2}]")
      case MISSING_REQUIRED_COL =>
        new FilterReason(
          code.toString,
          argStrings,
          s"Index does not contain required column. Required columns: [${argStrings(0)._2}], " +
            s"Index columns: [${argStrings(1)._2}]")
      case NOT_ELIGIBLE_JOIN =>
        new FilterReason(
          code.toString,
          argStrings,
          s"Join condition is not eligible. Reason: ${argStrings(0)._2}")
      case NO_AVAIL_JOIN_INDEX_PAIR =>
        new FilterReason(
          code.toString,
          argStrings,
          s"No available indexes for ${argStrings(0)._2} subplan. " +
            "Both left and right index are required for Join query")
      case NOT_ALL_JOIN_COL_INDEXED =>
        new FilterReason(
          code.toString,
          argStrings,
          s"All join condition column should be the indexed columns. " +
            s"Join columns: [${argStrings(0)._2}], Indexed columns: [${argStrings(1)._2}]")
      case MISSING_INDEXED_COL =>
        new FilterReason(
          code.toString,
          argStrings,
          s"Index does not contain required columns for ${argStrings(0)._2} subplan. " +
            s"Required indexed columns: [${argStrings(1)._2}], " +
            s"Indexed columns: [${argStrings(2)._2}]")
      case NO_COMPATIBLE_JOIN_INDEX_PAIR =>
        new FilterReason(
          code.toString,
          argStrings,
          "No compatible left and right index pair."
        )
      case ANOTHER_INDEX_APPLIED =>
        new FilterReason(
          code.toString,
          argStrings,
          s"Another candidate index is applied: ${argStrings(0)._2}"
        )

    }
  }
}
