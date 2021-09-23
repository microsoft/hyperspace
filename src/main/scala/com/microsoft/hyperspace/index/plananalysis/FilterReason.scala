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

trait FilterReason {
  val args: Seq[(String, String)]
  def codeStr: String
  final def argStr: String = {
    // key1=[value1], key2=[value2]
    args.map(kv => s"${kv._1}=[${kv._2}]").mkString(", ")
  }
  def verboseStr: String
}

trait FilterReasonNoArg extends FilterReason {
  final override val args: Seq[(String, String)] = Seq.empty
}

object FilterReasons {

  case class ColSchemaMismatch(sourceColumns: String, indexColumns: String) extends FilterReason {
    override final val codeStr: String = "COL_SCHEMA_MISMATCH"
    override val args = Seq("sourceColumns" -> sourceColumns, "indexColumns" -> indexColumns)
    override def verboseStr: String = {
      s"Column Schema does not match. Source data columns: [$sourceColumns], " +
        s"Index columns: [$indexColumns]"
    }
  }

  case class SourceDataChanged() extends FilterReasonNoArg {
    override final val codeStr: String = "SOURCE_DATA_CHANGED"
    override def verboseStr: String = "Index signature does not match."
  }

  case class NoDeleteSupport() extends FilterReasonNoArg {
    override def codeStr: String = "NO_DELETE_SUPPORT"
    override def verboseStr: String = "Index doesn't support deleted files."
  }

  case class NoCommonFiles() extends FilterReasonNoArg {
    override def codeStr: String = "NO_COMMON_FILES"
    override def verboseStr: String = "No common files."
  }

  case class TooMuchAppended(appendedRatio: String, hybridScanAppendThreshold: String)
      extends FilterReason {
    override final def codeStr: String = "TOO_MUCH_APPENDED"
    override val args = Seq(
      "appendedRatio" -> appendedRatio,
      "hybridScanAppendThreshold" -> hybridScanAppendThreshold)
    override def verboseStr: String =
      s"Appended bytes ratio ($appendedRatio) is larger than " +
        s"threshold config $hybridScanAppendThreshold). "
  }

  case class TooMuchDeleted(deletedRatio: String, hybridScanDeleteThreshold: String)
      extends FilterReason {
    override final def codeStr: String = "TOO_MUCH_DELETED"
    override val args = Seq(
      "deletedRatio" -> deletedRatio,
      "hybridScanDeleteThreshold" -> hybridScanDeleteThreshold)
    override def verboseStr: String =
      s"Deleted bytes ratio ($deletedRatio) is larger than " +
        s"threshold config $hybridScanDeleteThreshold). "
  }

  case class MissingRequiredCol(requiredCols: String, indexCols: String) extends FilterReason {
    override final def codeStr: String = "MISSING_REQUIRED_COL"
    override val args = Seq("requiredCols" -> requiredCols, "indexCols" -> indexCols)
    override def verboseStr: String =
      s"Index does not contain required columns. Required columns: [$requiredCols], " +
        s"Index columns: [$indexCols]"
  }

  case class NoFirstIndexedColCond(firstIndexedCol: String, filterCols: String)
      extends FilterReason {
    override final def codeStr: String = "NO_FIRST_INDEXED_COL_COND"
    override val args = Seq("firstIndexedCol" -> firstIndexedCol, "filterCols" -> filterCols)
    override def verboseStr: String =
      "The first indexed column should be used in filter conditions. " +
        s"The first indexed column: $firstIndexedCol, " +
        s"Columns in filter condition: [$filterCols]"
  }

  case class NotEligibleJoin(reason: String) extends FilterReason {
    override final def codeStr: String = "NOT_ELIGIBLE_JOIN"
    override val args = Seq("reason" -> reason)
    override def verboseStr: String =
      s"Join condition is not eligible. Reason: $reason"
  }

  case class NoAvailJoinIndexPair(leftOrRight: String) extends FilterReason {
    override def codeStr: String = "NO_AVAIL_JOIN_INDEX_PAIR"
    override val args = Seq("child" -> leftOrRight)
    override def verboseStr: String =
      s"No available indexes for $leftOrRight subplan. " +
        "Both left and right indexes are required for Join query."
  }

  case class MissingIndexedCol(
      leftOrRight: String,
      requiredIndexedCols: String,
      indexedCols: String)
      extends FilterReason {
    override final def codeStr: String = "MISSING_INDEXED_COL"
    override val args = Seq(
      "child" -> leftOrRight,
      "requiredIndexedCols" -> requiredIndexedCols,
      "indexedCols" -> indexedCols)
    override def verboseStr: String =
      s"Index does not contain required columns for $leftOrRight subplan. " +
        s"Required indexed columns: [$requiredIndexedCols], " +
        s"Indexed columns: [$indexedCols]"
  }

  case class NotAllJoinColIndexed(leftOrRight: String, joinCols: String, indexedCols: String)
      extends FilterReason {
    override final def codeStr: String = "NOT_ALL_JOIN_COL_INDEXED"
    override val args =
      Seq("child" -> leftOrRight, "joinCols" -> joinCols, "indexedCols" -> indexedCols)
    override def verboseStr: String =
      s"All join condition column and indexed column should be the same. " +
        s"Join columns: [$joinCols], Indexed columns for $leftOrRight subplan: [$indexedCols]"
  }

  case class NoCompatibleJoinIndexPair() extends FilterReasonNoArg {
    override final def codeStr: String = "NO_COMPATIBLE_JOIN_INDEX_PAIR"
    override def verboseStr: String = "No compatible left and right index pair."
  }

  case class AnotherIndexApplied(appliedIndex: String) extends FilterReason {
    override final def codeStr: String = "ANOTHER_INDEX_APPLIED"
    override val args = Seq("appliedIndex" -> appliedIndex)
    override def verboseStr: String =
      s"Another candidate index is applied: $appliedIndex"
  }

  case class IneligibleFilterCondition(condition: String) extends FilterReason {
    override final def codeStr: String = "INELIGIBLE_FILTER_CONDITION"
    override val args = Seq("condition" -> condition)
    override def verboseStr: String =
      s"Ineligible filter condition: $condition"
  }
}
