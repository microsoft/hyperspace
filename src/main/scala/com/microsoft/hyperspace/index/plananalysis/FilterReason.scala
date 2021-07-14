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
    args.map(kv => s"${kv._1}=[${kv._2}]").mkString(",")
  }
  def verboseStr: String
}

trait FilterReasonNoArg extends FilterReason {
  final override val args: Seq[(String, String)] = Seq.empty
}

object FilterReasonCode extends Enumeration {
  type FilterReasonCode = Value

  // Common
  val COL_SCHEMA_MISMATCH = Value
  val SOURCE_DATA_CHANGED = Value
  val NO_DELETE_SUPPORT = Value
  val NO_COMMON_FILES = Value
  val TOO_MUCH_APPENDED = Value
  val TOO_MUCH_DELETED = Value
  val ANOTHER_INDEX_APPLIED = Value

  // CoveringIndex - FilterIndexRule
  val NO_FIRST_INDEXED_COL_COND = Value
  val MISSING_REQUIRED_COL = Value

  // CoveringIndex - JoinIndexRule
  val NOT_ELIGIBLE_JOIN = Value
  val NO_AVAIL_JOIN_INDEX_PAIR = Value
  val NO_COMPATIBLE_JOIN_INDEX_PAIR = Value
  val NOT_ALL_JOIN_COL_INDEXED = Value
  val MISSING_INDEXED_COL = Value
}

object FilterReasons {
  import com.microsoft.hyperspace.index.plananalysis.FilterReasonCode._
  def apply(code: FilterReasonCode, args: (String, String)*): FilterReason = {
    code match {
      case COL_SCHEMA_MISMATCH =>
        ColSchemaMismatch(args)
      case SOURCE_DATA_CHANGED =>
        SourceDataChanged()
      case NO_DELETE_SUPPORT =>
        NoDeleteSupport()
      case NO_COMMON_FILES =>
        NoCommonFiles()
      case TOO_MUCH_APPENDED =>
        TooMuchAppended(args)
      case TOO_MUCH_DELETED =>
        TooMuchDeleted(args)
      case MISSING_REQUIRED_COL =>
        MissingRequiredCol(args)
      case NO_FIRST_INDEXED_COL_COND =>
        NoFirstIndexedColCond(args)
      case NOT_ELIGIBLE_JOIN =>
        NotEligibleJoin(args)
      case NO_AVAIL_JOIN_INDEX_PAIR =>
        NoAvailJoinIndexPair(args)
      case MISSING_INDEXED_COL =>
        MissingIndexedCol(args)
      case NOT_ALL_JOIN_COL_INDEXED =>
        NotAllJoinColIndexed(args)
      case NO_COMPATIBLE_JOIN_INDEX_PAIR =>
        NoCompatibleJoinIndexPair()
      case ANOTHER_INDEX_APPLIED =>
        AnotherIndexApplied(args)
    }
  }

  case class ColSchemaMismatch(override val args: Seq[(String, String)])
      extends FilterReason {
    override final val codeStr: String = "COL_SCHEMA_MISMATCH"
    override def verboseStr: String = {
      "Column Schema does not match. Source data columns: [" + args(0)._2 +
        "], Index columns: [" + args(1)._2
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

  case class TooMuchAppended(override val args: Seq[(String, String)])
      extends FilterReason {
    override def codeStr: String = "TOO_MUCH_APPENDED"
    override def verboseStr: String =
      s"Appended bytes ratio (${args(0)._2}) is larger than " +
        s"threshold config ${args(1)._2}). "
  }

  case class TooMuchDeleted(override val args: Seq[(String, String)]) extends FilterReason {
    override def codeStr: String = "TOO_MUCH_DELETED"
    override def verboseStr: String =
      s"Deleted bytes ratio (${args(0)._2}) is larger than " +
        s"threshold config ${args(1)._2}). "
  }

  case class MissingRequiredCol(override val args: Seq[(String, String)])
      extends FilterReason {
    override def codeStr: String = "MISSING_REQUIRED_COL"
    override def verboseStr: String =
      s"Index does not contain required column. Required columns: [${args(0)._2}], " +
        s"Index columns: [${args(1)._2}]"
  }

  case class NoFirstIndexedColCond(override val args: Seq[(String, String)])
      extends FilterReason {
    override def codeStr: String = "NO_FIRST_INDEXED_COL_COND"
    override def verboseStr: String =
      "The first indexed column should be used in filter conditions. " +
        s"The first indexed column: ${args(0)._2}, " +
        s"Columns in filter condition: [${args(1)._2}]"
  }

  case class NotEligibleJoin(override val args: Seq[(String, String)])
      extends FilterReason {
    override def codeStr: String = "NOT_ELIGIBLE_JOIN"
    override def verboseStr: String =
      s"Join condition is not eligible. Reason: ${args(0)._2}"
  }

  case class NoAvailJoinIndexPair(override val args: Seq[(String, String)])
      extends FilterReason {
    override def codeStr: String = "NO_AVAIL_JOIN_INDEX_PAIR"
    override def verboseStr: String =
      s"No available indexes for ${args(0)._2} subplan. " +
        "Both left and right index are required for Join query"
  }

  case class MissingIndexedCol(override val args: Seq[(String, String)])
      extends FilterReason {
    override def codeStr: String = "MISSING_INDEXED_COL"
    override def verboseStr: String =
      s"Index does not contain required columns for ${args(0)._2} subplan. " +
        s"Required indexed columns: [${args(1)._2}], " +
        s"Indexed columns: [${args(2)._2}]"
  }

  case class NotAllJoinColIndexed(override val args: Seq[(String, String)])
      extends FilterReason {
    override def codeStr: String = "NOT_ALL_JOIN_COL_INDEXED"
    override def verboseStr: String =
      s"All join condition column should be the indexed columns. " +
        s"Join columns: [${args(0)._2}], Indexed columns: [${args(1)._2}]"
  }

  case class NoCompatibleJoinIndexPair() extends FilterReasonNoArg {
    override def codeStr: String = "NO_COMPATIBLE_JOIN_INDEX_PAIR"
    override def verboseStr: String = "No compatible left and right index pair."
  }

  case class AnotherIndexApplied(override val args: Seq[(String, String)])
      extends FilterReason {
    override def codeStr: String = "ANOTHER_INDEX_APPLIED"
    override def verboseStr: String =
      s"Another candidate index is applied: ${args(0)._2}"
  }
}
