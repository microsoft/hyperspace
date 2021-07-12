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

object FilterReasonCode extends Enumeration {
  type FilterReasonCode = Value

  val COL_SCHEMA_MISMATCH = Value("COL_SCHEMA_MISMATCH")
  val SOURCE_DATA_CHANGE = Value("SOURCE_DATA_CHANGE")
  val NO_DELETE_SUPPORT = Value("NO_DELETE_SUPPORT")
  val NO_COMMON_FILES = Value("NO_COMMON_FILES")
  val TOO_MUCH_APPENDED = Value("TOO_MUCH_APPENDED")
  val TOO_MUCH_DELETED = Value("TOO_MUCH_DELETED")
  val ANOTHER_INDEX_APPLIED = Value("ANOTHER_INDEX_APPLIED")

  // CoveringIndex - FilterIndexRule
  val NO_FIRST_INDEXED_COL_COND = Value("NO_FIRST_INDEXED_COL_COND")
  val MISSING_REQUIRED_COL = Value("MISSING_REQUIRED_COL")

  // CoveringIndex - JoinIndexRule
  val NOT_ELIGIBLE_JOIN = Value("NOT_ELIGIBLE_JOIN")
  val NO_AVAIL_JOIN_INDEX_PAIR = Value("NO_AVAIL_INDEX_PAIR")
  val NO_COMPATIBLE_JOIN_INDEX_PAIR = Value("NO_COMPATIBLE_INDEX_PAIR")
  val NOT_ALL_JOIN_COL_INDEXED = Value("NOT_ALL_JOIN_COL_INDEXED")
  val MISSING_INDEXED_COL = Value("MISSING_INDEXED_COL")
}
