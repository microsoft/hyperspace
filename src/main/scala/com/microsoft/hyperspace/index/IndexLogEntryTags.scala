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

package com.microsoft.hyperspace.index

// A tag of a `IndexLogEntry`, which defines name and type.
case class IndexLogEntryTag[T](name: String)

object IndexLogEntryTags {
  // INDEX_HYBRIDSCAN_REQUIRED_TAG indicates if Hybrid Scan is required for this index or not.
  // This is set in getCandidateIndexes and utilized in transformPlanToUseIndex.
  val INDEX_HYBRIDSCAN_REQUIRED_TAG: IndexLogEntryTag[Boolean] =
    IndexLogEntryTag[Boolean]("hybridScanRequired")

  // INDEX_COMMON_BYTES_TAG stores overlapping bytes of index source files and given relation.
  // This is set in getCandidateIndexes and utilized in rank functions.
  val INDEX_COMMON_BYTES_TAG: IndexLogEntryTag[Long] = IndexLogEntryTag[Long]("commonBytes")

}
