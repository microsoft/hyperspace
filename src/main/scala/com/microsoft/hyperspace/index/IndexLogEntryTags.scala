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

object IndexLogEntryTags {
  // HYBRIDSCAN_REQUIRED indicates if Hybrid Scan is required for this index or not.
  // This is set in getCandidateIndexes and utilized in transformPlanToUseIndex.
  val HYBRIDSCAN_REQUIRED: IndexLogEntryTag[Boolean] =
    IndexLogEntryTag[Boolean]("hybridScanRequired")

  // COMMON_BYTES stores overlapping bytes of index source files and given relation.
  // This is set in getCandidateIndexes and utilized in rank functions.
  val COMMON_BYTES: IndexLogEntryTag[Long] = IndexLogEntryTag[Long]("commonBytes")
}
