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

import org.apache.spark.sql.execution.datasources.InMemoryFileIndex

object IndexLogEntryTags {
  // HYBRIDSCAN_REQUIRED indicates if Hybrid Scan is required for the index or not.
  val HYBRIDSCAN_REQUIRED: IndexLogEntryTag[Boolean] =
    IndexLogEntryTag[Boolean]("hybridScanRequired")

  // COMMON_SOURCE_SIZE_IN_BYTES stores overlapping bytes of index source files and given relation.
  val COMMON_SOURCE_SIZE_IN_BYTES: IndexLogEntryTag[Long] =
    IndexLogEntryTag[Long]("commonSourceSizeInBytes")

  // SIGNATURE_MATCHED indicates if the plan has the same signature value with the index or not.
  val SIGNATURE_MATCHED: IndexLogEntryTag[Boolean] =
    IndexLogEntryTag[Boolean]("signatureMatched")

  // IS_HYBRIDSCAN_CANDIDATE indicates if the index can be applied to the plan using Hybrid Scan.
  // This tag is reset when HYBRIDSCAN_RELATED_CONFIGS was changed.
  val IS_HYBRIDSCAN_CANDIDATE: IndexLogEntryTag[Boolean] =
    IndexLogEntryTag[Boolean]("isHybridScanCandidate")

  // HYBRIDSCAN_RELATED_CONFIGS contains Seq of value strings of Hybrid Scan related configs.
  val HYBRIDSCAN_RELATED_CONFIGS: IndexLogEntryTag[Seq[String]] =
    IndexLogEntryTag[Seq[String]]("hybridScanRelatedConfigs")

  // INMEMORYFILEINDEX_INDEX_ONLY stores InMemoryFileIndex for index only scan.
  val INMEMORYFILEINDEX_INDEX_ONLY: IndexLogEntryTag[InMemoryFileIndex] =
    IndexLogEntryTag[InMemoryFileIndex]("inMemoryFileIndexIndexOnly")

  // INMEMORYFILEINDEX_HYBRID_SCAN stores InMemoryFileIndex including index data files and also
  // appended files for Hybrid Scan.
  val INMEMORYFILEINDEX_HYBRID_SCAN: IndexLogEntryTag[InMemoryFileIndex] =
  IndexLogEntryTag[InMemoryFileIndex]("inMemoryFileIndexHybridScan")

  // INMEMORYFILEINDEX_HYBRID_SCAN_APPENDED stores InMemoryFileIndex including only appended files
  // for Hybrid Scan.
  val INMEMORYFILEINDEX_HYBRID_SCAN_APPENDED: IndexLogEntryTag[InMemoryFileIndex] =
  IndexLogEntryTag[InMemoryFileIndex]("inMemoryFileIndexHybridScanAppended")

  // WHYNOT_REASON stores reason string for disqualification.
  val WHYNOT_REASON: IndexLogEntryTag[String] =
    IndexLogEntryTag[String]("whyNotReason")
}
