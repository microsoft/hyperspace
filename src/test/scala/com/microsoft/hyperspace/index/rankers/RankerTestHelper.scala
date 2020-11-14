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

package com.microsoft.hyperspace.index.rankers

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{StructField, StructType}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.{Content, CoveringIndex, Directory, FileInfo, Hdfs, IndexLogEntry, IndexLogEntryTags, LogicalPlanFingerprint, Relation, Signature, Source, SparkPlan}
import com.microsoft.hyperspace.index.Hdfs.Properties

object RankerTestHelper {
  def createIndex(
      name: String,
      indexCols: Seq[AttributeReference],
      includedCols: Seq[AttributeReference],
      numBuckets: Int = 10,
      inputFiles: Seq[FileInfo] = Seq(),
      plan: LogicalPlan = null): IndexLogEntry = {
    val sourcePlanProperties = SparkPlan.Properties(
      Seq(
        Relation(
          Seq("dummy"),
          Hdfs(Properties(Content(Directory("/", files = inputFiles)))),
          "schema",
          "format",
          Map())),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(Seq(Signature("signClass", "sign(plan)")))))

    val entry = IndexLogEntry(
      name,
      CoveringIndex(
        CoveringIndex.Properties(
          CoveringIndex.Properties
            .Columns(indexCols.map(_.name), includedCols.map(_.name)),
          IndexLogEntry.schemaString(schemaFromAttributes(indexCols ++ includedCols: _*)),
          numBuckets)),
      Content(Directory(name)),
      Source(SparkPlan(sourcePlanProperties)),
      Map())
    val commonBytes = inputFiles.foldLeft(0L) { (bytes, f) =>
      bytes + f.size
    }
    // This tag is originally set in getCandidateIndex, for Hybrid Scan.
    // Set tag for testing rank algorithms.
    entry.setTagValue(plan, IndexLogEntryTags.COMMON_BYTES, commonBytes)
    entry.state = Constants.States.ACTIVE
    entry
  }

  private def schemaFromAttributes(attributes: Attribute*): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
}
