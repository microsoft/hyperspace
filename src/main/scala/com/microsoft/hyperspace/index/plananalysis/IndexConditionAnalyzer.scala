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

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.{FileInfo, IndexConstants, IndexLogEntry, LogicalPlanSignatureProvider, PathResolver}
import com.microsoft.hyperspace.util.ResolverUtils

object IndexConditionAnalyzer {

  def whyNotString(spark: SparkSession, df: DataFrame, entry: IndexLogEntry): String = {
    // Collect all possible logical relations & its signatures.
    val relations = df.queryExecution.optimizedPlan.collect {
      case relation @ LogicalRelation(_: HadoopFsRelation, _, _, _) =>
        relation
    }

    if (relations.isEmpty) {
      return "No applicable relation type in query. Hyperspace only supports HadoopFsRelation."
    }

    // Check Column Schema.
    val allColumnsInIndex = entry.indexedColumns ++ entry.includedColumns
    val relationsAfterSchemaCheck = relations.filter { rel =>
      val relationCols = rel.output.map(_.name)
      ResolverUtils.resolve(spark, allColumnsInIndex, relationCols).isDefined
    }

    if (relationsAfterSchemaCheck.isEmpty) {
      return s"Not found relation including all indexed/included columns in the given index. " +
        s"Column Names: $allColumnsInIndex"
    }

    val sourcePlanSignatures = entry.source.plan.properties.fingerprint.properties.signatures
    val sourcePlanSignature = sourcePlanSignatures.head

    val relationsAfterSignatureCheck = relationsAfterSchemaCheck.filter { rel =>
      LogicalPlanSignatureProvider
        .create(sourcePlanSignature.provider)
        .signature(rel) match {
        case Some(s) => s.equals(sourcePlanSignature.value)
        case None => false
      }
    }

    val outputStream = BufferStream(PlanAnalyzerUtils.getDisplayMode(spark))

    if (relationsAfterSignatureCheck.isEmpty) {
      outputStream.writeLine("Inapplicable relations in the given query:")
      relationsAfterSchemaCheck.map {
        case rel @ LogicalRelation(
              HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
              _,
              _,
              _) =>
          val curFiles = Hyperspace
            .getContext(spark)
            .sourceProviderManager
            .allFiles(rel)
            .map(
              f =>
                FileInfo(
                  f.getPath.toString,
                  f.getLen,
                  f.getModificationTime,
                  IndexConstants.UNKNOWN_FILE_ID))
            .toSet

          val appendedFiles = curFiles -- entry.sourceFileInfoSet
          val deletedFiles = entry.sourceFileInfoSet -- curFiles
          outputStream.writeLine(rel.schemaString)
          val numCommonFiles = entry.sourceFileInfoSet.size - deletedFiles.size
          if (numCommonFiles == 0) {
            // No common files.
            outputStream.writeLine("  - No common source file.")
            outputStream.writeLine(s"    - Relation files sample: ${curFiles.head.name}")
            outputStream.writeLine(
              s"    - Index files sample: ${entry.sourceFileInfoSet.head.name}")
          } else {
            outputStream.writeLine(
              s"  - Detected $numCommonFiles of common source file(s) between " +
                s"${entry.sourceFileInfoSet.size} of index files and ${curFiles.size} of " +
                "relation files.")
            if (appendedFiles.nonEmpty) {
              outputStream.writeLine(s"  - Detected ${appendedFiles.size} of appended file(s).")
              outputStream.writeLine(s"    - Appended files sample: ${appendedFiles.head.name}")
            }
            if (deletedFiles.nonEmpty) {
              outputStream.writeLine(s"    - Detected ${deletedFiles.size} of deleted file(s).")
              outputStream.writeLine(s"      - Deleted files sample: ${deletedFiles.head.name}")
            }
          }
      }
      return outputStream.toString
    }

    {
      outputStream.writeLine("Candidate logical relations after signature match:")
      relationsAfterSchemaCheck.foreach { rel =>
        outputStream.writeLine(rel.schema.toString)
      }
      outputStream.writeLine()

      PlanAnalyzerUtils.withHyperspaceState(spark, desiredState = true) {
        val originalPlan = spark.sessionState
          .executePlan(df.queryExecution.optimizedPlan)
          .executedPlan
        val indexPath = PathResolver(spark.sessionState.conf).getIndexPath(entry.name).toString
        if (PlanAnalyzerUtils.getPaths(originalPlan).exists(f => f.startsWith(indexPath))) {
          outputStream.writeLine("The index is applied to the given query. Plan:")
          outputStream.writeLine(originalPlan.toString)
          return outputStream.toString
        }
        outputStream.writeLine("Given query plan using Hyperspace:")
        outputStream.writeLine(originalPlan.toString)
      }

      // TODO - check query plan for each candidate relation.
      // TODO - elaborate possible causes.
      outputStream.writeLine("There can be several possible causes:")
      outputStream.writeLine(
        "  - Filter condition: " +
          "filter column should be the first \"indexed\" column.")
      outputStream.writeLine("  - Join condition: only support Equi-join and ANDs.")
      outputStream.writeLine(
        "  - Join condition: " +
          "both left and right child should have a candidate index.")
      outputStream.writeLine("  - Rule order: Join Rule is prior to Filter Rule.")
      outputStream.writeLine("  - Index rank: another index is applied to the relation.")
      outputStream.toString
    }
  }
}
