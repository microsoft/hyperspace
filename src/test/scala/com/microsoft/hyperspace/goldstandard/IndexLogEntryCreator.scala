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

package com.microsoft.hyperspace.goldstandard

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.util.PathUtils

object IndexLogEntryCreator {
  def createIndex(index: IndexDefinition, spark: SparkSession): Unit = {
    val indexPath =
      PathUtils.makeAbsolute(s"${spark.conf.get(IndexConstants.INDEX_SYSTEM_PATH)}/${index.name}")
    val entry = getIndexLogEntry(index, indexPath, spark)
    assert(new IndexLogManagerImpl(indexPath).writeLog(0, entry))
  }

  private def toRelation(sourceDf: DataFrame): Relation = {
    val leafPlans = sourceDf.queryExecution.optimizedPlan.collectLeaves()
    assert(leafPlans.size == 1)
    leafPlans.head match {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, dataSchema, _, _, _),
          _,
          _,
          _) =>
        val sourceDataProperties =
          Hdfs.Properties(Content.fromDirectory(location.rootPaths.head, new FileIdTracker))

        Relation(
          location.rootPaths.map(_.toString),
          Hdfs(sourceDataProperties),
          dataSchema.json,
          "parquet",
          Map.empty)
      case _ => throw HyperspaceException("Unexpected relation found.")
    }
  }

  private def getIndexLogEntry(
      index: IndexDefinition,
      indexPath: Path,
      spark: SparkSession): IndexLogEntry = {
    val indexRootPath = new Path(indexPath, "v__=0")
    val sourceDf = spark.table(index.tableName)
    val indexSchema = {
      val allCols = index.indexedCols ++ index.includedCols
      StructType(sourceDf.schema.filter(f => allCols.contains(f.name)))
    }
    val relation = toRelation(sourceDf)

    val sourcePlanProperties = SparkPlan.Properties(
      Seq(relation),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(
          Seq(
            Signature(
              "com.microsoft.hyperspace.goldstandard.MockSignatureProvider",
              index.tableName)))))

    val entry = IndexLogEntry(
      index.name,
      CoveringIndex(
        CoveringIndex.Properties(
          CoveringIndex.Properties
            .Columns(index.indexedCols, index.includedCols),
          IndexLogEntry.schemaString(indexSchema),
          200,
          Map(IndexConstants.HAS_PARQUET_AS_SOURCE_FORMAT_PROPERTY -> "true"))),
      Content(Directory.fromDirectory(indexRootPath, new FileIdTracker)),
      Source(SparkPlan(sourcePlanProperties)),
      Map())
    entry.state = Constants.States.ACTIVE
    entry
  }
}
