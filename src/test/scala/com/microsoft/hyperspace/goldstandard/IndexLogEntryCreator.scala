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
  def createIndex(indexDefinition: String, spark: SparkSession): Unit = {
    val splits = indexDefinition.split(";")
    val indexName = splits(0)
    val tableName = splits(1)
    val indexCols = splits(2).split(",").toSeq
    val includedCols = splits(3).split(",").toSeq
    val indexConfig = IndexConfig(indexName, indexCols, includedCols)
    val indexPath = {
      PathUtils.makeAbsolute(s"${spark.conf.get(IndexConstants.INDEX_SYSTEM_PATH)}/$indexName")
    }
    val entry = getIndexLogEntry(indexConfig, indexPath, tableName, spark)
    new IndexLogManagerImpl(indexPath).writeLog(0, entry)
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
      config: IndexConfig,
      indexPath: Path,
      tableName: String,
      spark: SparkSession): IndexLogEntry = {
    val indexRootPath = new Path(indexPath, "v__=0")
    val sourceDf = spark.table(tableName)
    val indexSchema = {
      val allCols = config.indexedColumns ++ config.includedColumns
      StructType(sourceDf.schema.filter(f => allCols.contains(f.name)))
    }
    val relation = toRelation(sourceDf)

    val sourcePlanProperties = SparkPlan.Properties(
      Seq(relation),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(Seq(
          Signature("com.microsoft.hyperspace.goldstandard.MockSignatureProvider", tableName)))))

    val entry = IndexLogEntry(
      config.indexName,
      CoveringIndex(
        CoveringIndex.Properties(
          CoveringIndex.Properties
            .Columns(config.indexedColumns, config.includedColumns),
          IndexLogEntry.schemaString(indexSchema),
          200,
          Map("hasParquetAsSourceFormat" -> "true"))),
      Content(Directory.fromDirectory(indexRootPath, new FileIdTracker)),
      Source(SparkPlan(sourcePlanProperties)),
      Map())
    entry.state = Constants.States.ACTIVE
    entry
  }
}
