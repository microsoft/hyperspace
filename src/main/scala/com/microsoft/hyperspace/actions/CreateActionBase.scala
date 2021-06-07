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

package com.microsoft.hyperspace.actions

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.{DataFrame, SparkSession}

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException}
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.sources.FileBasedRelation
import com.microsoft.hyperspace.util.{PathUtils, ResolverUtils}

/**
 * CreateActionBase provides functionality to write dataframe as covering index.
 */
private[actions] abstract class CreateActionBase(dataManager: IndexDataManager)
    extends IndexerContext {
  override lazy val indexDataPath: Path = {
    dataManager
      .getLatestVersionId()
      .map(id => dataManager.getPath(id + 1))
      .getOrElse(dataManager.getPath(0))
  }

  override val fileIdTracker = new FileIdTracker

  protected def getIndexLogEntry(
      spark: SparkSession,
      df: DataFrame,
      indexName: String,
      index: Index,
      path: Path,
      versionId: Int): IndexLogEntry = {
    val hadoopConf = spark.sessionState.newHadoopConf()
    val absolutePath = PathUtils.makeAbsolute(path, hadoopConf)

    val signatureProvider = LogicalPlanSignatureProvider.create()

    val sourcePlan = df.queryExecution.optimizedPlan
    signatureProvider.signature(sourcePlan) match {
      case Some(s) =>
        val relation = RelationUtils.getRelation(spark, sourcePlan)
        val sourcePlanProperties = SparkPlan.Properties(
          Seq(relation.createRelationMetadata(fileIdTracker)),
          null,
          null,
          LogicalPlanFingerprint(
            LogicalPlanFingerprint.Properties(Seq(Signature(signatureProvider.name, s)))))

        val indexProperties =
          Hyperspace
            .getContext(spark)
            .sourceProviderManager
            .getRelationMetadata(sourcePlanProperties.relations.head)
            .enrichIndexProperties(
              index.properties
                + (IndexConstants.INDEX_LOG_VERSION -> versionId.toString)
                ++ hasParquetAsSourceFormatProperty(relation))

        IndexLogEntry.create(
          indexName,
          index.withNewProperties(indexProperties),
          Content.fromDirectory(absolutePath, fileIdTracker, hadoopConf),
          Source(SparkPlan(sourcePlanProperties)),
          Map())

      case None => throw HyperspaceException("Invalid plan for creating an index.")
    }
  }

  protected def updateFileIdTracker(spark: SparkSession, sourceData: DataFrame): Unit = {
    val signatureProvider = LogicalPlanSignatureProvider.create()
    signatureProvider.signature(sourceData.queryExecution.optimizedPlan) match {
      case Some(_) =>
        val relation = RelationUtils.getRelation(spark, sourceData.queryExecution.optimizedPlan)
        relation.createRelationMetadata(fileIdTracker)
      case None => throw HyperspaceException("Invalid plan for creating an index.")
    }
  }

  private def hasParquetAsSourceFormatProperty(
      relation: FileBasedRelation): Option[(String, String)] = {
    if (relation.hasParquetAsSourceFormat) {
      Some(IndexConstants.HAS_PARQUET_AS_SOURCE_FORMAT_PROPERTY -> "true")
    } else {
      None
    }
  }
}
