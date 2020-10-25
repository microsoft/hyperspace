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

package com.microsoft.hyperspace.index.sources

import scala.util.{Success, Try}

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.util.hyperspace.Utils

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.Relation

/**
 *
 * @param spark
 */
class SourceProviderManager(spark: SparkSession) {
  private lazy val builders: Seq[SourceProviderBuilder] = {
    val builders = spark.sessionState.conf
      .getConfString(
        "spark.hyperspace.index.sources.builders",
        "com.microsoft.hyperspace.index.sources.default.DefaultFileBasedSourceBuilder")

    builders.split(",").map(_.trim).map { name =>
      Try(Utils.classForName(name).getConstructor().newInstance()) match {
        case Success(builder: SourceProviderBuilder) => builder
        case _ => throw HyperspaceException(s"Cannot load SourceProviderBuilder: '$name'")
      }
    }
  }

  private lazy val sourceProviders: Seq[SourceProvider] = builders.map(_.build(spark))

  /**
   *
   * @param logicalRelation
   * @return
   */
  def createRelation(logicalRelation: LogicalRelation): Relation = {
    sourceProviders.view
      .map(provider => provider.createRelation(logicalRelation))
      .collectFirst { case Some(x) => x }
      .getOrElse(
        throw HyperspaceException("No source providers could reconstruct the given relation."))
  }

  /**
   *
   */
  def reconstructDataFrame(relation: Relation): DataFrame = {
    sourceProviders.view
      .map(source => source.reconstructDataFrame(relation))
      .collectFirst { case Some(x) => x }
      .getOrElse(
        throw HyperspaceException("No source provider could reconstruct the given relation."))
  }

  /**
   *
   * @param logicalRelation
   * @return
   */
  def signature(logicalRelation: LogicalRelation): String = {
    sourceProviders.view
      .map(source => source.signature(logicalRelation))
      .collectFirst { case Some(x) => x }
      .getOrElse(throw HyperspaceException("No signature is found from source providers"))
  }
}
