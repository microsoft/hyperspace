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

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.FileIndex
import org.apache.spark.util.hyperspace.Utils

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.{FileIdTracker, Relation}
import com.microsoft.hyperspace.util.{CacheWithTransform, HyperspaceConf}

/**
 * [[FileBasedSourceProviderManager]] is responsible for loading source providers which implements
 * [[FileBasedSourceProvider]] and running APIs for each provider loaded.
 *
 * Each API in [[FileBasedSourceProvider]] returns [[Option]] and this manager ensures that only
 * one provider returns [[Some]] for each API.
 *
 * @param spark Spark session.
 */
class FileBasedSourceProviderManager(spark: SparkSession) {
  private val sourceProviders: CacheWithTransform[String, Seq[FileBasedSourceProvider]] =
    new CacheWithTransform[String, Seq[FileBasedSourceProvider]]({ () =>
      HyperspaceConf.fileBasedSourceBuilders(spark)
    }, { builderClassNames =>
      buildProviders(builderClassNames)
    })

  /**
   * Runs createRelation() for each provider.
   *
   * @param logicalPlan Logical plan to create [[Relation]] from.
   * @param fileIdTracker [[FileIdTracker]] to use when populating the data of [[Relation]].
   * @return [[Relation]] created from the given logical relation.
   * @throws HyperspaceException if multiple providers returns [[Some]] or
   *                             if no providers return [[Some]].
   */
  def createRelation(logicalPlan: LogicalPlan, fileIdTracker: FileIdTracker): Relation = {
    run(p => p.createRelation(logicalPlan, fileIdTracker))
  }

  /**
   * Runs refreshRelation() for each provider.
   *
   * @param relation [[Relation]] to refresh.
   * @return Refreshed [[Relation]].
   * @throws HyperspaceException if multiple providers returns [[Some]] or
   *                             if no providers return [[Some]].
   */
  def refreshRelation(relation: Relation): Relation = {
    run(p => p.refreshRelation(relation))
  }

  /**
   * Runs internalFileFormatName() for each provider.
   *
   * @param relation [[Relation]] object to read internal data files.
   * @return File format to read internal data files.
   */
  def internalFileFormatName(relation: Relation): String = {
    run(p => p.internalFileFormatName(relation))
  }

  /**
   * Runs signature() for each provider.
   *
   * @param logicalPlan Logical plan to compute signature from.
   * @return Computed signature.
   * @throws HyperspaceException if multiple providers returns [[Some]] or
   *                             if no providers return [[Some]].
   */
  def signature(logicalPlan: LogicalPlan): String = {
    run(p => p.signature(logicalPlan))
  }

  /**
   * Runs allFiles() for each provider.
   *
   * @param logicalPlan Logical plan to retrieve all input files.
   * @return List of all input files.
   * @throws HyperspaceException if multiple providers returns [[Some]] or
   *                             if no providers return [[Some]].
   */
  def allFiles(logicalPlan: LogicalPlan): Seq[FileStatus] = {
    run(p => p.allFiles(logicalPlan))
  }

  /**
   * Runs partitionBasePath() for each provider.
   *
   * @param location Partitioned location.
   * @return Optional basePath string to read the given partitioned location.
   * @throws HyperspaceException if multiple providers returns [[Some]] or
   *                             if no providers return [[Some]].
   */
  def partitionBasePath(location: FileIndex): Option[String] = {
    run(p => p.partitionBasePath(location))
  }

  /**
   * Runs lineagePairs() for each provider.
   *
   * @param logicalPlan Logical plan to check the relation type.
   * @param fileIdTracker [[FileIdTracker]] to create the list of (file path, file id).
   * @return List of (file path, file id).
   * @throws HyperspaceException if multiple providers returns [[Some]] or
   *                             if no providers return [[Some]].
   */
  def lineagePairs(
      logicalPlan: LogicalPlan,
      fileIdTracker: FileIdTracker): Seq[(String, Long)] = {
    run(p => p.lineagePairs(logicalPlan, fileIdTracker))
  }

  /**
   * Returns whether the given relation has parquet source files or not.
   *
   * @param logicalPlan Logical plan to check the source file format.
   * @return True if source files in the given relation are parquet.
   */
  def hasParquetAsSourceFormat(logicalPlan: LogicalPlan): Boolean = {
    run(p => p.hasParquetAsSourceFormat(logicalPlan))
  }

  /**
   * Runs the given function 'f', which executes a [[FileBasedSourceProvider]]'s API that returns
   * [[Option]] for each provider built. This function ensures that only one provider returns
   * [[Some]] when 'f' is executed.
   *
   * @param f Function that runs a [[FileBasedSourceProvider]]'s API that returns [[Option]]
   *          given a provider.
   * @tparam A Type of the object that 'f' returns, wrapped in [[Option]].
   * @return The object in [[Some]] that 'f' returns.
   */
  private def run[A](f: FileBasedSourceProvider => Option[A]): A = {
    sourceProviders
      .load()
      .foldLeft(Option.empty[(A, FileBasedSourceProvider)]) { (result, provider) =>
        val cur = f(provider)
        if (cur.isDefined) {
          if (result.isDefined) {
            throw HyperspaceException(
              "Multiple source providers returned valid results: " +
                s"'${provider.getClass.getName}' and '${result.get._2.getClass.getName}'")
          }
          Some(cur.get, provider)
        } else {
          result
        }
      }
      .map(_._1)
      .getOrElse {
        throw HyperspaceException("No source provider returned valid results.")
      }
  }

  /**
   * Given a comma separated class names that implement [[SourceProviderBuilder]], this method
   * builds source providers that implement [[FileBasedSourceProvider]].
   *
   * @param builderClassNames Name of classes to load as [[SourceProviderBuilder]].
   * @return [[FileBasedSourceProvider]] objects built.
   * @throws HyperspaceException if given builders cannot be loaded or
   *                             if builder doesn't build [[FileBasedSourceProvider]].
   */
  private def buildProviders(builderClassNames: String): Seq[FileBasedSourceProvider] = {
    val builders = builderClassNames.split(",").map(_.trim).map { name =>
      Try(Utils.classForName(name).getConstructor().newInstance()) match {
        case Success(builder: SourceProviderBuilder) => builder
        case _ => throw HyperspaceException(s"Cannot load SourceProviderBuilder: '$name'")
      }
    }

    builders.map { builder =>
      builder.build(spark) match {
        case p: FileBasedSourceProvider => p
        case other =>
          throw HyperspaceException(
            s"'$builder' did not build FileBasedSourceProvider: '$other')")
      }
    }
  }
}
