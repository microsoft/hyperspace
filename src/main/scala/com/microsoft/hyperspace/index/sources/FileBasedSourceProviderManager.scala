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
import org.apache.spark.sql.execution.datasources.{FileIndex, LogicalRelation}
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
   * @param logicalRelation Logical relation to create [[Relation]] from.
   * @param fileIdTracker [[FileIdTracker]] to use when populating the data of [[Relation]].
   * @return [[Relation]] created from the given logical relation.
   * @throws HyperspaceException if multiple providers returns [[Some]] or
   *                             if no providers return [[Some]].
   */
  def createRelation(logicalRelation: LogicalRelation, fileIdTracker: FileIdTracker): Relation = {
    run(p => p.createRelation(logicalRelation, fileIdTracker))
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
   * @param logicalRelation Logical relation to compute signature from.
   * @return Computed signature.
   * @throws HyperspaceException if multiple providers returns [[Some]] or
   *                             if no providers return [[Some]].
   */
  def signature(logicalRelation: LogicalRelation): String = {
    run(p => p.signature(logicalRelation))
  }

  /**
   * Runs allFiles() for each provider.
   *
   * @param logicalRelation Logical relation to retrieve all input files.
   * @return List of all input files.
   * @throws HyperspaceException if multiple providers returns [[Some]] or
   *                             if no providers return [[Some]].
   */
  def allFiles(logicalRelation: LogicalRelation): Seq[FileStatus] = {
    run(p => p.allFiles(logicalRelation))
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
   * @param logicalRelation Logical Relation to check the relation type.
   * @param fileIdTracker [[FileIdTracker]] to create the list of (file path, file id).
   * @return List of (file path, file id).
   * @throws HyperspaceException if multiple providers returns [[Some]] or
   *                             if no providers return [[Some]].
   */
  def lineagePairs(
      logicalRelation: LogicalRelation,
      fileIdTracker: FileIdTracker): Seq[(String, Long)] = {
    run(p => p.lineagePairs(logicalRelation, fileIdTracker))
  }

  /**
   * Returns whether the given relation has parquet source files or not.
   *
   * @param logicalRelation Logical Relation to check the source file format.
   * @return True if source files in the given relation are parquet.
   */
  def hasParquetAsSourceFormat(logicalRelation: LogicalRelation): Boolean = {
    run(p => p.hasParquetAsSourceFormat(logicalRelation))
  }

  /**
   * Returns true if the given logical plan is a supported relation. If all of the registered
   * providers return None, this returns false. (e.g, the given plan could be RDD-based relation,
   * which is not applicable).
   *
   * @param plan Logical plan to check if it's supported.
   * @return True if the given plan is a supported relation.
   */
  def isSupportedRelation(plan: LogicalPlan): Boolean = {
    runWithDefault(p => p.isSupportedRelation(plan))(false)
  }

  /**
   * Returns the [[FileBasedRelation]] that wraps the given logical plan.
   * If you are using this from an extractor, check if the logical plan
   * is supported first by using [[isSupportedRelation]]. Otherwise, HyperspaceException can be
   * thrown if none of the registered providers supports the given plan.
   *
   * @param plan Logical plan to wrap to [[FileBasedRelation]]
   * @return [[FileBasedRelation]] that wraps the given logical plan.
   */
  def getRelation(plan: LogicalPlan): FileBasedRelation = {
    run(p => p.getRelation(plan))
  }

  /**
   * Runs the given function 'f', which executes a [[FileBasedSourceProvider]]'s API that returns
   * [[Option]] for each provider built. This function ensures that only one provider returns
   * [[Some]] when 'f' is executed. If all of the providers return None, it will throw
   * HyperspaceException.
   *
   * @param f Function that runs a [[FileBasedSourceProvider]]'s API that returns [[Option]]
   *          given a provider.
   * @tparam A Type of the object that 'f' returns, wrapped in [[Option]].
   * @return The object in [[Some]] that 'f' returns.
   */
  private def run[A](f: FileBasedSourceProvider => Option[A]): A = {
    runWithDefault(f) {
      throw HyperspaceException("No source provider returned valid results.")
    }
  }

  /**
   * Runs the given function 'f', which executes a [[FileBasedSourceProvider]]'s API that returns
   * [[Option]] for each provider built. This function ensures that only one provider returns
   * [[Some]] when 'f' is executed. If all of the providers return None, it will return the given
   * default value.
   *
   * @param f Function that runs a [[FileBasedSourceProvider]]'s API that returns [[Option]]
   *          given a provider.
   * @param default Expression to compute the default value.
   * @tparam A Type of the object that 'f' returns, wrapped in [[Option]].
   * @return The object in [[Some]] that 'f' returns.
   */
  private def runWithDefault[A](f: FileBasedSourceProvider => Option[A])(default: => A): A = {
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
        default
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
