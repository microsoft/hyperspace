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
import com.microsoft.hyperspace.util.{CacheWithTransform, HyperspaceConf}

/**
 *
 * @param spark
 */
class FileBasedSourceProviderManager(spark: SparkSession) {
  private val sourceProviders: CacheWithTransform[String, Seq[FileBasedSourceProvider]] =
    new CacheWithTransform[String, Seq[FileBasedSourceProvider]]({ () =>
      HyperspaceConf.fileBasedSourceBuilders(spark)
    }, { builderClassNames =>
      providers(builderClassNames)
    })

  /**
   *
   * @param logicalRelation
   * @return
   */
  def createRelation(logicalRelation: LogicalRelation): Relation = {
    run(p => p.createRelation(logicalRelation))
  }

  /**
   *
   */
  def reconstructDataFrame(relation: Relation): DataFrame = {
    run(p => p.reconstructDataFrame(relation))
  }

  /**
   *
   * @param logicalRelation
   * @return
   */
  def signature(logicalRelation: LogicalRelation): String = {
    run(p => p.signature(logicalRelation))
  }

  /**
   *
   * @param f
   * @tparam A
   * @return
   */
  private def run[A](f: FileBasedSourceProvider => Option[A]): A = {
    sourceProviders
      .load()
      .foldLeft(Option.empty[(A, FileBasedSourceProvider)]) { (acc, p) =>
        val result = f(p)
        if (result.isDefined) {
          if (acc.isDefined) {
            throw HyperspaceException(
              "Multiple source providers returned valid results: " +
                s"'${p.getClass.getName}' and '${acc.get._2.getClass.getName}'")
          }
          Some(result.get, p)
        } else {
          acc
        }
      }
      .map(_._1)
      .getOrElse {
        throw HyperspaceException("No source provider returned valid results.")
      }
  }

  /**
   *
   * @param builderClassNames
   * @return
   */
  private def providers(builderClassNames: String): Seq[FileBasedSourceProvider] = {
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
            s"Builder '$builder' did not build FileBasedSourceProvider: '$other')")
      }
    }
  }
}
