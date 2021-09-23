/*
 * Copyright (2021) The Hyperspace Project Authors.
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

import java.net.URLDecoder

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.udf

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.util.ResolverUtils
import com.microsoft.hyperspace.util.ResolverUtils.ResolvedColumn

object IndexUtils {

  /**
   * Returns whether the lineage feature is enabled for the index by looking at
   * the index properties.
   *
   * If the index has no corresponding key in the property map,
   * [[IndexConstants.INDEX_LINEAGE_ENABLED_DEFAULT]] will be returned.
   *
   * @param properties Index properties
   */
  def hasLineageColumn(properties: Map[String, String]): Boolean = {
    properties
      .getOrElse(IndexConstants.LINEAGE_PROPERTY, IndexConstants.INDEX_LINEAGE_ENABLED_DEFAULT)
      .toBoolean
  }

  def resolveColumns(df: DataFrame, columns: Seq[String]): Seq[ResolvedColumn] = {
    val spark = df.sparkSession
    val plan = df.queryExecution.analyzed
    val resolvedColumns = ResolverUtils.resolve(spark, columns, plan)

    resolvedColumns match {
      case Some(cs) => cs
      case _ =>
        val unresolvedColumns = columns
          .map(c => (c, ResolverUtils.resolve(spark, Seq(c), plan).map(_.map(_.name))))
          .collect { case (c, r) if r.isEmpty => c }
        throw HyperspaceException(
          s"Columns '${unresolvedColumns.mkString(",")}' could not be resolved " +
            s"from available source columns:\n${df.schema.treeString}")
    }
  }

  /**
   * Spark UDF to decode the URL-encoded string returned by input_file_name().
   */
  lazy val decodeInputFileName = udf(
    (p: String) => URLDecoder.decode(p.replace("+", "%2B"), "UTF-8"))

  /**
   * Returns the path part of the URI-like string.
   *
   * This can be used to compare the results of input_file_name() and the paths
   * stored in FileIdTracker.
   */
  lazy val getPath = udf((p: String) => new Path(p).toUri.getPath)
}
