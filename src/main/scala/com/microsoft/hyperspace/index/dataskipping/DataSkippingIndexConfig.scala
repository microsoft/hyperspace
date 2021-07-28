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

package com.microsoft.hyperspace.index.dataskipping

import scala.collection.mutable

import org.apache.spark.sql.DataFrame

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.{IndexConfigTrait, IndexerContext}
import com.microsoft.hyperspace.index.dataskipping.sketch.Sketch
import com.microsoft.hyperspace.index.dataskipping.util.ExpressionUtils

/**
 * DataSkippingIndexConfig is used to create a [[DataSkippingIndex]] via
 * [[Hyperspace.createIndex]].
 *
 * A sketch is a set of values for a file that contains aggregated information
 * about columns to be indexed. For example, a MinMax sketch on a column X
 * stores two values per file, min(X) and max(X) for each file.
 *
 * @param indexName Name of the index to create
 * @param firstSketch Sketch to be used for the index
 * @param moreSketches More sketches, if there are more than one sketch
 */
case class DataSkippingIndexConfig(
    override val indexName: String,
    firstSketch: Sketch,
    moreSketches: Sketch*)
    extends IndexConfigTrait {
  checkDuplicateSketches(sketches)

  /**
   * Returns all sketches this config has.
   */
  def sketches: Seq[Sketch] = firstSketch +: moreSketches

  /**
   * Returns the columns that the sketches reference.
   */
  override def referencedColumns: Seq[String] = sketches.flatMap(_.referencedColumns)

  override def createIndex(
      ctx: IndexerContext,
      sourceData: DataFrame,
      properties: Map[String, String]): (DataSkippingIndex, DataFrame) = {
    val resolvedSketches = ExpressionUtils.resolve(ctx.spark, sketches, sourceData)
    checkDuplicateSketches(resolvedSketches)
    val index = DataSkippingIndex(resolvedSketches, properties)
    (index, index.index(ctx, sourceData))
  }

  private def checkDuplicateSketches(sketches: Seq[Sketch]): Unit = {
    val uniqueSketches = mutable.Set[Sketch]()
    sketches.foreach { sketch =>
      if (uniqueSketches.contains(sketch)) {
        throw HyperspaceException(s"$sketch is specified multiple times.")
      }
      uniqueSketches.add(sketch)
    }
  }
}
