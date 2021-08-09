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

package com.microsoft.hyperspace.index.dataskipping.util

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

object DataFrameUtils {

  /**
   * Returns an estimated size of a cached dataframe in bytes.
   *
   * The dataframe should have been fully cached to get an accurate result.
   *
   * This method relies on the RDD of the dataframe. DataFrame.rdd is a lazy
   * val and doesn't change once set, but `cache()` creates new RDDs when a
   * dataframe is unpersisted and then cached again. Therefore, you shouldn't
   * call this function with a dataframe that has been unpersisted and cached
   * again.
   */
  def getSizeInBytes(df: DataFrame): Long = {
    def dependencies(rdd: RDD[_]): Seq[Int] = {
      rdd.id +: rdd.dependencies.flatMap(d => dependencies(d.rdd))
    }
    val deps = dependencies(df.rdd).toSet
    df.rdd.context.getRDDStorageInfo
      .filter(info => deps.contains(info.id))
      .map(info => info.memSize + info.diskSize)
      .sum
  }
}
