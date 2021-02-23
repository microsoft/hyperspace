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

package com.microsoft.hyperspace.index

import org.apache.hadoop.yarn.util.SystemClock
import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.HyperspaceException

object IndexCacheType {
  val CREATION_TIME_BASED = "CREATION_TIME_BASED"
}

trait IndexCacheFactory {
  def create(spark: SparkSession, cacheType: String): Cache[Seq[IndexLogEntry]]
}

object IndexCacheFactoryImpl extends IndexCacheFactory {
  override def create(spark: SparkSession, cacheType: String): Cache[Seq[IndexLogEntry]] = {
    cacheType match {
      case IndexCacheType.CREATION_TIME_BASED =>
        new CreationTimeBasedIndexCache(spark, new SystemClock)
      case _ => throw HyperspaceException(s"Unknown cache type: $cacheType.")
    }
  }
}
