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

import org.apache.hadoop.yarn.util.Clock
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * An IndexCollectionManager which leverages a cache to
 * accelerate fetching indexes, with below properties:
 * - Cache entry is only set/used on the read path (i.e. getIndexes() API).
 * - If cache entry is stale, indexes are fetched directly from the
 * system path (as done in the parent class) and cache entry will
 * be refreshed.
 * - If any API is called to add a new index or modify
 * an existing index's status, cache gets cleared.
 *
 * @param spark Spark session
 * @param indexCacheFactory provides cache instance
 * @param indexLogManagerFactory provides IndexLogManager instance
 * @param indexDataManagerFactory provides IndexDataManager instance
 * @param fileSystemFactory provides FileSystem instance
 */
class CachingIndexCollectionManager(
    spark: SparkSession,
    indexCacheFactory: IndexCacheFactory,
    indexLogManagerFactory: IndexLogManagerFactory,
    indexDataManagerFactory: IndexDataManagerFactory,
    fileSystemFactory: FileSystemFactory)
    extends IndexCollectionManager(
      spark,
      indexLogManagerFactory,
      indexDataManagerFactory,
      fileSystemFactory) {

  private val indexCache: Cache[Seq[IndexLogEntry]] =
    indexCacheFactory.create(spark, IndexCacheType.CREATION_TIME_BASED)

  /**
   * This API tries to use the cache to fetch a list of indexes.
   * If cache entry is stale, it reverts to parent class to fetch indexes
   * from the system path and refreshes cache entry for later calls.
   *
   * @param states list of index states of interest
   * @return list of indexes whose current state is among desired states
   */
  override def getIndexes(states: Seq[String] = Seq()): Seq[IndexLogEntry] = {
    indexCache
      .get()
      .getOrElse {
        val originalIndexes = super.getIndexes()
        indexCache.set(originalIndexes)
        originalIndexes
      }
      .filter(index => states.isEmpty || states.contains(index.state))
  }

  /**
   * Clear current cache entry.
   */
  def clearCache(): Unit = {
    indexCache.clear()
  }

  override def create(df: DataFrame, indexConfig: IndexConfigTrait): Unit = {
    clearCache()
    super.create(df, indexConfig)
  }

  override def delete(indexName: String): Unit = {
    clearCache()
    super.delete(indexName)
  }

  override def restore(indexName: String): Unit = {
    clearCache()
    super.restore(indexName)
  }

  override def vacuum(indexName: String): Unit = {
    clearCache()
    super.vacuum(indexName)
  }

  override def refresh(indexName: String, mode: String): Unit = {
    clearCache()
    super.refresh(indexName, mode)
  }

  override def optimize(indexName: String, mode: String): Unit = {
    clearCache()
    super.optimize(indexName, mode)
  }
}

object CachingIndexCollectionManager {
  def apply(spark: SparkSession): IndexCollectionManager =
    new CachingIndexCollectionManager(
      spark,
      IndexCacheFactoryImpl,
      IndexLogManagerFactoryImpl,
      IndexDataManagerFactoryImpl,
      FileSystemFactoryImpl)
}

/**
 * An implementation of cache to store indexes that uses entry's last reset time to check
 * validity of cache entry.
 * Cache entry is stale if it has been in the cache for some (configurable) time.
 *
 * @param spark Spark session
 */
class CreationTimeBasedIndexCache(spark: SparkSession, clock: Clock)
    extends Cache[Seq[IndexLogEntry]] {

  private var entries: Seq[IndexLogEntry] = Seq[IndexLogEntry]()
  private var lastCacheTime: Long = 0L

  /**
   * Returns cache entry with the following expiration policy:
   *  If lastCacheTime shows a valid time and the duration since then does not exceed
   *  configured expiry duration, then current cache entry is valid; otherwise stale
   *
   * @return current cache entry if not expired, otherwise None.
   */
  override def get(): Option[Seq[IndexLogEntry]] = {
    if (lastCacheTime > 0) {
      val currentTime = clock.getTime
      val expiryDurationInSec = spark.sessionState.conf
        .getConfString(
          IndexConstants.INDEX_CACHE_EXPIRY_DURATION_SECONDS,
          IndexConstants.INDEX_CACHE_EXPIRY_DURATION_SECONDS_DEFAULT)
        .toLong

      if (currentTime < lastCacheTime + (expiryDurationInSec * 1000L)) {
        return Some(entries)
      }
    }

    None
  }

  /**
   * Reset cache content with new entry and reset cache time
   *
   * @param entry new entry to cache
   */
  override def set(entry: Seq[IndexLogEntry]): Unit = {
    entries = entry
    lastCacheTime = clock.getTime
  }

  /**
   * Clear current cache entry
   */
  override def clear(): Unit = {
    lastCacheTime = 0L
  }
}
