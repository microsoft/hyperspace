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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.util.Clock
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.microsoft.hyperspace.{Hyperspace, HyperspaceException, SampleData}
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.covering.CoveringIndex
import com.microsoft.hyperspace.util.FileUtils

class IndexCacheTest extends HyperspaceSuite {
  val sampleParquetDataLocation = inTempDir("sampleparquet")
  val indexConfig1 = IndexConfig("index1", Seq("RGUID"), Seq("Date"))

  before {
    FileUtils.delete(new Path(sampleParquetDataLocation))
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(sampleParquetDataLocation))
    super.afterAll()
  }

  def testIndex(schema: StructType, indexDir: String): IndexLogEntry = {
    val sourcePlanProperties = SparkPlan.Properties(
      Seq(),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(Seq(Signature("signatureProvider", "dfSignature")))))

    val entry = IndexLogEntry(
      "index1",
      CoveringIndex(Seq("RGUID"), Seq("Date"), schema, 10, Map()),
      Content(Directory(indexDir)),
      Source(SparkPlan(sourcePlanProperties)),
      Map())
    entry.state = Constants.States.ACTIVE
    entry
  }

  val index1 = testIndex(
    StructType(Seq(StructField("RGUID", StringType), StructField("Date", StringType))),
    "path1")
  val index2 = testIndex(
    StructType(Seq(StructField("Date", StringType), StructField("RGUID", StringType))),
    "plan")

  test("Verify CreationTimeBasedIndexCache APIs.") {
    val indexCache = new CreationTimeBasedIndexCache(spark, new MockClock(1))

    val initialEntry = indexCache.get()
    assert(initialEntry.isEmpty, "Cache initially has to be empty.")

    val entryToSet = Seq(index1, index2)
    indexCache.set(entryToSet)
    val entry = indexCache.get()
    assert(entry.isDefined)
    assert(entry.get.equals(entryToSet), "Cache entry does not match with what was set in it.")

    indexCache.clear()
    assert(indexCache.get().isEmpty, "Cache Clear API should remove entry.")
  }

  test("verify CreationTimeBasedIndexCache expiry configuration.") {
    spark.conf.set(IndexConstants.INDEX_CACHE_EXPIRY_DURATION_SECONDS, "6000")
    val clock = new MockClock(1)
    val indexCache = new CreationTimeBasedIndexCache(spark, clock)

    indexCache.set(Seq(index1))
    assert(indexCache.get().isDefined)

    // Change cache expiry configuration and wait enough so cache entry expires.
    spark.conf.set(IndexConstants.INDEX_CACHE_EXPIRY_DURATION_SECONDS, "1")

    clock.advanceTime(1000)

    assert(indexCache.get().isEmpty, "Cache entry should have been expired.")
  }

  test("verify CachingIndexCollectionManager APIs thru Cache.") {

    val mockIndexCacheFactory = new MockIndexCacheFactoryImpl(spark)
    val entry = Seq(index1)
    mockIndexCacheFactory.indexCache.set(entry)

    val indexCollectionManager = new CachingIndexCollectionManager(
      spark,
      mockIndexCacheFactory,
      IndexLogManagerFactoryImpl,
      IndexDataManagerFactoryImpl,
      FileSystemFactoryImpl)

    assert(indexCollectionManager.getIndexes().equals(entry))

    indexCollectionManager.clearCache()
    assert(mockIndexCacheFactory.indexCache.get().isEmpty)
  }

  test("verify CachingIndexCollectionManager API updates cache.") {
    // Create sample index. This index is used to set cache entry and validate it.
    import spark.implicits._
    val sampleData = SampleData.testData
    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    dfFromSample.write.parquet(sampleParquetDataLocation)

    val df = spark.read.parquet(sampleParquetDataLocation)

    val hyperspace = new Hyperspace(spark)
    hyperspace.createIndex(df, indexConfig1)

    val mockIndexCacheFactory = new MockIndexCacheFactoryImpl(spark)

    val indexCollectionManager = new CachingIndexCollectionManager(
      spark,
      mockIndexCacheFactory,
      IndexLogManagerFactoryImpl,
      IndexDataManagerFactoryImpl,
      FileSystemFactoryImpl)

    assert(mockIndexCacheFactory.indexCache.get().isEmpty)
    val indexes = indexCollectionManager.getIndexes()
    assert(indexes.length == 1)

    val entry = mockIndexCacheFactory.indexCache.get()
    assert(entry.isDefined)
    assert(entry.get.equals(indexes), "Cache entry does not match with the created index.")
  }
}

/**
 * Mock for testing purposes so we can validate and invalidate entries based on time.
 *
 * @param time Current time.
 */
class MockClock(private var time: Long = 0L) extends Clock {
  override def getTime: Long = time

  def advanceTime(value: Long): Unit = {
    time = time + value
  }
}

/**
 * Mock IndexCacheFactory so test code can directly access single index cache instance and
 * validate/modify cache entry in different scenarios.
 *
 * @param spark Spark session
 */
class MockIndexCacheFactoryImpl(spark: SparkSession) extends IndexCacheFactory {
  val indexCache = new CreationTimeBasedIndexCache(spark, new MockClock(1))

  override def create(spark: SparkSession, cacheType: String): Cache[Seq[IndexLogEntry]] = {
    cacheType match {
      case IndexCacheType.CREATION_TIME_BASED => indexCache
      case _ => throw HyperspaceException(s"Unknown cache type: $cacheType.")
    }
  }
}
