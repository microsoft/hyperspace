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

package com.microsoft.hyperspace

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

import com.microsoft.hyperspace.telemetry.Constants.HYPERSPACE_EVENT_LOGGER_CLASS_KEY

trait SparkInvolvedSuite extends BeforeAndAfterAll with BeforeAndAfter {
  self: SparkFunSuite =>

  override def suiteName: String = getClass.getSimpleName

  val numParallelism: Int = 4

  protected lazy val spark: SparkSession = SparkSession
    .builder()
    .master(s"local[$numParallelism]")
    .config(HYPERSPACE_EVENT_LOGGER_CLASS_KEY, "com.microsoft.hyperspace.MockEventLogger")
    .config("delta.log.cacheSize", "3")
    .config("spark.databricks.delta.snapshotPartitions", "2")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.shuffle.partitions", "5")
    .config("spark.sql.sources.parallelPartitionDiscovery.parallelism", "5")
    .config("spark.ui.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
    .appName(suiteName)
    .getOrCreate()

  override def beforeAll(): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
    spark.sparkContext.setLogLevel("ERROR")
  }

  override def afterAll(): Unit = spark.stop()
}
