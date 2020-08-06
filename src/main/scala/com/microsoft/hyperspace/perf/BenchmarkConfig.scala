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

package com.microsoft.hyperspace.perf

import java.util.Locale

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.io.Source

class BenchmarkConfig(configFile: String) {

  private val configsMap = readBenchmarkConfigs(configFile)

  def benchmarkName: String = {
    val benchmark = configsMap(BenchmarkConstants.BENCHMARK_NAME)
      .asInstanceOf[String]
      .trim()
      .toLowerCase()

    if (benchmark.equals("tpch") || benchmark.equals("tpcds")) {
      benchmark
    } else {
      throw new IllegalArgumentException(s"Unknown benchmark $benchmark.")
    }
  }

  def iterations: Int = configsMap.getOrElse(BenchmarkConstants.ITERATIONS, 1).asInstanceOf[Int]

  def dataPath: String =
    configsMap(BenchmarkConstants.DATA_DIRECTORY).asInstanceOf[String]

  def systemPath: String =
    configsMap(BenchmarkConstants.SYSTEM_DIRECTORY).asInstanceOf[String]

  def enableHyperspace: Boolean =
    configsMap.getOrElse(BenchmarkConstants.ENABLE_HYPERSPACE, false).asInstanceOf[Boolean]

  def createIndex: Boolean =
    configsMap.getOrElse(BenchmarkConstants.CREATE_INDEX, false).asInstanceOf[Boolean]

  def runWorkload: Boolean =
    configsMap.getOrElse(BenchmarkConstants.RUN_WORKLOAD, false).asInstanceOf[Boolean]

  def workload: Seq[String] = configsMap(BenchmarkConstants.WORKLOAD).asInstanceOf[Seq[String]]

  def format: String = {
    val format =
      configsMap
        .getOrElse(BenchmarkConstants.FORMAT, "")
        .asInstanceOf[String]
        .trim
        .toLowerCase(Locale.ROOT)
    if (format.equals("")) {
      "parquet"
    } else if (format.equals("parquet") || format.equals("csv")) {
      format
    } else {
      throw new IllegalArgumentException(s"Unknown format $format.")
    }
  }

  def buckets: String =
    configsMap
      .getOrElse(BenchmarkConstants.INDEX_BUCKETS, BenchmarkConstants.DEFAULT_BUCKETS)
      .asInstanceOf[String]

  private def readBenchmarkConfigs(filePath: String) = {
    val src = Source.fromFile(filePath)
    val configs = src.mkString
    val mapper = new ObjectMapper
    mapper.registerModule(DefaultScalaModule)
    val configsMap = mapper.readValue(configs, classOf[Map[String, Any]])
    src.close()
    configsMap
  }
}

object BenchmarkConfig {
  def apply(configFile: String): BenchmarkConfig = new BenchmarkConfig(configFile)
}
