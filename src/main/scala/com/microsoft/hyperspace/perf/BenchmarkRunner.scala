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

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace._
import com.microsoft.hyperspace.index.{IndexConfig, IndexConstants}
import com.microsoft.hyperspace.util.FileUtils

object BenchmarkRunner {
  // scalastyle:off

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      throw new IllegalArgumentException(s"Path to benchmark configuration file not specified.")
    }

    val configs = BenchmarkConfig(args(0))

    val benchmarkName = configs.benchmarkName
    val dataDir = configs.dataPath
    val systemDir = configs.systemPath
    val createIx = configs.createIndex
    val runWorkload = configs.runWorkload
    val enableHyperspace = configs.enableHyperspace
    val format = configs.format
    val buckets = configs.buckets

    val spark = SparkSession
      .builder()
      .getOrCreate()

    // Change log level to clean up output
    spark.sparkContext.setLogLevel("ERROR")

    // Config overrides
    spark.conf.set("spark.sql.join.preferSortMergeJoin", "true")

    val appStartTime = getAppStartTime(spark)
    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, systemDir)

    if (createIx) {
      println(s"Creating indexes with $buckets buckets on data files in $format format ...")
      println(s"\tdata directory\t$dataDir")
      println(s"\tsystem directory\t$systemDir")

      val totalCreateIXStats = setupIndexes(spark, benchmarkName, dataDir, buckets, format)

      saveCreateIndexResults(
        spark,
        dataDir,
        systemDir,
        appStartTime,
        benchmarkName,
        format,
        buckets,
        totalCreateIXStats._2,
        totalCreateIXStats._1)

      // Get Index Sizes
      println("Collecting index sizes and printing sample records from each.")
      val indexNames =
        if (benchmarkName.equals("tpch")) TPCHBenchmark.recommendedIndexes.map(x => parseIndexConf(x).config.indexName)
        else TPCDSBenchmark.recommendedIndexes.map(x => parseIndexConf(x).config.indexName)

      var indexesSizeStats = Seq[Tuple2[String, Long]]()
      indexNames.foreach { x =>
        val indexDirPath = s"$systemDir/$x/v__=0"
        val size = FileUtils.getDirectorySize(new Path(indexDirPath))
        indexesSizeStats :+= (x, size)

        println(s" Sample record from index $x (total index size is $size):")
        spark.read.parquet(indexDirPath).show(2)
      }

      println("Index Size Summary:\n")
      indexesSizeStats.sortBy(_._1).foreach(xs => println(s"${xs._1}, ${xs._2}"))
    }

    if (runWorkload) {
      val iterations = configs.iterations
      val workload = configs.workload

      // Validate workload
      validateWorkload(benchmarkName, workload)

      println(s"\nRunning queries for $iterations iterations from workload $workload.")
      println(s"data directory\t$dataDir")
      println(s"system directory\t$systemDir")

      // Setup tables
      setupBaseTables(spark, benchmarkName, dataDir, format)

      // Run benchmark
      if (enableHyperspace) spark.enableHyperspace()
      val benchmarkQueries =
        if (benchmarkName.equals("tpch")) TPCHBenchmark.queries else TPCDSBenchmark.queries

      var stats = Seq[Tuple3[String, Long, Long]]()
      for (q <- workload) {
        val query = benchmarkQueries.filter(x => x.id == q).head
        for (i <- 1 to iterations) {
          val stat = runQuery(spark, query)
          stats :+= stat

          println(
            s"${stat._1} (Iteration $i, EnableHyperspace $enableHyperspace)\tExecution: ${stat._2} ms, Explain: ${stat._3} ms.")
        }
      }

      println("\nWorkload execution finished.")

      // show results on output
      prettyPrint(
        spark,
        appStartTime,
        benchmarkName,
        format,
        enableHyperspace,
        buckets,
        dataDir,
        systemDir,
        workload,
        stats)

      // save results in csv format
      saveRunBenchmarkResults(
        spark,
        dataDir,
        systemDir,
        appStartTime,
        benchmarkName,
        format,
        enableHyperspace,
        buckets,
        workload,
        stats)
    }

    // Stop session
    spark.stop()
  }

  private def runQuery(spark: SparkSession, query: Query): Tuple3[String, Long, Long] = {
    println(s"\n------- Running Query ${query.id} -------")
    try {
      val df = spark.sql(query.text)

      val explStartTime = System.currentTimeMillis()
      df.explain(true)
      val explEndTime = System.currentTimeMillis()

      val duration = {
        val execStartTime = System.currentTimeMillis()
        df.show
        val execEndTime = System.currentTimeMillis()
        execEndTime - execStartTime
      }

      (query.id, duration, explEndTime - explStartTime)
    } catch {
      case e: Exception =>
        println(s"Unexpected exception running query ${query.id}: ${e.getMessage}")
        (query.id, -1, -1)
    }
  }

  private def setupBaseTables(
      spark: SparkSession,
      benchmark: String,
      dataDir: String,
      format: String): Unit = {
    println(s"Creating base tables from format $format ...")
    val tables = if (benchmark.equals("tpch")) TPCHBenchmark.tables else TPCDSBenchmark.tables
    tables.foreach { table =>
      val baseTableData = s"$dataDir/${table._1}"
      val df = readBaseTableData(spark, baseTableData, format)
      df.createOrReplaceTempView(table._1)
      println(s"\tTable ${table._1} created from data under $baseTableData.")
    }
  }

  private def setupIndexes(
      spark: SparkSession,
      benchmark: String,
      dataDir: String,
      buckets: String,
      format: String): Tuple2[Long, Int] = {
    val hyperspace = Hyperspace()
    require(!spark.conf.get(IndexConstants.INDEX_SYSTEM_PATH).isEmpty)
    spark.conf.set(IndexConstants.INDEX_NUM_BUCKETS, buckets)

    val indexes =
      if (benchmark.equals("tpch")) TPCHBenchmark.recommendedIndexes
      else TPCDSBenchmark.recommendedIndexes
    val tables = if (benchmark.equals("tpch")) TPCHBenchmark.tables else TPCDSBenchmark.tables

    val indexCount = indexes.length
    val indexInfo = indexes.map(x => parseIndexConf(x))

    // Validate index configs
    indexInfo.foreach { x =>
      if (!validateIndexConfig(tables, x)) {
        throw new IllegalArgumentException(s"Invalid indexConfig ${x.config.indexName}.")
      }
    }

    // Validate index buckets config
    require(spark.conf.get(IndexConstants.INDEX_NUM_BUCKETS).equals(buckets))

    // Create indexes
    var totalTime = 0L
    indexInfo.foreach { x =>
      val baseTableData = s"$dataDir/${x.tableName}"
      totalTime += createIndex(spark, hyperspace, baseTableData, x, format)
    }

    hyperspace.indexes.show(100)
    (totalTime, indexCount)
  }

  private def validateIndexConfig(
      tables: Array[(String, StructType)],
      indexInfo: CoveringIndex): Boolean = {
    val baseTable = tables.filter(e => e._1 == indexInfo.tableName)
    if (baseTable.length != 1) { return false }

    val tableColumns = baseTable(0)._2.fieldNames
    val indexedColumns = indexInfo.config.indexedColumns
    val includedColumns = indexInfo.config.includedColumns
    indexedColumns.forall(tableColumns.contains) && includedColumns.forall(tableColumns.contains)
  }

  private def validateWorkload(benchmark: String, workload: Seq[String]): Unit = {
    val benchmarkQueries =
      if (benchmark.equals("tpch")) TPCHBenchmark.queries else TPCDSBenchmark.queries
    require(workload != null && benchmarkQueries.length > 0)

    workload.foreach { qid =>
      if (benchmarkQueries.filter(x => x.id == qid).isEmpty) {
        throw new IllegalArgumentException(s"Unknown query $qid in the workload.")
      }
    }
  }

  private def createIndex(
      spark: SparkSession,
      hyperspace: Hyperspace,
      baseTableData: String,
      indexInfo: CoveringIndex,
      format: String): Long = {
    val df = readBaseTableData(spark, baseTableData, format)
    println(
      s"\tCreating index ${indexInfo.config.indexName} on data files under $baseTableData ...")

    val startTime = System.currentTimeMillis()
    hyperspace.createIndex(df, indexInfo.config)
    val endTime = System.currentTimeMillis()

    endTime - startTime
  }

  private def parseIndexConf(content: String): CoveringIndex = {
    val tokens = content.split(";")
    require(tokens.size == 3 || tokens.size == 4)
    val indexedColumns = tokens(2).split(",")
    val includedColumns = if (tokens.size == 4) { tokens(3).split(",") } else { Array[String]() }
    CoveringIndex(tokens(1), IndexConfig(tokens(0), indexedColumns.toSeq, includedColumns.toSeq))
  }

  private def readBaseTableData(
      spark: SparkSession,
      baseTableData: String,
      format: String): DataFrame = {
    if (format.equals("parquet")) {
      spark.read.parquet(baseTableData)
    } else if (format.equals("csv")) {
      spark.read.option("header", "true").csv(baseTableData)
    } else {
      throw new IllegalArgumentException(s"Unknown format $format.")
    }
  }

  private def prettyPrint(
      spark: SparkSession,
      startTime: String,
      benchmarkName: String,
      format: String,
      enableHyperspace: Boolean,
      buckets: String,
      dataDir: String,
      systemDir: String,
      workload: Seq[String],
      stats: Seq[Tuple3[String, Long, Long]]): Unit = {
    val appId = spark.sparkContext.applicationId
    val appName = spark.sparkContext.appName
    val execNum = spark.conf.get("spark.executor.instances", "Not Found")
    val cores = spark.conf.get("spark.executor.cores", "Not Found")
    val memory = spark.conf.get("spark.executor.memory", "Not Found")
    val driverMemory = spark.conf.get("spark.driver.memory", "Not Found")
    val driverCores = spark.conf.get("spark.driver.cores", "Not Found")

    println(s"\n\n----- Summary -----")
    println(s"***** $appName *****\n")

    println(s"***** Query Execution Times *****\n")
    for (x <- workload) {
      print(s"$x\t")
      stats.filter(r => r._1 == x).foreach(t => print(s"\t${t._2}"))
      println()
    }

    println(s"\n\n***** Query Explain Times *****\n")
    for (x <- workload) {
      print(s"$x\t")
      stats.filter(r => r._1 == x).foreach(t => print(s"\t${t._3}"))
      println()
    }

    println(s"\napplication start time:\t$startTime")
    println(s"applicationId\t$appId")
    println(s"dataset\t$benchmarkName")
    println(s"format\t$format")
    println(s"enableHyperspace\t$enableHyperspace")
    println(s"buckets\t$buckets")
    println(s"requested executor number\t$execNum")
    println(s"requested executor cores\t$cores")
    println(s"requested executor memory\t$memory")
    println(s"driver memory\t$driverMemory")
    println(s"driver cores\t$driverCores")
    println(s"data directory\t$dataDir")
    println(s"system directory\t$systemDir")
    println(s"-------------------")
  }

  private def saveRunBenchmarkResults(
      spark: SparkSession,
      dataPath: String,
      systemPath: String,
      startTime: String,
      benchmarkName: String,
      format: String,
      enableHyperspace: Boolean,
      buckets: String,
      workload: Seq[String],
      stats: Seq[Tuple3[String, Long, Long]]): Unit = {
    val appId = spark.sparkContext.applicationId
    val execNum = spark.conf.get("spark.executor.instances", "Not Found")
    val cores = spark.conf.get("spark.executor.cores", "Not Found")
    val memory = spark.conf.get("spark.executor.memory", "Not Found")

    val prefix =
      s"$startTime,$appId,$dataPath,$systemPath,$benchmarkName,$format,$execNum,$cores,$memory,$enableHyperspace,$buckets"

    val header =
      "appTS,appId,dataPath,systemPath,dataset,format,execNum,execCore,execMem,enableHyperspace,indexBuckets,queryId,iteration,qExecTime,qExplainTime"
    var rows = List[String]()
    for (x <- workload) {
      val queryStats = stats.filter(r => r._1 == x)
      for (i <- queryStats.indices) {
        rows :+= s"$prefix,$x,$i,${queryStats(i)._2},${queryStats(i)._3}"
      }
    }

    println(s"Workload execution stats in the csv format:")
    println(s"\n$header")
    rows.foreach(println(_))
  }

  private def saveCreateIndexResults(
      spark: SparkSession,
      dataPath: String,
      systemPath: String,
      startTime: String,
      benchmarkName: String,
      format: String,
      buckets: String,
      ixCount: Int,
      totalTime: Long): Unit = {
    val appId = spark.sparkContext.applicationId
    val execNum = spark.conf.get("spark.executor.instances", "Not Found")
    val cores = spark.conf.get("spark.executor.cores", "Not Found")
    val memory = spark.conf.get("spark.executor.memory", "Not Found")

    val statToSave =
      s"$startTime,$appId,$dataPath,$systemPath,$benchmarkName,$format,$execNum,$cores,$memory,$buckets,$ixCount,$totalTime"

    val header =
      "appTS,appId,dataPath,systemPath,dataset,format,execNum,execCore,execMem,indexBuckets,indexCount,totalCreateIXTime"

    println(s"Create indexes stats in the csv format:")
    println(s"\n$header")
    println(statToSave)
  }

  private def getAppStartTime(spark: SparkSession): String = {
    val dateFormat = "yyyy-MM-dd HH:mm:ss"
    spark
      .range(1)
      .select(date_format(current_timestamp, dateFormat))
      .first()
      .mkString
  }

  // scalastyle:on
}

case class CoveringIndex(tableName: String, config: IndexConfig)

case class Query(id: String, text: String)
