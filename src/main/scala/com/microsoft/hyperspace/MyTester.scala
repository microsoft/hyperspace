package com.microsoft.hyperspace

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace._
import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.{IndexConfig, IndexConstants}
import com.microsoft.hyperspace.util.FileUtils

object MyTester {
  // scalastyle:off
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val systemPath = "C:\\Users\\pouriap\\Desktop\\scratch\\relTest\\ix"
    FileUtils.delete(new Path(systemPath))

    val sparkConf = new SparkConf().setMaster("local[*]")
    val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    // read data
    val dataPath = "C:\\Users\\pouriap\\Desktop\\scratch\\relTest\\data\\nation"
    val df = spark.read.parquet(dataPath)

    // create index
    spark.conf.set(IndexConstants.INDEX_SYSTEM_PATH, systemPath)
    spark.conf.set(IndexConstants.INDEX_LINEAGE_ENABLED, "true")

    val hyperspace: Hyperspace = Hyperspace()
    val indexConfig = IndexConfig("nix", Seq("n_nationkey"), Seq("n_name", "n_regionkey"))
    hyperspace.createIndex(df, indexConfig)
    println("Indexes created.")

    // query
    spark.enableHyperspace
    val qdf = spark.read.parquet(dataPath)
      .filter("n_nationkey > 12")
      .select("n_name", "n_nationkey")
    qdf.explain(true)
    qdf.show()

    // read index content
    spark.read.parquet("C:\\Users\\pouriap\\Desktop\\scratch\\relTest\\ix\\nix\\v__=0")
      .groupBy(IndexConstants.DATA_FILE_NAME_COLUMN)
      .count()
//      .select(IndexConstants.DATA_FILE_NAME_COLUMN)
//      .distinct()
      .show(30, false)
  }
}
