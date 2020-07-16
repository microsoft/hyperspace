package com.microsoft.hyperspace

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

trait ActiveSparkSession {
  def spark: SparkSession = {
    SparkSession.getActiveSession.getOrElse {
      throw HyperspaceException("No spark session found")
    }
  }

  def sparkContext: SparkContext = spark.sparkContext
}
