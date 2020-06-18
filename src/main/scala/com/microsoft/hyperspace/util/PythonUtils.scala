package com.microsoft.hyperspace.util

import org.apache.spark.sql.SparkSession

import com.microsoft.hyperspace.Implicits

/**
 * Provides helper methods for python APIs.
 */
private[hyperspace] object PythonUtils {

  /**
   * Wrapper for Implicits.enableHyperspace to be called from the Python API.
   *
   * @param spark SparkSession
   * @return a spark session that contains Hyperspace-specific rules.
   */
  def enableHyperspace(spark: SparkSession): SparkSession = {
    spark.enableHyperspace()
  }

  /**
   * Wrapper for Implicits.disableHyperspace to be called from the Python API.
   *
   * @param spark SparkSession
   * @return a spark session that does not contain Hyperspace-specific rules.
   */
  def disableHyperspace(spark: SparkSession): SparkSession = {
    spark.disableHyperspace()
  }

  /**
   * Wrapper for Implicits.isHyperspaceEnabled to be called from the Python API.
   *
   * @param spark SparkSession
   * @return true if Hyperspace is enabled or false otherwise.
   */
  def isHyperspaceEnabled(spark: SparkSession): Boolean = {
    spark.isHyperspaceEnabled()
  }
}
