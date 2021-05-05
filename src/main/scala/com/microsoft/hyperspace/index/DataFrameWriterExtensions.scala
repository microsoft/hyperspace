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

import scala.collection.immutable.HashMap

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.DataSource

import com.microsoft.hyperspace.shim.SQLExecution

object DataFrameWriterExtensions {

  /**
   * Extend the DataFrameWriter class with support for bucketing without using saveAsTable().
   *
   * This extension is currently for Hyperspace internal use only. We could have made it more
   * general by making it a typed class and passing a "sortByColNames" parameter to the
   * saveWithBuckets() function we implement. However, currently there is no such need
   * since Hyperspace indexes have ensured that the "bucketBy" and "sortBy" columns be the same.
   *
   * @param dfWriter The DataFrameWriter.
   */
  implicit class Bucketizer(val dfWriter: DataFrameWriter[Row]) {

    /**
     * Save the DataFrame to the given path with specified bucketing properties.
     *
     * @param df The DataFrame to be saved.
     * @param path The path to save the DataFrame.
     * @param numBuckets The number of buckets.
     * @param bucketByColNames The "bucket-by" column names.
     */
    def saveWithBuckets(
        df: DataFrame,
        path: String,
        numBuckets: Int,
        bucketByColNames: Seq[String],
        mode: SaveMode): Unit = {
      require(numBuckets > 0, "The number of buckets must be a positive integer.")

      runCommand(df.sparkSession) {
        DataSource(
          sparkSession = df.sparkSession,
          className = "parquet",
          // In the current implementation of Hyperspace indexes, we have made sure that
          // the "bucketBy" and "sortBy" columns are the same.
          bucketSpec = Some(BucketSpec(numBuckets, bucketByColNames, bucketByColNames)),
          options = HashMap("path" -> path))
          .planForWriting(mode, df.queryExecution.analyzed)
      }
    }

    /**
     * Wrap a DataFrameWriter action to track the QueryExecution.
     *
     * This function is copied from the runCommand() function of DataFrameWriter, with simple
     * tweaks that remove code for dealing with user-provided query execution listeners.
     */
    private def runCommand(session: SparkSession)(command: LogicalPlan): Unit = {
      val qe = session.sessionState.executePlan(command)
      // Call `QueryExecution.toRDD` to trigger the execution of commands.
      SQLExecution.withNewExecutionId(session, qe)(qe.toRdd)
    }
  }
}
