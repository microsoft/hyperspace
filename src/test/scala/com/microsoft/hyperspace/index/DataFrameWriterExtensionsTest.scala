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

import java.io.File

import scala.collection.mutable.ListBuffer

import org.apache.hadoop.fs.Path
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.apache.spark.sql.catalyst.expressions.UnsafeProjection
import org.apache.spark.sql.catalyst.plans.physical.HashPartitioning
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.functions._

import com.microsoft.hyperspace.{SampleData, SparkInvolvedSuite}
import com.microsoft.hyperspace.index.DataFrameWriterExtensions.Bucketizer
import com.microsoft.hyperspace.util.FileUtils

class DataFrameWriterExtensionsTest extends HyperspaceSuite {

  private val sampleData = SampleData.testData
  private val sampleParquetDataLocation = inTempDir("sampleparquet")
  private val sampleDataBucketedLocation = inTempDir("sampleparquet-withBuckets")

  private var df: DataFrame = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val sparkSession = spark
    import sparkSession.implicits._

    FileUtils.delete(new Path(sampleParquetDataLocation))
    FileUtils.delete(new Path(sampleDataBucketedLocation))

    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    dfFromSample.write.parquet(sampleParquetDataLocation)

    df = spark.read.parquet(sampleParquetDataLocation)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(sampleParquetDataLocation))
    super.afterAll()
  }

  after {
    FileUtils.delete(new Path(sampleDataBucketedLocation))
  }

  /**
   * An internal wrapper method for testing bucketing.
   */
  private def testInternal(
      numBuckets: Int,
      bucketByCols: Seq[String],
      mode: SaveMode = SaveMode.Overwrite): Unit = {
    df.write.saveWithBuckets(df, sampleDataBucketedLocation, numBuckets, bucketByCols, mode)

    testBucketing(
      new File(sampleDataBucketedLocation),
      "parquet",
      numBuckets,
      bucketByCols,
      bucketByCols,
      mode)
  }

  /**
   * A helper method to check the bucket write functionality in low level, i.e. check the written
   * bucket files to see if the data are correct. User should pass in a data dir that these bucket
   * files are written to, and the format of data(parquet, json, etc.), and the bucketing
   * information.
   *
   * This method is copied from BucketWriteSuite with minor changes.
   */
  private def testBucketing(
      dataDir: File,
      source: String,
      numBuckets: Int,
      bucketCols: Seq[String],
      sortCols: Seq[String],
      mode: SaveMode): Unit = {
    val allBucketFiles =
      dataDir.listFiles().filterNot(f => f.getName.startsWith(".") || f.getName.startsWith("_"))

    val allRowsInBucketFiles = new ListBuffer[Row]
    for (bucketFile <- allBucketFiles) {
      val bucketId = BucketingUtils.getBucketId(bucketFile.getName).getOrElse {
        fail(s"Unable to find the related bucket files.")
      }

      // Remove the duplicate columns in bucketCols and sortCols;
      // Otherwise, we got analysis errors due to duplicate names
      val selectedColumns = (bucketCols ++ sortCols).distinct
      // We may lose the type information after write(e.g. json format doesn't keep schema
      // information), here we get the types from the original dataframe.
      val types = df.select(selectedColumns.map(col): _*).schema.map(_.dataType)
      val columns = selectedColumns.zip(types).map {
        case (colName, dt) => col(colName).cast(dt)
      }

      // Read the bucket file into a dataframe, so that it's easier to test.
      val readBackAllCols = spark.read
        .format(source)
        .load(bucketFile.getAbsolutePath)
      allRowsInBucketFiles ++= readBackAllCols.collect().toSeq

      // Project to only "bucketBy" and "sortBy" columns.
      val readBack = readBackAllCols.select(columns: _*)

      // If we specified sort columns while writing bucket table, make sure the data in this
      // bucket file is already sorted.
      if (sortCols.nonEmpty) {
        assert(readBack.sort(sortCols.map(col): _*).collect().toSeq == readBack.collect().toSeq)
      }

      // Go through all rows in this bucket file, calculate bucket id according to bucket column
      // values, and make sure it equals to the expected bucket id that inferred from file name.
      val qe = readBack.select(bucketCols.map(col): _*).queryExecution
      val rows = qe.toRdd.map(_.copy()).collect()
      val getBucketId = UnsafeProjection.create(
        HashPartitioning(qe.analyzed.output, numBuckets).partitionIdExpression :: Nil,
        qe.analyzed.output)

      for (row <- rows) {
        val actualBucketId = getBucketId(row).getInt(0)
        assert(actualBucketId == bucketId)
      }
    }

    // Check that the rows read back from all bucket files match the rows in the data frame.
    val allRowsInDataFrame = df.collect().toSeq
    if (mode.equals(SaveMode.Overwrite)) {
      assert(allRowsInBucketFiles.length == allRowsInDataFrame.length)
    } else if (mode.equals(SaveMode.Append)) {
      assert(allRowsInBucketFiles.length == allRowsInDataFrame.length * 2)
    }
    assert(
      allRowsInBucketFiles.forall(allRowsInDataFrame.contains(_)) && allRowsInDataFrame
        .forall(allRowsInBucketFiles.contains(_)))
  }

  test("test of saveWithBuckets using a single bucket column") {
    val numBuckets = 3
    val bucketByCols = Seq("Query")

    testInternal(numBuckets, bucketByCols)
  }

  test("test of saveWithBuckets using multiple bucketing columns") {
    val numBuckets = 3
    val bucketByCols = Seq("clicks", "Query")

    testInternal(numBuckets, bucketByCols)
  }

  test("test of saveWithBuckets using Append mode") {
    val numBuckets = 3
    val bucketByCols = Seq("clicks", "Query")

    testInternal(numBuckets, bucketByCols)
    testInternal(numBuckets, bucketByCols, SaveMode.Append)
  }
}
