/*
 * Copyright (2021) The Hyperspace Project Authors.
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

package com.microsoft.hyperspace.index.dataskipping

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.hyperspace.RDDTestUtils
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{input_file_name, max, min}
import org.apache.spark.sql.types.IntegerType

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.{Content, FileInfo, Index, IndexConstants}
import com.microsoft.hyperspace.index.dataskipping.sketch.MinMaxSketch
import com.microsoft.hyperspace.util.JsonUtils

class DataSkippingIndexTest extends DataSkippingSuite {
  override val numParallelism: Int = 3

  test("""kind returns "DataSkippingIndex".""") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A")))
    assert(index.kind === "DataSkippingIndex")
  }

  test("""kindAbbr returns "DS".""") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A")))
    assert(index.kindAbbr === "DS")
  }

  test("indexedColumns returns indexed columns of sketches.") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A"), MinMaxSketch("B")))
    assert(index.indexedColumns === Seq("A", "B"))
  }

  test("referencedColumns returns indexed columns of sketches.") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A"), MinMaxSketch("B")))
    assert(index.referencedColumns === Seq("A", "B"))
  }

  test(
    "withNewProperties returns a new index which copies the original index except the " +
      "properties.") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A")))
    val newIndex = index.withNewProperties(Map("foo" -> "bar"))
    assert(newIndex.properties === Map("foo" -> "bar"))
    assert(newIndex.sketches === index.sketches)
  }

  test("statistics returns a string-formatted list of sketches.") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A"), MinMaxSketch("B")))
    assert(index.statistics() === Map("sketches" -> "MinMax(A), MinMax(B)"))
  }

  test("canHandleDeletedFiles returns true.") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A")))
    assert(index.canHandleDeletedFiles === true)
  }

  test("write writes the index data in a Parquet format.") {
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    index.write(ctx, indexData)
    val writtenIndexData = spark.read.parquet(indexDataPath.toString)
    checkAnswer(writtenIndexData, indexData)
  }

  test(
    "optimize reduces the number of index data files to 1 " +
      "if minimum records per index data file is 10000.") {
    testOptimize(10000, 1)
  }

  test(
    "optimize reduces the number of index data files to 3 " +
      "if minimum records per index data file is 400.") {
    testOptimize(400, 3)
  }

  def testOptimize(targetIndexDataFileSize: Long, expectedNumIndexDataFiles: Long): Unit = {
    withSQLConf(
      IndexConstants.DATASKIPPING_TARGET_INDEX_DATA_FILE_SIZE ->
        targetIndexDataFileSize.toString) {
      val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
      val sourceData = createSourceData(spark.range(100).toDF("A"))
      val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
      index.write(ctx, indexData)

      // Create more index data files by refreshing the index incrementally.
      val iterations = 5
      val indexDataPaths = (1 until iterations).map { i =>
        val appendedSourceData = createSourceData(
          spark.range(i * 100, (i + 1) * 100).toDF("A"),
          saveMode = SaveMode.Append,
          appendedDataOnly = true)
        val newIndexDataPath = new Path(inTempDir(s"Index$i"))
        indexDataPathVar = newIndexDataPath
        index.refreshIncremental(ctx, Some(appendedSourceData), Nil, emptyContent)
        newIndexDataPath
      }

      // During refresh, index data files are put in different paths.
      val indexDataFiles = listFiles(indexDataPaths: _*)
      indexDataPathVar = new Path(inTempDir(s"Index$iterations"))
      index.optimize(ctx, indexDataFiles.map(f => FileInfo(f, fileIdTracker.addFile(f), true)))

      val optimizedIndexDataFiles = listFiles(indexDataPathVar).filter(isParquet)
      assert(optimizedIndexDataFiles.length === expectedNumIndexDataFiles)
    }
  }

  test("write throws an exception if target index data file size is too small.") {
    withSQLConf(IndexConstants.DATASKIPPING_TARGET_INDEX_DATA_FILE_SIZE -> "1") {
      val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
      val sourceData = createSourceData(spark.range(100).toDF("A"))
      val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
      val mockIndexData = RDDTestUtils.getMockDataFrameWithFakeSize(spark, 4000000000L)
      val ex = intercept[HyperspaceException](index.write(ctx, mockIndexData))
      assert(
        ex.getMessage.contains("Could not create index data files due to too many files: " +
          "indexDataSize=4000000000, targetIndexDataFileSize=1"))
    }
  }

  test("refreshIncremental works correctly for appended data.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    index.write(ctx, indexData)

    val appendedSourceData = createSourceData(
      spark.range(100, 200).toDF("A"),
      saveMode = SaveMode.Append,
      appendedDataOnly = true)

    val indexDataPath2 = new Path(inTempDir("Index2"))
    indexDataPathVar = indexDataPath2
    val (newIndex, updateMode) =
      index.refreshIncremental(ctx, Some(appendedSourceData), Nil, emptyContent)
    assert(newIndex === index)
    assert(updateMode === Index.UpdateMode.Merge)

    val updatedIndexData = spark.read.parquet(indexDataPath.toString, indexDataPath2.toString)
    val expectedSketchValues = sourceData
      .union(appendedSourceData)
      .groupBy(input_file_name().as(fileNameCol))
      .agg(min("A"), max("A"))
    checkAnswer(updatedIndexData, withFileId(expectedSketchValues))
  }

  test("refreshIncremental works correctly for deleted data.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    index.write(ctx, indexData)

    val deletedFile = listFiles(dataPath()).filter(isParquet).head
    deleteFile(deletedFile.getPath)

    val indexDataPath2 = new Path(inTempDir("Index2"))
    indexDataPathVar = indexDataPath2
    val (newIndex, updateMode) =
      index.refreshIncremental(
        ctx,
        None,
        Seq(FileInfo(deletedFile, fileIdTracker.addFile(deletedFile), true)),
        Content.fromDirectory(indexDataPath, fileIdTracker, new Configuration))
    assert(newIndex === index)
    assert(updateMode === Index.UpdateMode.Overwrite)

    val updatedIndexData = spark.read.parquet(indexDataPath2.toString)
    val expectedSketchValues = spark.read
      .parquet(dataPath().toString)
      .union(spark.read.parquet(dataPath().toString))
      .groupBy(input_file_name().as(fileNameCol))
      .agg(min("A"), max("A"))
    checkAnswer(updatedIndexData, withFileId(expectedSketchValues))
  }

  test("refreshIncremental works correctly for appended and deleted data.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    index.write(ctx, indexData)

    val deletedFile = listFiles(dataPath()).filter(isParquet).head
    deleteFile(deletedFile.getPath)
    val appendedSourceData = createSourceData(
      spark.range(100, 200).toDF("A"),
      saveMode = SaveMode.Append,
      appendedDataOnly = true)

    val indexDataPath2 = new Path(inTempDir("Index2"))
    indexDataPathVar = indexDataPath2
    val (newIndex, updateMode) =
      index.refreshIncremental(
        ctx,
        Some(appendedSourceData),
        Seq(FileInfo(deletedFile, fileIdTracker.addFile(deletedFile), true)),
        Content.fromDirectory(indexDataPath, fileIdTracker, new Configuration))
    assert(newIndex === index)
    assert(updateMode === Index.UpdateMode.Overwrite)

    val updatedIndexData = spark.read.parquet(indexDataPath2.toString)
    val expectedSketchValues = spark.read
      .parquet(dataPath().toString)
      .union(spark.read.parquet(dataPath().toString))
      .groupBy(input_file_name().as(fileNameCol))
      .agg(min("A"), max("A"))
    checkAnswer(updatedIndexData, withFileId(expectedSketchValues))
  }

  test("refreshFull works correctly for fully overwritten data.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    index.write(ctx, indexData)

    val newSourceData = createSourceData(spark.range(200).toDF("A"))

    val (newIndex, newIndexData) = index.refreshFull(ctx, newSourceData)
    assert(newIndex === index)

    val expectedSketchValues = newSourceData
      .groupBy(input_file_name().as(fileNameCol))
      .agg(min("A"), max("A"))
    checkAnswer(newIndexData, withFileId(expectedSketchValues))
  }

  test("At least one sketch must be specified.") {
    val ex = intercept[AssertionError](DataSkippingIndex(Nil))
    assert(ex.getMessage().contains("At least one sketch is required"))
  }

  test("Indexes are equal if they have the same sketches and data types.") {
    val ds1 = DataSkippingIndex(Seq(MinMaxSketch("A"), MinMaxSketch("B")))
    val ds2 = DataSkippingIndex(Seq(MinMaxSketch("B"), MinMaxSketch("A")))
    assert(ds1 === ds2)
    assert(ds1.hashCode === ds2.hashCode)
  }

  test("Indexes are not equal to objects which are not indexes.") {
    val ds = DataSkippingIndex(Seq(MinMaxSketch("A")))
    assert(ds !== "ds")
  }

  test("Index can be serialized.") {
    val ds = DataSkippingIndex(Seq(MinMaxSketch("A", Some(IntegerType))), Map("a" -> "b"))
    val json = JsonUtils.toJson(ds)
    assert(
      json ===
        """|{
           |  "type" : "com.microsoft.hyperspace.index.dataskipping.DataSkippingIndex",
           |  "sketches" : [ {
           |    "type" : "com.microsoft.hyperspace.index.dataskipping.sketch.MinMaxSketch",
           |    "expr" : "A",
           |    "dataType" : "integer"
           |  } ],
           |  "properties" : {
           |    "a" : "b"
           |  }
           |}""".stripMargin)
  }

  test("Index can be deserialized.") {
    val json =
      """|{
         |  "type" : "com.microsoft.hyperspace.index.dataskipping.DataSkippingIndex",
         |  "sketches" : [ {
         |    "type" : "com.microsoft.hyperspace.index.dataskipping.sketch.MinMaxSketch",
         |    "expr" : "A",
         |    "dataType" : "integer"
         |  } ],
         |  "properties" : {
         |    "a" : "b"
         |  }
         |}""".stripMargin
    val ds = JsonUtils.fromJson[DataSkippingIndex](json)
    assert(ds === DataSkippingIndex(Seq(MinMaxSketch("A", Some(IntegerType)))))
    assert(ds.properties === Map("a" -> "b"))
  }
}
