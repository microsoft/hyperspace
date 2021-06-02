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

package com.microsoft.hyperspace.index.types.dataskipping

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.{input_file_name, max, min}

import com.microsoft.hyperspace.index.{FileInfo, Index}
import com.microsoft.hyperspace.index.types.dataskipping.sketch.{MinMaxSketch, ValueListSketch}

class DataSkippingIndexTest extends DataSkippingSuite {
  test("sketchOffsets is computed correctly for a single sketch.") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A")))
    assert(index.sketchOffsets === Seq(0, 2))
  }

  test("sketchOffsets is computed correctly for two sketches.") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A"), ValueListSketch("B")))
    assert(index.sketchOffsets === Seq(0, 2, 3))
  }

  test("""kind returns "DataSkippingIndex".""") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A")))
    assert(index.kind === "DataSkippingIndex")
  }

  test("""kindAbbr returns "DS".""") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A")))
    assert(index.kindAbbr === "DS")
  }

  test("indexedColumns returns indexed columns of sketches.") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A"), ValueListSketch("B")))
    assert(index.indexedColumns === Seq("A", "B"))
  }

  test("indexedColumns may return same columns multiple times.") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A"), ValueListSketch("A")))
    assert(index.indexedColumns === Seq("A", "A"))
  }

  test("referencedColumns returns indexed columns of sketches.") {
    val index = DataSkippingIndex(Seq(MinMaxSketch("A"), ValueListSketch("B")))
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
    val index = DataSkippingIndex(Seq(MinMaxSketch("A"), ValueListSketch("B")))
    assert(index.statistics() === Map("sketches" -> "MinMax(A), ValueList(B)"))
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

  test("optimize reduces the number of index data files.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    index.write(ctx, indexData)

    // Create more index data files by refreshing the index incrementally.
    val iterations = 10
    val indexDataPaths = (1 until iterations).map { i =>
      val appendedSourceData =
        createSourceData(spark.range(i * 100, (i + 1) * 100).toDF("A"), SaveMode.Append)
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
    assert(optimizedIndexDataFiles.length < indexDataFiles.length)
  }

  test("refreshIncremental") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    index.write(ctx, indexData)

    // Index data files for the appended data is put in indexDataPath2.
    val appendedSourceData =
      createSourceData(spark.range(100, 200).toDF("A"), SaveMode.Append)
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

  test("refreshFull") {
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

  test("normalizeColumnName removes invalid characters for Parquet.") {
    assert(DataSkippingIndex.normalizeColumnName("A(B)") === "A_B_")
    assert(DataSkippingIndex.normalizeColumnName("x y,;{}(\n\t=)") === "x_y_________")
  }
}
