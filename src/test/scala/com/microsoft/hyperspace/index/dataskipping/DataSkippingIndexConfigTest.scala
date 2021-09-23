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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions.{input_file_name, max, min}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.util.sketch.BloomFilter

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.IndexConstants
import com.microsoft.hyperspace.index.dataskipping.sketches._

class DataSkippingIndexConfigTest extends DataSkippingSuite with BloomFilterTestUtils {
  test("indexName returns the index name.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    assert(indexConfig.indexName === "myIndex")
  }

  test("sketches returns a single sketch.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"))
    assert(indexConfig.sketches === Seq(MinMaxSketch("A")))
  }

  test("sketches returns multiple sketches.") {
    val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"), MinMaxSketch("B"))
    assert(indexConfig.sketches === Seq(MinMaxSketch("A"), MinMaxSketch("B")))
  }

  test("Duplicate sketches are not allowed.") {
    val exception = intercept[HyperspaceException] {
      DataSkippingIndexConfig("myIndex", MinMaxSketch("A"), MinMaxSketch("B"), MinMaxSketch("A"))
    }
    assert(exception.getMessage.contains("Some sketches are specified multiple times: MinMax(A)"))
  }

  test("Duplicate sketches are not allowed after the column resolution.") {
    val sourceData = createSourceData(spark.range(10).toDF("A"))
    val exception = intercept[HyperspaceException] {
      val indexConfig = DataSkippingIndexConfig("myIndex", MinMaxSketch("A"), MinMaxSketch("a"))
      indexConfig.createIndex(ctx, sourceData, Map())
    }
    assert(exception.getMessage.contains("Some sketches are specified multiple times: MinMax(A)"))
  }

  test("referencedColumns returns referenced columns of sketches.") {
    val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("A"), MinMaxSketch("B"))
    assert(indexConfig.referencedColumns === Seq("A", "B"))
  }

  test("createIndex works correctly with a MinMaxSketch.") {
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    assert(index.sketches === Seq(MinMaxSketch("A", Some(LongType))))
    val expectedSketchValues = sourceData
      .groupBy(input_file_name().as(fileNameCol))
      .agg(min("A"), max("A"))
    checkAnswer(indexData, withFileId(expectedSketchValues))
    assert(
      indexData.columns === Seq(IndexConstants.DATA_FILE_NAME_ID, "MinMax_A__0", "MinMax_A__1"))
  }

  test("createIndex works correctly with file paths with special characters.") {
    assume(!Path.WINDOWS)
    val sourceData = createSourceData(spark.range(100).toDF("A"), "table ,.;'`~!@#$%^&()_+|\"<>")
    val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("A"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    val expectedSketchValues = sourceData
      .groupBy(input_file_name().as(fileNameCol))
      .agg(min("A"), max("A"))
    checkAnswer(indexData, withFileId(expectedSketchValues))
  }

  test("createIndex works correctly with a BloomFilterSketch.") {
    val sourceData = createSourceData(spark.range(100).toDF("A"))
    val indexConfig = DataSkippingIndexConfig("MyIndex", BloomFilterSketch("A", 0.001, 20))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    assert(index.sketches === Seq(BloomFilterSketch("A", 0.001, 20, Some(LongType))))
    val valuesAndBloomFilters = indexData
      .collect()
      .map { row =>
        val fileId = row.getAs[Long](IndexConstants.DATA_FILE_NAME_ID)
        val filePath = fileIdTracker.getIdToFileMapping().toMap.apply(fileId)
        val values = spark.read.parquet(filePath).collect().toSeq.map(_.getLong(0))
        val bfData = row.getAs[Any]("BloomFilter_A__0.001__20__0")
        (values, bfData)
      }
    valuesAndBloomFilters.foreach {
      case (values, bfData) =>
        val bf = BloomFilter.create(20, 0.001)
        values.foreach(bf.put)
        assert(bfData === encodeExternal(bf))
    }
    assert(
      indexData.columns === Seq(IndexConstants.DATA_FILE_NAME_ID, "BloomFilter_A__0.001__20__0"))
  }

  test("createIndex resolves column names and data types.") {
    val sourceData = createSourceData(spark.range(10).toDF("Foo"))
    val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("foO"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    assert(index.sketches === Seq(MinMaxSketch("Foo", Some(LongType))))
  }

  test("createIndex creates partition sketches for partitioned source data.") {
    val sourceData =
      createPartitionedSourceData(spark.range(10).selectExpr("id as A", "id * 2 as B"), Seq("A"))
    val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("B"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    assert(
      index.sketches === Seq(
        PartitionSketch(Seq(("A", Some(IntegerType)))),
        MinMaxSketch("B", Some(LongType))))
  }

  test(
    "createIndex creates partition sketches for partitioned source data " +
      "with multiple partition columns.") {
    val sourceData =
      createPartitionedSourceData(
        spark.range(10).selectExpr("id as A", "id as B", "id * 2 as C"),
        Seq("A", "B"))
    val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("C"))
    val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
    assert(
      index.sketches === Seq(
        PartitionSketch(Seq(("A", Some(IntegerType)), ("B", Some(IntegerType)))),
        MinMaxSketch("C", Some(LongType))))
  }

  test(
    "createIndex does not create partition sketches for partitioned source data " +
      "if the config is turned off.") {
    withSQLConf(IndexConstants.DATASKIPPING_AUTO_PARTITION_SKETCH -> "false") {
      val sourceData =
        createPartitionedSourceData(
          spark.range(10).selectExpr("id as A", "id * 2 as B"),
          Seq("A"))
      val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("B"))
      val (index, indexData) = indexConfig.createIndex(ctx, sourceData, Map())
      assert(index.sketches === Seq(MinMaxSketch("B", Some(LongType))))
    }
  }

  test("createIndex throws an error if the data type is wrong.") {
    val sourceData = createSourceData(spark.range(10).toDF("Foo"))
    val indexConfig = DataSkippingIndexConfig("MyIndex", MinMaxSketch("foO", Some(StringType)))
    val ex = intercept[HyperspaceException] {
      indexConfig.createIndex(ctx, sourceData, Map())
    }
    assert(
      ex.getMessage.contains("Specified and analyzed data types differ: " +
        "expr=foO, specified=StringType, analyzed=LongType"))
  }
}
