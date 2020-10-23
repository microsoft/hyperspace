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

import org.apache.hadoop.fs.Path

import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.util.FileUtils

class PartitionedDataHybridScanTest extends HybridScanTest with HyperspaceSuite {

  override def beforeAll(): Unit = {
    super[HyperspaceSuite].beforeAll()
    import spark.implicits._
    val dfFromSample = sampleData.toDF("Date", "RGUID", "Query", "imprs", "clicks")
    hyperspace = new Hyperspace(spark)
    fileFormat = "parquet"
    fileFormat2 = "json"

    FileUtils.delete(new Path(sampleDataLocationRoot))
    dfFromSample.write
      .partitionBy("clicks", "Date")
      .format(fileFormat)
      .save(sampleDataFormatAppend)
    dfFromSample.write.partitionBy("Date").format(fileFormat).save(sampleDataFormatAppend2)
    dfFromSample.write.partitionBy("Date").format(fileFormat).save(sampleDataFormatDelete)
    dfFromSample.write.partitionBy("imprs").format(fileFormat).save(sampleDataFormatDelete2)
    dfFromSample.write
      .partitionBy("Date", "imprs")
      .format(fileFormat)
      .save(sampleDataFormatDelete3)
    dfFromSample.write.partitionBy("Date").format(fileFormat).save(sampleDataFormatDelete4)
    dfFromSample.write.partitionBy("Date").format(fileFormat).save(sampleDataFormatBoth)
    dfFromSample.write
      .partitionBy("Date", "clicks")
      .format(fileFormat2)
      .save(sampleDataFormat2Append)
    dfFromSample.write.partitionBy("Date").format(fileFormat2).save(sampleDataFormat2Delete)

    val indexConfig1 = IndexConfig("indexType1", Seq("clicks"), Seq("query"))
    val indexConfig2 = IndexConfig("indexType2", Seq("clicks"), Seq("Date"))

    setupIndexAndChangeData(
      fileFormat,
      sampleDataFormatAppend,
      indexConfig1.copy(indexName = "index_ParquetAppend"),
      appendCnt = 1,
      deleteCnt = 0)
    setupIndexAndChangeData(
      fileFormat,
      sampleDataFormatAppend2,
      indexConfig2.copy(indexName = "indexType2_ParquetAppend2"),
      appendCnt = 1,
      deleteCnt = 0)
    setupIndexAndChangeData(
      fileFormat2,
      sampleDataFormat2Append,
      indexConfig1.copy(indexName = "index_JsonAppend"),
      appendCnt = 1,
      deleteCnt = 0)

    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      setupIndexAndChangeData(
        fileFormat,
        sampleDataFormatDelete,
        indexConfig1.copy(indexName = "index_ParquetDelete"),
        appendCnt = 0,
        deleteCnt = 2)
      setupIndexAndChangeData(
        fileFormat,
        sampleDataFormatDelete3,
        indexConfig2.copy(indexName = "indexType2_ParquetDelete3"),
        appendCnt = 0,
        deleteCnt = 2)
      setupIndexAndChangeData(
        fileFormat,
        sampleDataFormatBoth,
        indexConfig1.copy(indexName = "index_ParquetBoth"),
        appendCnt = 1,
        deleteCnt = 1)
      setupIndexAndChangeData(
        fileFormat2,
        sampleDataFormat2Delete,
        indexConfig1.copy(indexName = "index_JsonDelete"),
        appendCnt = 0,
        deleteCnt = 2)
    }
  }

}
