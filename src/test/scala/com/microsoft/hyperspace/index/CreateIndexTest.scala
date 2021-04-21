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

import scala.collection.mutable.WrappedArray
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.plans.SQLHelper
import com.microsoft.hyperspace.{Hyperspace, HyperspaceException, SampleData}
import com.microsoft.hyperspace.util.FileUtils
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.output.ByteArrayOutputStream
import org.apache.spark.util.sketch.BloomFilter

import java.io.ByteArrayInputStream

class CreateIndexTest extends HyperspaceSuite with SQLHelper {
  override val systemPath = new Path("src/test/resources/indexLocation")
  private val testDir = "src/test/resources/createIndexTests/"
  private val nonPartitionedDataPath = testDir + "sampleparquet"
  private val partitionedDataPath = testDir + "samplepartitionedparquet"
  private val comicDataDir = testDir + "comics/"
  private val dcDataPath = comicDataDir + "dc"
  private val marvelDataPath = comicDataDir + "marvel"
  private val partitionKeys = Seq("Date", "Query")
  private val indexConfig1 = IndexConfig("index1", Seq("RGUID"), Seq("Date"))
  private val indexConfig2 = IndexConfig("index2", Seq("Query"), Seq("imprs"))
  private val indexConfig3 = IndexConfig("index3", Seq("imprs"), Seq("clicks"))
  private val indexConfig4 = IndexConfig("index4", Seq("Date", "Query"), Seq("clicks"))
  private val bloomIndexConfig = BloomFilterIndexConfig("indexBF1", "Affiliation", 100)
  private var nonPartitionedDataDF: DataFrame = _
  private var partitionedDataDF: DataFrame = _
  private var comicDataDF: DataFrame = _
  private var hyperspace: Hyperspace = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    hyperspace = new Hyperspace(spark)
    FileUtils.delete(new Path(testDir), isRecursive = true)

    val dataColumns = Seq("Date", "RGUID", "Query", "imprs", "clicks")
    val comicDataColumns = Seq("Year", "Name", "Power", "Affiliation", "Universe")

    // save test data non-partitioned.
    SampleData.save(spark, nonPartitionedDataPath, dataColumns)
    nonPartitionedDataDF = spark.read.parquet(nonPartitionedDataPath)

    // save test data partitioned.
    SampleData.save(spark, partitionedDataPath, dataColumns, Some(partitionKeys))
    partitionedDataDF = spark.read.parquet(partitionedDataPath)

    // save test data comic.
    SampleData.saveComics(spark, SampleData.testDataMarvel, marvelDataPath, comicDataColumns)
    SampleData.saveComics(spark, SampleData.testDataDC, dcDataPath, comicDataColumns)
    comicDataDF = spark.read.parquet(marvelDataPath, dcDataPath)
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(testDir), isRecursive = true)
    super.afterAll()
  }

  after {
    FileUtils.delete(systemPath)
  }

  test("Sho How Bloom Works") {
    // How BF are created
    val mbfS = spark.read.parquet(marvelDataPath).select("Affiliation")
      .stat.bloomFilter("Affiliation", 100, 0.03)

    val dbfS = spark.read.parquet(dcDataPath).select("Affiliation")
      .stat.bloomFilter("Affiliation", 100, 0.03)

    // All BF are stored locally and read again
    val mbfByteStream = new ByteArrayOutputStream()
    mbfS.writeTo(mbfByteStream)

    val dbfByteStream = new ByteArrayOutputStream()
    dbfS.writeTo(dbfByteStream)
    val bloomDF = spark.createDataFrame(
      Seq(
        ("marvel", Base64.encodeBase64String(mbfByteStream.toByteArray)),
        ("dc", Base64.encodeBase64String(dbfByteStream.toByteArray))
      )
    ).toDF("FileName", "Data")
    bloomDF.write.parquet(new Path(comicDataDir, "bf.parquet").toString)

    // BF read
    val bloomDFR = spark.read.parquet(new Path(comicDataDir, "bf.parquet").toString)

    // BF used to check for filters
//    bloomDFR.filter().select("filename")
    bloomDFR.filter("FileName = 'marvel'").select("Data").collect()
      .foreach {
        row => {
          val mbf = BloomFilter.readFrom(new ByteArrayInputStream(
            Base64.decodeBase64(row.getString(0))))
          assert(mbf.mightContain("XMen"))
          assert(mbf.mightContainString("Avengers"))
          assert(!mbf.mightContainString("Suicide Squad"))
          assert(!mbf.mightContainString("Justice League"))
          assert(!mbf.mightContainString("Suicide"))
        }
      }
    bloomDFR.filter("FileName = 'dc'").select("Data").collect()
      .foreach{
        row => {
            val dbf = BloomFilter.readFrom(new ByteArrayInputStream(
              Base64.decodeBase64(row.getString(0))))
            assert(!dbf.mightContainString("XMen"))
            assert(!dbf.mightContainString("Avengers"))
            assert(dbf.mightContainString("Suicide Squad"))
            assert(dbf.mightContainString("Justice League"))
            assert(!dbf.mightContainString("Justice"))
          }
        }
  }

  test("Creating one covering index.") {
    hyperspace.createIndex(nonPartitionedDataDF, indexConfig1)
    val count = hyperspace.indexes.where(s"name = '${indexConfig1.indexName}' ").count
    assert(count == 1)
  }

  test("Creating one bloom filter index.") {
    hyperspace.createIndex(comicDataDF, bloomIndexConfig)
    spark.read.parquet(
      systemPath.toString + "\\" + bloomIndexConfig.indexName + "\\v__=0\\bf.parquet").show()
    val count = hyperspace.indexes.where(s"name = '${bloomIndexConfig.indexName}' ").count
    assert(count == 1)
  }

  test("Creating index with existing index name fails.") {
    hyperspace.createIndex(nonPartitionedDataDF, indexConfig1)
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig2.copy(indexName = "index1"))
    }
    assert(exception.getMessage.contains("Another Index with name index1 already exists"))
  }

  test("Creating index with existing index name (case-insensitive) fails.") {
    hyperspace.createIndex(nonPartitionedDataDF, indexConfig1)
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig1.copy(indexName = "INDEX1"))
    }
    assert(exception.getMessage.contains("Another Index with name INDEX1 already exists"))
  }

  test("Index creation fails since indexConfig does not satisfy the table schema.") {
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(
        nonPartitionedDataDF,
        IndexConfig("index1", Seq("IllegalColA"), Seq("IllegalColB")))
    }
    assert(exception.getMessage.contains("Index config is not applicable to dataframe schema"))
  }

  test("Index creation passes with columns of different case if case-sensitivity is false.") {
    hyperspace.createIndex(
      nonPartitionedDataDF,
      IndexConfig("index1", Seq("qUeRy"), Seq("ImpRS")))
    val indexes = hyperspace.indexes.where(s"name = '${indexConfig1.indexName}' ")
    assert(indexes.count == 1)
    assert(
      indexes.head.getAs[WrappedArray[String]]("indexedColumns").head == "Query",
      "Indexed columns with wrong case are stored in metadata")
    assert(
      indexes.head.getAs[WrappedArray[String]]("includedColumns").head == "imprs",
      "Included columns with wrong case are stored in metadata")
  }

  test("Index creation fails with columns of different case if case-sensitivity is true.") {
    withSQLConf("spark.sql.caseSensitive" -> "true") {
      val exception = intercept[HyperspaceException] {
        hyperspace.createIndex(
          nonPartitionedDataDF,
          IndexConfig("index1", Seq("qUeRy"), Seq("ImpRS")))
      }
      assert(exception.getMessage.contains("Index config is not applicable to dataframe schema."))
    }
  }

  test("Index creation fails since the dataframe has a filter node.") {
    val dfWithFilter = nonPartitionedDataDF.filter("Query='facebook'")
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(dfWithFilter, indexConfig1)
    }
    assert(
      exception.getMessage.contains(
        "Only creating index over HDFS file based scan nodes is supported."))
  }

  test("Index creation fails since the dataframe has a projection node.") {
    val dfWithSelect = nonPartitionedDataDF.select("Query")
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(dfWithSelect, indexConfig1)
    }
    assert(
      exception.getMessage.contains(
        "Only creating index over HDFS file based scan nodes is supported."))
  }

  test("Index creation fails since the dataframe has a join node.") {
    val dfJoin = nonPartitionedDataDF
      .join(nonPartitionedDataDF, nonPartitionedDataDF("Query") === nonPartitionedDataDF("Query"))
      .select(
        nonPartitionedDataDF("RGUID"),
        nonPartitionedDataDF("Query"),
        nonPartitionedDataDF("imprs"))
    val exception = intercept[HyperspaceException] {
      hyperspace.createIndex(dfJoin, indexConfig1)
    }
    assert(
      exception.getMessage.contains(
        "Only creating index over HDFS file based scan nodes is supported."))
  }

  test("Check lineage in index records for non-partitioned data.") {
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      hyperspace.createIndex(nonPartitionedDataDF, indexConfig1)
      val indexRecordsDF = spark.read.parquet(
        s"$systemPath/${indexConfig1.indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")

      // For non-partitioned data, only file name lineage column should be added to index schema.
      assert(
        indexRecordsDF.schema.fieldNames.sorted ===
          (indexConfig1.indexedColumns ++ indexConfig1.includedColumns ++
            Seq(IndexConstants.DATA_FILE_NAME_ID)).sorted)
    }
  }

  test("Check lineage in index records for partitioned data when partition key is not in config.") {
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      hyperspace.createIndex(partitionedDataDF, indexConfig3)
      val indexRecordsDF = spark.read.parquet(
        s"$systemPath/${indexConfig3.indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")

      // For partitioned data, beside file name lineage column all partition keys columns
      // should be added to index schema if they are not already among index config columns.
      assert(
        indexRecordsDF.schema.fieldNames.sorted ===
          (indexConfig3.indexedColumns ++ indexConfig3.includedColumns ++
            Seq(IndexConstants.DATA_FILE_NAME_ID) ++ partitionKeys).sorted)
    }
  }

  test("Check lineage in index records for partitioned data when partition key is in config.") {
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      hyperspace.createIndex(partitionedDataDF, indexConfig4)
      val indexRecordsDF = spark.read.parquet(
        s"$systemPath/${indexConfig4.indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")

      // For partitioned data, if partition keys are already in index config columns,
      // there should be no duplicates due to adding lineage.
      assert(
        indexRecordsDF.schema.fieldNames.sorted ===
          (indexConfig4.indexedColumns ++ indexConfig4.includedColumns ++
            Seq(IndexConstants.DATA_FILE_NAME_ID)).sorted)
    }
  }

  test("Check lineage in index records for partitioned data when partition key is in load path.") {
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      val dataDF =
        spark.read.parquet(s"$partitionedDataPath/${partitionKeys.head}=2017-09-03")
      hyperspace.createIndex(dataDF, indexConfig3)
      val indexRecordsDF = spark.read.parquet(
        s"$systemPath/${indexConfig3.indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")

      // As data load path includes first partition key, index schema should only contain
      // file name column and second partition key column as lineage columns.
      assert(
        indexRecordsDF.schema.fieldNames.sorted ===
          (indexConfig3.indexedColumns ++ indexConfig3.includedColumns ++
            Seq(IndexConstants.DATA_FILE_NAME_ID, partitionKeys(1))).sorted)
    }
  }

  test("Verify content of lineage column.") {
    withSQLConf(IndexConstants.INDEX_LINEAGE_ENABLED -> "true") {
      val dataPath = new Path(nonPartitionedDataPath, "*parquet")
      val dataFilesCount = dataPath
        .getFileSystem(new Configuration)
        .globStatus(dataPath)
        .length
        .toLong

      // File ids are assigned incrementally starting from 0.
      val lineageRange = 0L to dataFilesCount

      hyperspace.createIndex(nonPartitionedDataDF, indexConfig1)
      val indexRecordsDF = spark.read.parquet(
        s"$systemPath/${indexConfig1.indexName}/${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")
      val lineageValues = indexRecordsDF
        .select(IndexConstants.DATA_FILE_NAME_ID)
        .distinct()
        .collect()
        .map(r => r.getLong(0))

      lineageValues.forall(lineageRange.contains(_))
    }
  }
}
