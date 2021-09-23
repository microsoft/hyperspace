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
import org.apache.spark.SparkException
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types._

import com.microsoft.hyperspace._
import com.microsoft.hyperspace.index.IndexConstants
import com.microsoft.hyperspace.index.covering.CoveringIndexConfig
import com.microsoft.hyperspace.index.dataskipping.sketches._
import com.microsoft.hyperspace.index.plans.logical.IndexHadoopFsRelation

class DataSkippingIndexIntegrationTest extends DataSkippingSuite with IcebergTestUtils {
  import spark.implicits._

  override val numParallelism: Int = 10

  test("MinMax index is applied for a filter query (EqualTo).") {
    val df = createSourceData(spark.range(100).toDF("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    def query: DataFrame = df.filter("A = 1")
    checkIndexApplied(query, 1)
  }

  test("Empty relation is returned if no files match the index predicate.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    def query: DataFrame = df.filter("A = -1")
    checkIndexApplied(query, 0)
  }

  test("MinMax index is applied for a filter query (EqualTo) with expression.") {
    val df = createSourceData(spark.range(100).selectExpr("id as A", "id * 2 as B"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A + B")))
    def query: DataFrame = df.filter("A+B < 40")
    checkIndexApplied(query, 2)
  }

  test("Non-deterministic expression is blocked.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    val ex = intercept[HyperspaceException](
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A + rand()"))))
    assert(
      ex.msg.contains("DataSkippingIndex does not support indexing an expression " +
        "which is non-deterministic: A + rand()"))
  }

  test("Subquery expression is blocked.") {
    withTable("T") {
      spark.range(100).toDF("B").write.saveAsTable("T")
      val df = createSourceData(spark.range(100).toDF("A"))
      val ex = intercept[HyperspaceException](
        hs.createIndex(
          df,
          DataSkippingIndexConfig("myind", MinMaxSketch("A + (select max(B) from T)"))))
      assert(
        ex.msg.contains("DataSkippingIndex does not support indexing an expression " +
          "which has a subquery: A + (select max(B) from T)"))
    }
  }

  test("Foldable expression is blocked.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    val ex = intercept[HyperspaceException](
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("1+1"))))
    assert(
      ex.msg.contains("DataSkippingIndex does not support indexing an expression " +
        "which is evaluated to a constant: 1+1"))
  }

  test("Aggregate function is blocked.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    val ex = intercept[HyperspaceException](
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("min(A)"))))
    assert(
      ex.msg.contains("DataSkippingIndex does not support indexing aggregate functions: " +
        "min(A)"))
  }

  test("Window function is blocked.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    val ex = intercept[HyperspaceException](
      hs.createIndex(
        df,
        DataSkippingIndexConfig(
          "myind",
          MinMaxSketch("min(a) over (rows between 1 preceding and 1 following)"))))
    assert(
      ex.msg.contains("DataSkippingIndex does not support indexing window functions: " +
        "min(a) over (rows between 1 preceding and 1 following)"))
  }

  test("Expression not referencing the source column is blocked.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    val f = spark.udf.register("myfunc", () => 1)
    val ex = intercept[HyperspaceException](
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("myfunc()"))))
    assert(
      ex.msg.contains(
        "DataSkippingIndex does not support indexing an expression which does not " +
          "reference source columns: myfunc()"))
  }

  test("MinMax index is applied for a filter query (EqualTo) with UDF.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    spark.udf.register("myfunc", (a: Int) => a * 2)
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("myfunc(A)")))
    def query: DataFrame = df.filter("myfunc(A) = 10")
    checkIndexApplied(query, 1)
  }

  test("UDF matching is based on the name, not the actual lambda object.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    spark.udf.register("myfunc", (a: Int) => a * 2)
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("myfunc(A)")))
    // Register a new function with the same semantics.
    spark.udf.register("myfunc", (a: Int) => 2 * a)
    def query: DataFrame = df.filter("myfunc(A) = 10")
    checkIndexApplied(query, 1)
  }

  test("MinMax index is not applied for a filter query if it is not applicable.") {
    val df = createSourceData(spark.range(100).selectExpr("id as A", "id * 2 as B"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("B")))
    def query: DataFrame = df.filter("A = 1")
    checkIndexApplied(query, numParallelism)
  }

  test("MinMax index is not applied for a filter query if the filter condition is unsuitable.") {
    val df = createSourceData(spark.range(100).selectExpr("id as A", "id * 2 as B"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    def query: DataFrame = df.filter("A = 1 or B = 2")
    checkIndexApplied(query, numParallelism)
  }

  test("MinMax index is not applied for a filter query if the filter condition is IsNull.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    def query: DataFrame = df.filter("A is null")
    checkIndexApplied(query, numParallelism)
  }

  test("Multiple indexes are applied to multiple filters.") {
    val df = createSourceData(spark.range(100).toDF("A"), path = "TA")
    val df2 = createSourceData(spark.range(100, 200).toDF("B"), path = "TB")
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    hs.createIndex(df2, DataSkippingIndexConfig("myind2", MinMaxSketch("B")))
    def query: DataFrame = df.filter("A = 10").union(df2.filter("B = 110"))
    checkIndexApplied(query, 2)
  }

  test("Single index is applied to multiple filters.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    def query: DataFrame = df.filter("A = 10").union(df.filter("A = 20"))
    checkIndexApplied(query, 2)
  }

  test("Single index is applied to a single filter.") {
    val df = createSourceData(spark.range(100).selectExpr("id as A", "id * 2 as B"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    def query: DataFrame = df.filter("A = 10").union(df.filter("B = 120"))
    checkIndexApplied(query, numParallelism + 1)
  }

  test("BloomFilter index is applied for a filter query (EqualTo).") {
    withAndWithoutCodegen {
      withIndex("myind") {
        val df = createSourceData(spark.range(100).toDF("A"))
        hs.createIndex(df, DataSkippingIndexConfig("myind", BloomFilterSketch("A", 0.01, 10)))
        def query: DataFrame = df.filter("A = 1")
        checkIndexApplied(query, 1)
      }
    }
  }

  test(
    "BloomFilter index is applied for a filter query (EqualTo) " +
      "where some source data files has only null values.") {
    withAndWithoutCodegen {
      withIndex("myind") {
        val df = createSourceData(Seq[Integer](1, 2, 3, null, 5, null, 7, 8, 9, null).toDF("A"))
        hs.createIndex(df, DataSkippingIndexConfig("myind", BloomFilterSketch("A", 0.01, 10)))
        def query: DataFrame = df.filter("A = 1")
        checkIndexApplied(query, 1)
      }
    }
  }

  test("BloomFilter index is applied for a filter query (In).") {
    withAndWithoutCodegen {
      withIndex("myind") {
        val df = createSourceData(spark.range(100).toDF("A"))
        hs.createIndex(df, DataSkippingIndexConfig("myind", BloomFilterSketch("A", 0.01, 10)))
        def query: DataFrame = df.filter("A in (1, 11, 19)")
        checkIndexApplied(query, 2)
      }
    }
  }

  test("BloomFilter index support string type.") {
    withAndWithoutCodegen {
      withIndex("myind") {
        val df = createSourceData(('a' to 'z').map(_.toString).toDF("A"))
        hs.createIndex(df, DataSkippingIndexConfig("myind", BloomFilterSketch("A", 0.01, 10)))
        def query: DataFrame = df.filter("A = 'a'")
        checkIndexApplied(query, 1)
      }
    }
  }

  test("BloomFilter index does not support double type.") {
    val df = createSourceData((0 until 10).map(_.toDouble).toDF("A"))
    val ex = intercept[SparkException](
      hs.createIndex(df, DataSkippingIndexConfig("myind", BloomFilterSketch("A", 0.01, 10))))
    assert(ex.getCause().getMessage().contains("BloomFilter does not support DoubleType"))
  }

  test(
    "DataSkippingIndex works correctly for CSV where the same source data files can be " +
      "interpreted differently.") {
    // String order: 1 < 10 < 2
    // Int order: 1 < 2 < 10
    createFile(dataPath("1.csv"), Seq("a", "1", "2", "10").mkString("\n").getBytes())
    createFile(dataPath("2.csv"), Seq("a", "3", "4", "5").mkString("\n").getBytes())
    val paths = Seq(dataPath("1.csv").toString, dataPath("2.csv").toString)
    val dfString = spark.read.option("header", "true").csv(paths: _*)
    assert(dfString.schema.head.dataType === StringType)
    val dfInt = spark.read.option("header", "true").option("inferSchema", "true").csv(paths: _*)
    assert(dfInt.schema.head.dataType === IntegerType)

    withIndex("myind") {
      hs.createIndex(dfString, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
      checkIndexApplied(dfString.filter("A = 3"), 2)
      checkIndexApplied(dfString.filter("A = 10"), 2)
      checkIndexApplied(dfString.filter("A = '3'"), 1)
      checkIndexApplied(dfString.filter("A = '10'"), 1)
      checkIndexApplied(dfInt.filter("A = 3"), 2)
      checkIndexApplied(dfInt.filter("A = 10"), 2)
      checkIndexApplied(dfInt.filter("A = '3'"), 2)
      checkIndexApplied(dfInt.filter("A = '10'"), 2)
    }
    withIndex("myind") {
      hs.createIndex(dfInt, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
      checkIndexApplied(dfString.filter("A = 3"), 2)
      checkIndexApplied(dfString.filter("A = 10"), 2)
      checkIndexApplied(dfString.filter("A = '3'"), 2)
      checkIndexApplied(dfString.filter("A = '10'"), 2)
      checkIndexApplied(dfInt.filter("A = 3"), 2)
      checkIndexApplied(dfInt.filter("A = 10"), 1)
      checkIndexApplied(dfInt.filter("A = '3'"), 2)
      checkIndexApplied(dfInt.filter("A = '10'"), 1)
    }
  }

  test("MinMax index is applied for a filter query (EqualTo) with selection.") {
    val df = createSourceData(spark.range(100).selectExpr("id as A", "id * 2 as B"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    def query: DataFrame = df.filter("A = 1").select("B")
    checkIndexApplied(query, 1)
  }

  test("MinMax index can be refreshed (mode = incremental).") {
    val df = createSourceData(spark.range(100).toDF("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    createSourceData(spark.range(100, 200).toDF("A"), saveMode = SaveMode.Append)
    hs.refreshIndex("myind", "incremental")
    def query: DataFrame = spark.read.parquet(dataPath().toString).filter("A = 1 OR A = 123")
    checkIndexApplied(query, 2)
    assert(numIndexDataFiles("myind") === 2)
  }

  test("MinMax index can be refreshed (mode = full).") {
    val df = createSourceData(spark.range(100).toDF("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    createSourceData(spark.range(100, 200).toDF("A"), saveMode = SaveMode.Append)
    hs.refreshIndex("myind", "full")
    def query: DataFrame = spark.read.parquet(dataPath().toString).filter("A = 1 OR A = 123")
    checkIndexApplied(query, 2)
    assert(numIndexDataFiles("myind") === 1)
  }

  test("MinMax index can be refreshed (mode = full) for partitioned data.") {
    val df = createPartitionedSourceData(
      spark.range(100).selectExpr("id as A", "cast(id / 10 as int) as B"),
      Seq("B"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    createPartitionedSourceData(
      spark.range(100, 200).selectExpr("id as A", "cast(id / 15 as int) as B"),
      Seq("B"),
      saveMode = SaveMode.Append)
    hs.refreshIndex("myind", "full")
    def query: DataFrame = spark.read.parquet(dataPath().toString).filter("A = 1 OR A = 123")
    checkIndexApplied(query, 2)
    assert(numIndexDataFiles("myind") === 1)
  }

  test(
    "MinMax index can be applied without refresh when source files are added " +
      "if hybrid scan is enabled.") {
    withSQLConf(
      IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
      IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "1") {
      val df = createSourceData(spark.range(100).toDF("A"))
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
      createSourceData(spark.range(100, 200).toDF("A"), saveMode = SaveMode.Append)
      def query: DataFrame = spark.read.parquet(dataPath().toString).filter("A = 1 OR A = 123")
      checkIndexApplied(query, 11)
    }
  }

  test(
    "BloomFilter index can be applied without refresh when source files are deleted " +
      "if hybrid scan is enabled.") {
    withSQLConf(
      IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
      IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD -> "1") {
      val df = createSourceData(spark.range(100).toDF("A"))
      hs.createIndex(df, DataSkippingIndexConfig("myind", BloomFilterSketch("A", 0.001, 10)))
      deleteFile(listFiles(dataPath()).filter(isParquet).head.getPath)
      def query: DataFrame = spark.read.parquet(dataPath().toString).filter("A in (25, 50, 75)")
      checkIndexApplied(query, 3)
    }
  }

  test("Empty source data does not cause an error.") {
    val df = createSourceData(spark.range(0).toDF("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    def query: DataFrame = df.filter("A = 1")
    checkIndexApplied(query, 1)
  }

  test("Empty source data followed by refresh incremental works as expected.") {
    val df = createSourceData(spark.range(0).toDF("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    createSourceData(spark.range(100).toDF("A"), saveMode = SaveMode.Append)
    hs.refreshIndex("myind", "incremental")
    def query: DataFrame = spark.read.parquet(dataPath().toString).filter("A = 1")
    checkIndexApplied(query, 2)
  }

  test("MinMax index can be optimized.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    createSourceData(spark.range(100, 200).toDF("A"), saveMode = SaveMode.Append)
    hs.refreshIndex("myind", "incremental")
    assert(numIndexDataFiles("myind") === 2)
    hs.optimizeIndex("myind")
    assert(numIndexDataFiles("myind") === 1)
    def query: DataFrame = spark.read.parquet(dataPath().toString).filter("A = 1 OR A = 123")
    checkIndexApplied(query, 2)
  }

  test("CoveringIndex is applied if both CoveringIndex and DataSkippingIndex are applicable.") {
    val df = createSourceData(spark.range(100).toDF("A"))
    hs.createIndex(df, DataSkippingIndexConfig("ds", MinMaxSketch("A")))
    hs.createIndex(df, CoveringIndexConfig("ci", Seq("A"), Nil))
    spark.enableHyperspace
    def query: DataFrame = df.filter("A = 1 or A = 50")
    val rel = query.queryExecution.optimizedPlan.collect {
      case LogicalRelation(rel: IndexHadoopFsRelation, _, _, _) => rel
    }
    assert(rel.map(_.indexName) === Seq("ci"))
    checkAnswer(query, Seq(1, 50).toDF("A"))
  }

  test("DataSkippingIndex is applied if CoveringIndex is not applicable.") {
    val df = createSourceData(spark.range(100).selectExpr("id as A", "id * 2 as B"))
    hs.createIndex(df, DataSkippingIndexConfig("ds", MinMaxSketch("A")))
    hs.createIndex(df, CoveringIndexConfig("ci", Seq("A"), Nil))
    spark.enableHyperspace
    def query: DataFrame = df.filter("A = 1 or A = 50")
    val rel = query.queryExecution.optimizedPlan.collect {
      case LogicalRelation(rel: IndexHadoopFsRelation, _, _, _) => rel
    }
    assert(rel.map(_.indexName) === Seq("ds"))
    checkAnswer(query, Seq((1, 2), (50, 100)).toDF("A", "B"))
  }

  test("Both CoveringIndex and DataSkippnigIndex can be applied.") {
    val df = createSourceData(spark.range(100).selectExpr("id as A", "id * 2 as B"))
    hs.createIndex(df, CoveringIndexConfig("ci", Seq("A"), Nil))
    hs.createIndex(df, DataSkippingIndexConfig("ds", MinMaxSketch("B")))
    spark.enableHyperspace
    def query: DataFrame = df.filter("A = 1").select("A").union(df.filter("B = 100").select("A"))
    val rel = query.queryExecution.optimizedPlan.collect {
      case LogicalRelation(rel: IndexHadoopFsRelation, _, _, _) => rel
    }
    assert(rel.map(_.indexName).sorted === Seq("ci", "ds"))
    checkAnswer(query, Seq(1, 50).toDF("A"))
  }

  test("DataSkippingIndex works correctly with files having special characters in their name.") {
    assume(!Path.WINDOWS)
    val df = createSourceData(spark.range(100).toDF("A"), "table ,.;'`~!@#$%^&()_+|\"<>")
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
    def query: DataFrame = df.filter("A = 1")
    checkIndexApplied(query, 1)
  }

  test("DataSkippingIndex works correctly with catalog tables") {
    withTable("T") {
      spark.range(100).toDF("A").write.saveAsTable("T")
      val df = spark.read.table("T")
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
      def query: DataFrame = df.filter("A = 1")
      checkIndexApplied(query, 1)
    }
  }

  test("DataSkippingIndex works correctly with partitioned data.") {
    val df = createPartitionedSourceData(
      spark.range(1000).selectExpr("cast(id/10 as int) as A", "id as B"),
      Seq("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("B")))
    def query: DataFrame = df.filter("A = 1 or B = 100")
    checkIndexApplied(query, 2)
  }

  test(
    "DataSkippingIndex works correctly with partitioned data " +
      "with multiple partition columns.") {
    val df = createPartitionedSourceData(
      spark
        .range(1000)
        .selectExpr("cast(id/100 as int) as A", "cast(id/10 as int) as B", "id as C"),
      Seq("A", "B"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("C")))
    def query: DataFrame = df.filter("A = 1 or B = 1 or C = 1")
    checkIndexApplied(query, 12)
  }

  test(
    "DataSkippingIndex works correctly with partitioned data " +
      "with a different filter condition.") {
    val df = createPartitionedSourceData(
      spark.range(1000).selectExpr("cast(id/200 as int)*200 as A", "id as B"),
      Seq("A"))
    hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("B")))
    def query: DataFrame = df.filter("A = B")
    checkIndexApplied(query, 5)
  }

  test("DataSkippingIndex works correctly with Delta Lake tables.") {
    withSQLConf(
      "spark.hyperspace.index.sources.fileBasedBuilders" ->
        "com.microsoft.hyperspace.index.sources.delta.DeltaLakeFileBasedSourceBuilder") {
      val df = createSourceData(spark.range(100).toDF("A"), format = "delta")
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
      def query: DataFrame = df.filter("A = 1")
      checkIndexApplied(query, 1)
    }
  }

  test("DataSkippingIndex works correctly with partitioned Delta Lake tables.") {
    withSQLConf(
      "spark.hyperspace.index.sources.fileBasedBuilders" ->
        "com.microsoft.hyperspace.index.sources.delta.DeltaLakeFileBasedSourceBuilder") {
      val df = createPartitionedSourceData(
        spark.range(100).selectExpr("id as A", "cast(id / 10 as int) as B"),
        Seq("B"),
        format = "delta")
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
      def query: DataFrame = df.filter("A = 1 or B = 5")
      checkIndexApplied(query, 2)
    }
  }

  test("DataSkippingIndex works correctly with Delta time travel.") {
    withTable("T") {
      withSQLConf(
        "spark.hyperspace.index.sources.fileBasedBuilders" ->
          "com.microsoft.hyperspace.index.sources.delta.DeltaLakeFileBasedSourceBuilder",
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
        IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "10",
        IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD -> "10") {

        // version 0
        spark.range(100).toDF("A").write.format("delta").save(dataPath("T").toString)

        // version 1
        spark
          .range(100, 200)
          .toDF("A")
          .write
          .format("delta")
          .mode("append")
          .save(dataPath("T").toString)

        // version 2
        spark
          .range(200, 300)
          .toDF("A")
          .write
          .format("delta")
          .mode("append")
          .save(dataPath("T").toString)

        val df = (v: Int) =>
          spark.read.format("delta").option("versionAsOf", v).load(dataPath("T").toString)

        // Create an index with version 1 data
        hs.createIndex(df(1), DataSkippingIndexConfig("myind", MinMaxSketch("A")))

        def query0: DataFrame = df(0).filter("A = 1 or A = 101 or A = 201")
        checkIndexApplied(query0, 1)

        def query1: DataFrame = df(1).filter("A = 1 or A = 101 or A = 201")
        checkIndexApplied(query1, 2)

        def query2: DataFrame = df(2).filter("A = 1 or A = 101 or A = 201")
        checkIndexApplied(query2, 12)
      }
    }
  }

  test("DataSkippingIndex works correctly with Delta time travel with partitions.") {
    withTable("T") {
      withSQLConf(
        "spark.hyperspace.index.sources.fileBasedBuilders" ->
          "com.microsoft.hyperspace.index.sources.delta.DeltaLakeFileBasedSourceBuilder",
        IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
        IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "10",
        IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD -> "10") {

        // version 0
        spark
          .range(100)
          .selectExpr("id as A", "cast(id / 10 as int) as B")
          .write
          .format("delta")
          .save(dataPath("T").toString)

        // version 1
        spark
          .range(100, 200)
          .selectExpr("id as A", "cast(id / 15 as int) as B")
          .write
          .format("delta")
          .mode("append")
          .save(dataPath("T").toString)

        // version 2
        spark
          .range(200, 300)
          .selectExpr("id as A", "cast(id / 20 as int) as B")
          .write
          .format("delta")
          .mode("append")
          .save(dataPath("T").toString)

        val df = (v: Int) =>
          spark.read.format("delta").option("versionAsOf", v).load(dataPath("T").toString)

        // Create an index with version 1 data
        hs.createIndex(df(1), DataSkippingIndexConfig("myind", MinMaxSketch("A")))

        def query0: DataFrame = df(0).filter("A = 1 or A = 101 or A = 201")
        checkIndexApplied(query0, 1)

        def query1: DataFrame = df(1).filter("A = 1 or A = 101 or A = 201")
        checkIndexApplied(query1, 2)

        def query2: DataFrame = df(2).filter("A = 1 or A = 101 or A = 201")
        checkIndexApplied(query2, 12)
      }
    }
  }

  test("DataSkippingIndex works correctly with Iceberg tables.") {
    withSQLConf(
      "spark.hyperspace.index.sources.fileBasedBuilders" ->
        "com.microsoft.hyperspace.index.sources.iceberg.IcebergFileBasedSourceBuilder") {
      val data = spark.range(100).toDF("A")
      createIcebergTable(dataPath("T").toString, data)
      val df = createSourceData(data, format = "iceberg")
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
      def query: DataFrame = df.filter("A = 1")
      checkIndexApplied(query, 1)
    }
  }

  test("DataSkippingIndex works correctly with partitioned Iceberg tables.") {
    withSQLConf(
      "spark.hyperspace.index.sources.fileBasedBuilders" ->
        "com.microsoft.hyperspace.index.sources.iceberg.IcebergFileBasedSourceBuilder") {
      val data = spark.range(100).selectExpr("id as A", "cast(id / 10 as int) as B")
      createIcebergTableWithPartitions(dataPath("T").toString, data, "B")
      val df = createPartitionedSourceData(data, Seq("B"), format = "iceberg")
      hs.createIndex(df, DataSkippingIndexConfig("myind", MinMaxSketch("A")))
      def query: DataFrame = df.filter("A = 1")
      checkIndexApplied(query, 1)
    }
  }

  test("DataSkippingIndex works correctly with Iceberg time travel.") {
    withSQLConf(
      "spark.hyperspace.index.sources.fileBasedBuilders" ->
        "com.microsoft.hyperspace.index.sources.iceberg.IcebergFileBasedSourceBuilder",
      IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
      IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "10",
      IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD -> "10") {

      // version 0
      val data = spark.range(100).toDF("A")
      val table = createIcebergTable(dataPath("T").toString, data)
      data.write.format("iceberg").mode("overwrite").save(dataPath("T").toString)
      val v0 = table.currentSnapshot.snapshotId

      // version 1
      spark
        .range(100, 200)
        .toDF("A")
        .write
        .format("iceberg")
        .mode("append")
        .save(dataPath("T").toString)
      table.newTransaction().commitTransaction()
      val v1 = table.currentSnapshot.snapshotId

      // version 2
      spark
        .range(200, 300)
        .toDF("A")
        .write
        .format("iceberg")
        .mode("append")
        .save(dataPath("T").toString)
      table.newTransaction().commitTransaction()
      val v2 = table.currentSnapshot.snapshotId

      val df = (v: Long) =>
        spark.read.format("iceberg").option("snapshot-id", v).load(dataPath("T").toString)

      // Create an index with version 1 data
      hs.createIndex(df(v1), DataSkippingIndexConfig("myind", MinMaxSketch("A")))

      def query0: DataFrame = df(v0).filter("A = 1 or A = 101 or A = 201")
      checkIndexApplied(query0, 1)

      def query1: DataFrame = df(v1).filter("A = 1 or A = 101 or A = 201")
      checkIndexApplied(query1, 2)

      def query2: DataFrame = df(v2).filter("A = 1 or A = 101 or A = 201")
      checkIndexApplied(query2, 12)
    }
  }

  test("DataSkippingIndex works correctly with Iceberg time travel with partitions.") {
    withSQLConf(
      "spark.hyperspace.index.sources.fileBasedBuilders" ->
        "com.microsoft.hyperspace.index.sources.iceberg.IcebergFileBasedSourceBuilder",
      IndexConstants.INDEX_HYBRID_SCAN_ENABLED -> "true",
      IndexConstants.INDEX_HYBRID_SCAN_APPENDED_RATIO_THRESHOLD -> "10",
      IndexConstants.INDEX_HYBRID_SCAN_DELETED_RATIO_THRESHOLD -> "10") {

      // version 0
      val data = spark.range(100).selectExpr("id as A", "cast(id / 10 as int) as B")
      val table = createIcebergTable(dataPath("T").toString, data)
      data.write.format("iceberg").mode("overwrite").save(dataPath("T").toString)
      val v0 = table.currentSnapshot.snapshotId

      // version 1
      spark
        .range(100, 200)
        .selectExpr("id as A", "cast(id / 15 as int) as B")
        .write
        .format("iceberg")
        .mode("append")
        .save(dataPath("T").toString)
      table.newTransaction().commitTransaction()
      val v1 = table.currentSnapshot.snapshotId

      // version 2
      spark
        .range(200, 300)
        .selectExpr("id as A", "cast(id / 20 as int) as B")
        .write
        .format("iceberg")
        .mode("append")
        .save(dataPath("T").toString)
      table.newTransaction().commitTransaction()
      val v2 = table.currentSnapshot.snapshotId

      val df = (v: Long) =>
        spark.read.format("iceberg").option("snapshot-id", v).load(dataPath("T").toString)

      // Create an index with version 1 data
      hs.createIndex(df(v1), DataSkippingIndexConfig("myind", MinMaxSketch("A")))

      def query0: DataFrame = df(v0).filter("A = 1 or A = 101 or A = 201")
      checkIndexApplied(query0, 1)

      def query1: DataFrame = df(v1).filter("A = 1 or A = 101 or A = 201")
      checkIndexApplied(query1, 2)

      def query2: DataFrame = df(v2).filter("A = 1 or A = 101 or A = 201")
      checkIndexApplied(query2, 12)
    }
  }

  def checkIndexApplied(query: => DataFrame, numExpectedFiles: Int): Unit = {
    withClue(s"query = ${query.queryExecution.logical}numExpectedFiles = $numExpectedFiles\n") {
      spark.disableHyperspace
      val queryWithoutIndex = query
      queryWithoutIndex.collect()
      spark.enableHyperspace
      val queryWithIndex = query
      queryWithIndex.collect()
      checkAnswer(queryWithIndex, queryWithoutIndex)
      assert(numAccessedFiles(queryWithIndex) === numExpectedFiles)
    }
  }

  def numIndexDataFiles(name: String): Int = {
    val manager = Hyperspace.getContext(spark).indexCollectionManager
    val latestVersion = manager.getIndexVersions(name, Seq("ACTIVE")).max
    manager.getIndex(name, latestVersion).get.content.files.length
  }
}
