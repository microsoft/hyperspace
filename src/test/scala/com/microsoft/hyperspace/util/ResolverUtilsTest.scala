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

package com.microsoft.hyperspace.util

import org.apache.spark.SparkFunSuite

import com.microsoft.hyperspace.{HyperspaceException, SparkInvolvedSuite}
import com.microsoft.hyperspace.util.ResolverUtils.ResolvedColumn

class ResolverUtilsTest extends SparkFunSuite with SparkInvolvedSuite {

  test("Verify testResolve against dataframe - simple.") {
    import spark.implicits._

    val coll = Seq((1, "a", "a2"))
    val df = coll.toDF("id", "name", "other")

    assert(
      ResolverUtils
        .resolve(spark, Seq("id", "name"), df.queryExecution.analyzed)
        .contains(Seq(ResolvedColumn("id", false), ResolvedColumn("name", false))))
    assert(
      ResolverUtils.resolve(spark, Seq("unknown", "name"), df.queryExecution.analyzed).isEmpty)
    assert(
      ResolverUtils
        .resolve(spark, Seq.empty[String], df.queryExecution.analyzed)
        .contains(Seq.empty[ResolvedColumn]))
  }

  test("Verify testResolve against dataframe - case sensitiveness false") {
    import spark.implicits._

    val coll = Seq((1, "a", "a2"))
    val df = coll.toDF("Id", "Name", "Other")

    assert(
      ResolverUtils
        .resolve(spark, Seq("iD", "nAme"), df.queryExecution.analyzed)
        .contains(Seq(ResolvedColumn("Id", false), ResolvedColumn("Name", false))))
  }

  test("Verify testResolve against dataframe - case sensitiveness true") {
    import spark.implicits._

    val coll = Seq((1, "a", "a2"))
    val df = coll.toDF("Id", "Name", "Other")

    val prevCaseSensitivity = spark.conf.get("spark.sql.caseSensitive")
    spark.conf.set("spark.sql.caseSensitive", "true")
    assert(ResolverUtils.resolve(spark, Seq("iD", "nAme"), df.queryExecution.analyzed).isEmpty)
    spark.conf.set("spark.sql.caseSensitive", prevCaseSensitivity)
  }

  test("Verify testResolve against dataframe - nested") {
    import spark.implicits._

    val coll =
      Seq((1, "a", NType2("n1", NType3("m1", NType4("o1", NType("p1", 1L))))))
    val df = coll.toDF("id", "nm", "nested")

    assert(
      ResolverUtils
        .resolve(spark, Seq("id", "nm"), df.queryExecution.analyzed)
        .contains(Seq(ResolvedColumn("id", false), ResolvedColumn("nm", false))))
    assert(
      ResolverUtils
        .resolve(
          spark,
          Seq("nm", "nested.n.n.n.f2", "nested.n.n.nf1_b", "nested.nf1"),
          df.queryExecution.analyzed)
        .contains(
          Seq(
            ResolvedColumn("nm", false),
            ResolvedColumn("nested.n.n.n.f2", true),
            ResolvedColumn("nested.n.n.nf1_b", true),
            ResolvedColumn("nested.nf1", true))))
    assert(
      ResolverUtils
        .resolve(
          spark,
          Seq("nm", "nested", "nested.n", "nested.n.n", "nested.nf1"),
          df.queryExecution.analyzed)
        .contains(Seq(
          ResolvedColumn("nm", false),
          ResolvedColumn("nested", false),
          ResolvedColumn("nested.n", true),
          ResolvedColumn("nested.n.n", true),
          ResolvedColumn("nested.nf1", true))))
  }

  test("Verify testResolve against dataframe - unsupported nested field names") {
    import spark.implicits._

    val coll = Seq((1, "a", NType5("m1", "s1")))
    val df = coll.toDF("id", "nm", "nested")

    assert(
      ResolverUtils
        .resolve(spark, Seq("id", "nm", "nested.n__y"), df.queryExecution.analyzed)
        .contains(
          Seq(
            ResolvedColumn("id", false),
            ResolvedColumn("nm", false),
            ResolvedColumn("nested.n__y", true))))
    val exc = intercept[HyperspaceException] {
      ResolverUtils.resolve(spark, Seq("nm", "nested.`nf9.x`"), df.queryExecution.analyzed)
    }
    assert(
      exc.getMessage.contains("Hyperspace does not support the nested column whose name " +
        "contains dots: nested.`nf9.x`"))
  }

  test("Verify testResolve against dataframe - unsupported nested array types") {
    import spark.implicits._

    val coll = Seq((1, "a", NType7("f1", Seq[NType](NType("ff1", 11L)))))
    val df = coll.toDF("id", "nm", "nested")
    val exc = intercept[HyperspaceException] {
      ResolverUtils.resolve(spark, Seq("nested.arr.f1"), df.queryExecution.analyzed)
    }
    assert(exc.getMessage.contains("Array types are not supported."))
  }

  test("Verify testResolve against dataframe - unsupported nested map types") {
    import spark.implicits._

    val coll =
      Seq((1, "a", NType8("f1", Map[NType, NType](NType("k1", 110L) -> NType("v1", 111L)))))
    val df = coll.toDF("id", "nm", "nested")
    val exc = intercept[HyperspaceException] {
      ResolverUtils.resolve(spark, Seq("nested.maps.value.f1"), df.queryExecution.analyzed)
    }
    assert(exc.getMessage.contains("Map types are not supported."))
    val exc2 = intercept[HyperspaceException] {
      ResolverUtils.resolve(spark, Seq("nested.maps.keys.f1"), df.queryExecution.analyzed)
    }
    assert(exc2.getMessage.contains("Map types are not supported."))
  }

  test("Verify ResolvedColumn functionality") {
    val col1 = ResolvedColumn("a.b.c", isNested = false)
    assert(col1.name == "a.b.c")
    assert(col1.normalizedName == "a.b.c")
    assert(col1 === ResolvedColumn(col1.normalizedName))
    // toString() calls toPrettySQL() internally and this verifies we are creating the
    // `Column` object correctly.
    assert(col1.toColumn.toString === "`a.b.c`")
    assert(col1.toNormalizedColumn.toString === "`a.b.c`")

    val col2 = ResolvedColumn("abc", isNested = false)
    assert(col2.name == "abc")
    assert(col2.normalizedName == "abc")
    assert(col2 === ResolvedColumn(col2.normalizedName))
    assert(col2.toColumn.toString === "abc")
    assert(col2.toNormalizedColumn.toString === "abc")

    val col3 = ResolvedColumn("a.b.c", isNested = true)
    assert(col3.name == "a.b.c")
    assert(col3.normalizedName == "__hs_nested.a.b.c")
    assert(col3 === ResolvedColumn(col3.normalizedName))
    assert(col3.toColumn.toString === "a.b.c AS `__hs_nested.a.b.c`")
    assert(col3.toNormalizedColumn.toString === "`__hs_nested.a.b.c`")
  }
}

case class NType8(f1: String, maps: Map[NType, NType])
case class NType7(f1: String, arr: Seq[NType])
case class NType6(`nf.1`: String, `n.2`: NType5)
case class NType5(`nf9.x`: String, n__y: String)
case class NType4(nf1_b: String, n: NType)
case class NType3(nf_a: String, n: NType4)
case class NType2(nf1: String, n: NType3)
case class NType(f1: String, f2: Long)
