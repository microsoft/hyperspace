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

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import com.microsoft.hyperspace.actions.Constants

@RunWith(classOf[JUnitRunner])
class IndexTests extends FunSuite {
  val indexConfig1 = IndexConfig("myIndex1", Array("id"), Seq("name"))
  val indexConfig2 = IndexConfig("myIndex2", Array("id"), Seq("school"))

  def toIndex(
      config: IndexConfig,
      path: String,
      schema: StructType,
      numBuckets: Int): IndexLogEntry = {
    val sourcePlanProperties = SparkPlan.Properties(
      "plan",
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(Seq(Signature("signatureProvider", "dfSignature")))))
    val sourceDataProperties =
      Hdfs.Properties(Content("", Seq(Content.Directory("", Seq(), NoOpFingerprint()))))

    val entry = IndexLogEntry(
      config.indexName,
      CoveringIndex(
        CoveringIndex.Properties(
          CoveringIndex.Properties
            .Columns(config.indexedColumns, config.includedColumns),
          IndexLogEntry.schemaString(schema),
          10)),
      Content(path, Seq()),
      Source(SparkPlan(sourcePlanProperties), Seq(Hdfs(sourceDataProperties))),
      Map())
    entry.state = Constants.States.ACTIVE
    entry
  }

  val index1 = toIndex(
    indexConfig1,
    "path1",
    StructType(Seq(StructField("id", IntegerType), StructField("name", StringType))),
    50)
  val index2 = toIndex(
    indexConfig2,
    "path2",
    StructType(Seq(StructField("id", IntegerType), StructField("school", StringType))),
    10)
  val index3 = toIndex(
    indexConfig1,
    "path1",
    StructType(Seq(StructField("id", IntegerType), StructField("name", StringType))),
    50)

  test("Test equals() function.") {
    assert(!index1.equals(AnyRef), "An Index must not be equal to AnyRef.")
    assert(!index1.equals(index2), "Indexes with different parameters must not be equal.")
    assert(index1.equals(index3), "Indexes with the same parameters must be equal.")
  }

  test("Test that if 2 Indexes are equal, their hash code must be equal.") {
    assert(index1.equals(index3))
    assert(index1.hashCode == index3.hashCode)
  }
}
