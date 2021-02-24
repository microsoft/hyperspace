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

package com.microsoft.hyperspace

import org.apache.spark.sql.SparkSession

/**
 * Sample data for testing.
 */
object SampleNestedData {

  val testData = Seq(
    ("2017-09-03", "810a20a2baa24ff3ad493bfbf064569a", "donde", 2, 1000,
      SampleNestedDataStruct("id1", SampleNestedDataLeaf("leaf_id1", 1))),
    ("2017-09-03", "fd093f8a05604515957083e70cb3dceb", "facebook", 1, 3000,
      SampleNestedDataStruct("id1", SampleNestedDataLeaf("leaf_id1", 2))),
    ("2017-09-03", "af3ed6a197a8447cba8bc8ea21fad208", "facebook", 1, 3000,
      SampleNestedDataStruct("id2", SampleNestedDataLeaf("leaf_id7", 1))),
    ("2017-09-03", "975134eca06c4711a0406d0464cbe7d6", "facebook", 1, 4000,
      SampleNestedDataStruct("id2", SampleNestedDataLeaf("leaf_id7", 2))),
    ("2018-09-03", "e90a6028e15b4f4593eef557daf5166d", "ibraco", 2, 3000,
      SampleNestedDataStruct("id2", SampleNestedDataLeaf("leaf_id7", 5))),
    ("2018-09-03", "576ed96b0d5340aa98a47de15c9f87ce", "facebook", 2, 3000,
      SampleNestedDataStruct("id2", SampleNestedDataLeaf("leaf_id9", 1))),
    ("2018-09-03", "50d690516ca641438166049a6303650c", "ibraco", 2, 1000,
      SampleNestedDataStruct("id3", SampleNestedDataLeaf("leaf_id9", 10))),
    ("2019-10-03", "380786e6495d4cd8a5dd4cc8d3d12917", "facebook", 2, 3000,
      SampleNestedDataStruct("id4", SampleNestedDataLeaf("leaf_id9", 12))),
    ("2019-10-03", "ff60e4838b92421eafc3e6ee59a9e9f1", "miperro", 2, 2000,
      SampleNestedDataStruct("id5", SampleNestedDataLeaf("leaf_id9", 21))),
    ("2019-10-03", "187696fe0a6a40cc9516bc6e47c70bc1", "facebook", 4, 3000,
      SampleNestedDataStruct("id6", SampleNestedDataLeaf("leaf_id9", 22))))

  def save(
      spark: SparkSession,
      path: String,
      columns: Seq[String],
      partitionColumns: Option[Seq[String]] = None): Unit = {
    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(testData)
    ).toDF(columns: _*)
    partitionColumns match {
      case Some(pcs) =>
        df.write.partitionBy(pcs: _*).parquet(path)
      case None =>
        df.write.parquet(path)
    }
  }
}

case class SampleNestedDataStruct(id: String, leaf: SampleNestedDataLeaf)
case class SampleNestedDataLeaf(id: String, cnt: Int)
