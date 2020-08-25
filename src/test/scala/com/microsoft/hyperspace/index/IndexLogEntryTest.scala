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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import com.microsoft.hyperspace.util.JsonUtils

class IndexLogEntryTest extends SparkFunSuite {
  test("IndexLogEntry spec example") {
    val schemaString =
      """{\"type\":\"struct\",
          |\"fields\":[
          |{\"name\":\"RGUID\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}},
          |{\"name\":\"Date\",\"type\":\"string\",\"nullable\":true,\"metadata\":{}}]}
          |""".stripMargin.replaceAll("\r", "").replaceAll("\n", "")

    val jsonString =
      s"""
        |{
        |  "name" : "indexName",
        |  "derivedDataset" : {
        |    "kind" : "CoveringIndex",
        |    "properties" : {
        |      "columns" : {
        |        "indexed" : [ "col1" ],
        |        "included" : [ "col2", "col3" ]
        |      },
        |      "schemaString" : "$schemaString",
        |      "numBuckets" : 200
        |    }
        |  },
        |  "content" : {
        |    "root" : "rootContentPath",
        |    "directories" : [ ]
        |  },
        |  "source" : {
        |    "plan" : {
        |      "kind" : "Spark",
        |      "properties" : {
        |        "relations" : [ {
        |          "rootPaths" : [ "rootpath" ],
        |          "options" : { },
        |          "data" : {
        |            "kind" : "HDFS",
        |            "properties" : {
        |              "content" : {
        |                "root" : "",
        |                "directories" : [ {
        |                  "path" : "",
        |                  "files" : [ {
        |                    "name" : "f1",
        |                    "size" : 100,
        |                    "modifiedTime" : 100
        |                  }, {
        |                    "name" : "f2",
        |                    "size" : 200,
        |                    "modifiedTime" : 200
        |                  } ],
        |                  "fingerprint" : {
        |                    "kind" : "NoOp",
        |                    "properties" : { }
        |                  }
        |                } ]
        |              }
        |            }
        |          },
        |          "dataSchemaJson" : "schema",
        |          "fileFormat" : "type"
        |          } ],
        |        "rawPlan" : null,
        |        "sql" : null,
        |        "fingerprint" : {
        |          "kind" : "LogicalPlan",
        |          "properties" : {
        |            "signatures" : [ {
        |              "provider" : "provider",
        |              "value" : "signatureValue"
        |            } ]
        |          }
        |        }
        |      }
        |    }
        |  },
        |  "extra" : { },
        |  "version" : "0.1",
        |  "id" : 0,
        |  "state" : "ACTIVE",
        |  "timestamp" : 1578818514080,
        |  "enabled" : true
        |}""".stripMargin

    val schema =
      StructType(Array(StructField("RGUID", StringType), StructField("Date", StringType)))

    val actual = JsonUtils.fromJson[IndexLogEntry](jsonString)

    val expectedSourcePlanProperties = SparkPlan.Properties(
      Seq(Relation(
        Seq("rootpath"),
        Hdfs(Hdfs.Properties(NewContent(
          Directory("", Seq(FileInfo1("f1", 100L, 100L), FileInfo1("f2", 200L, 200L)), Seq())))),
        "schema",
        "type",
        Map())),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(Seq(Signature("provider", "signatureValue")))))

    val expected = IndexLogEntry(
      "indexName",
      CoveringIndex(
        CoveringIndex.Properties(
          CoveringIndex.Properties
            .Columns(Seq("col1"), Seq("col2", "col3")),
          schema.json,
          200)),
      NewContent(Directory("rootContentPath")),
      Source(SparkPlan(expectedSourcePlanProperties)),
      Map())
    expected.state = "ACTIVE"
    expected.timestamp = 1578818514080L

    assert(actual.equals(expected))
  }
}
