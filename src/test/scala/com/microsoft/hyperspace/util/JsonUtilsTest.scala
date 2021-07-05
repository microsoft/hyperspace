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
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.covering.CoveringIndex

class JsonUtilsTest extends SparkFunSuite {
  test("Test for JsonUtils.") {
    val schema = StructType(
      Seq(
        StructField("id", IntegerType),
        StructField("name", StringType),
        StructField("school", StringType)))

    val sourcePlanProperties = SparkPlan.Properties(
      Seq(),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(Seq(Signature("signatureProvider", "dfSignature")))))

    val index = IndexLogEntry(
      "myIndex",
      CoveringIndex(Seq("id"), Seq("name", "school"), schema, 10, Map()),
      Content(Directory("path")),
      Source(SparkPlan(sourcePlanProperties)),
      Map())
    index.state = Constants.States.ACTIVE

    val deserializedIndex =
      JsonUtils.fromJson[IndexLogEntry](JsonUtils.toJson[IndexLogEntry](index))
    assert(deserializedIndex.equals(index))
  }
}
