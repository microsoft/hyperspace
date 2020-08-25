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

package com.microsoft.hyperspace.index.rankers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index._

class JoinIndexRankerTest extends SparkFunSuite {

  val t1c1 = AttributeReference("t1c1", IntegerType)()
  val t1c2 = AttributeReference("t1c2", StringType)()
  val t2c1 = AttributeReference("t2c1", IntegerType)()
  val t2c2 = AttributeReference("t2c2", StringType)()

  test("testRank: Prefer equal-bucket index pairs over unequal-bucket") {
    val l_10 = index("l1", Seq(t1c1), Seq(t1c2), 10)
    val l_20 = index("l2", Seq(t1c1), Seq(t1c2), 20)
    val r_20 = index("r1", Seq(t2c1), Seq(t2c2), 20)

    val indexPairs = Seq((l_10, r_20), (l_20, r_20))

    val expectedOrder = Seq((l_20, r_20), (l_10, r_20))
    val actualOrder = JoinIndexRanker.rank(indexPairs)
    assert(actualOrder.equals(expectedOrder))
  }

  test("testRank: Prefer higher number of buckets if multiple equal-bucket index pairs found") {
    val l_10 = index("l1", Seq(t1c1), Seq(t1c2), 10)
    val l_20 = index("l2", Seq(t1c1), Seq(t1c2), 20)
    val r_10 = index("r1", Seq(t2c1), Seq(t2c2), 10)
    val r_20 = index("r2", Seq(t2c1), Seq(t2c2), 20)

    val indexPairs = Seq((l_10, r_10), (l_10, r_20), (l_20, r_20))

    val expectedOrder = Seq((l_20, r_20), (l_10, r_10), (l_10, r_20))
    val actualOrder = JoinIndexRanker.rank(indexPairs)
    assert(actualOrder.equals(expectedOrder))
  }

  private def index(
      name: String,
      indexCols: Seq[AttributeReference],
      includedCols: Seq[AttributeReference],
      numBuckets: Int): IndexLogEntry = {
    val sourcePlanProperties = SparkPlan.Properties(
      Seq(),
      null,
      null,
      LogicalPlanFingerprint(
        LogicalPlanFingerprint.Properties(Seq(Signature("signClass", "sign(plan)")))))

    val entry = IndexLogEntry(
      name,
      CoveringIndex(
        CoveringIndex.Properties(
          CoveringIndex.Properties
            .Columns(indexCols.map(_.name), includedCols.map(_.name)),
          IndexLogEntry.schemaString(schemaFromAttributes(indexCols ++ includedCols: _*)),
          numBuckets)),
      NewContent(Directory(name)),
      Source(SparkPlan(sourcePlanProperties)),
      Map())
    entry.state = Constants.States.ACTIVE
    entry
  }

  private def schemaFromAttributes(attributes: Attribute*): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))
}
