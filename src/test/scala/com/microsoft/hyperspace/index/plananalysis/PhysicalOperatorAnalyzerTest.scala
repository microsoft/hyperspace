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

package com.microsoft.hyperspace.index.plananalysis

import org.apache.spark.SparkFunSuite
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.{Distribution, Partitioning, UnknownPartitioning}
import org.apache.spark.sql.execution.SparkPlan

import com.microsoft.hyperspace.SparkInvolvedSuite

class PhysicalOperatorAnalyzerTest extends SparkFunSuite with SparkInvolvedSuite {
  test("Two plans are the same") {
    val plan = DummySparkPlan("plan1", Seq(DummySparkPlan("plan1"), DummySparkPlan("plan2")))
    runPhysicalOperatorComparisonTest(
      plan,
      plan,
      Seq(PhysicalOperatorComparison("plan1", 2, 2), PhysicalOperatorComparison("plan2", 1, 1)))
  }

  test("Left plan has more physical plans") {
    runPhysicalOperatorComparisonTest(
      DummySparkPlan("plan1", Seq(DummySparkPlan("plan1"), DummySparkPlan("plan2"))),
      DummySparkPlan("plan1", Seq(DummySparkPlan("plan1"))),
      Seq(PhysicalOperatorComparison("plan1", 2, 2), PhysicalOperatorComparison("plan2", 1, 0)))
  }

  test("Right plan has more physical plans") {
    runPhysicalOperatorComparisonTest(
      DummySparkPlan("plan1", Seq(DummySparkPlan("plan1"))),
      DummySparkPlan("plan1", Seq(DummySparkPlan("plan1"), DummySparkPlan("plan2"))),
      Seq(PhysicalOperatorComparison("plan1", 2, 2), PhysicalOperatorComparison("plan2", 0, 1)))
  }

  private def runPhysicalOperatorComparisonTest(
      left: SparkPlan,
      right: SparkPlan,
      expected: Seq[PhysicalOperatorComparison]): Unit = {
    val analyzer = new PhysicalOperatorAnalyzer()
    val stat = analyzer.analyze(left, right)
    assert(stat.comparisonStats.sortBy(_.name) === expected)
  }
}

private case class DummySparkPlan(
    name: String = "",
    override val children: Seq[SparkPlan] = Nil,
    override val outputOrdering: Seq[SortOrder] = Nil,
    override val outputPartitioning: Partitioning = UnknownPartitioning(0),
    override val requiredChildDistribution: Seq[Distribution] = Nil,
    override val requiredChildOrdering: Seq[Seq[SortOrder]] = Nil)
    extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = throw new NotImplementedError
  override def output: Seq[Attribute] = Seq.empty
  override def nodeName: String = if (name.isEmpty) super.nodeName else name
}
