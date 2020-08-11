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

package com.microsoft.hyperspace.index.rules

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.{JoinType, SQLHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType}

import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.util.FileUtils

class JoinIndexRuleTest extends HyperspaceRuleTestSuite with SQLHelper {
  override val systemPath = new Path("src/test/resources/joinIndexRuleTest")

  val t1c1 = AttributeReference("t1c1", IntegerType)()
  val t1c2 = AttributeReference("t1c2", StringType)()
  val t1c3 = AttributeReference("t1c3", IntegerType)()
  val t1c4 = AttributeReference("t1c4", StringType)()
  val t2c1 = AttributeReference("t2c1", IntegerType)()
  val t2c2 = AttributeReference("t2c2", StringType)()
  val t2c3 = AttributeReference("t2c3", IntegerType)()
  val t2c4 = AttributeReference("t2c4", StringType)()

  val t1Schema = schemaFromAttributes(t1c1, t1c2, t1c3, t1c4)
  val t2Schema = schemaFromAttributes(t2c1, t2c2, t2c3, t2c4)

  var t1Relation: HadoopFsRelation = _
  var t2Relation: HadoopFsRelation = _
  var t1ScanNode: LogicalRelation = _
  var t2ScanNode: LogicalRelation = _
  var t1FilterNode: Filter = _
  var t2FilterNode: Filter = _
  var t1ProjectNode: Project = _
  var t2ProjectNode: Project = _

  /**
   * Test Setup:
   *
   * The basic scenario tested here is a [[Join]] logical plan node which consists of two children
   * Left child is a [[Project]] -> [[Filter]] -> [[LogicalRelation]] subplan which reads data
   * from files on disk.
   * Right child is also a [[Project]] -> [[Filter]] -> [[LogicalRelation]] subplan same as left.
   *
   * If the Join node satisfies the requirements for [[JoinIndexRule]], the plan must get updated
   * to use available indexes from the system. If not, the plan should remain unaffected on
   * application of the rule.
   */
  override def beforeAll(): Unit = {
    super.beforeAll()

    val t1Location =
      new InMemoryFileIndex(spark, Seq(new Path("t1")), Map.empty, Some(t1Schema), NoopCache)
    val t2Location =
      new InMemoryFileIndex(spark, Seq(new Path("t2")), Map.empty, Some(t2Schema), NoopCache)

    t1Relation = baseRelation(t1Location, t1Schema)
    t2Relation = baseRelation(t2Location, t2Schema)

    t1ScanNode = LogicalRelation(t1Relation, Seq(t1c1, t1c2, t1c3, t1c4), None, false)
    t2ScanNode = LogicalRelation(t2Relation, Seq(t2c1, t2c2, t2c3, t2c4), None, false)

    t1FilterNode = Filter(IsNotNull(t1c1), t1ScanNode)
    t2FilterNode = Filter(IsNotNull(t2c1), t2ScanNode)

    t1ProjectNode = Project(Seq(t1c1, t1c3), t1FilterNode)
    // Project [t1c1#0, t1c3#2]
    //  +- Filter isnotnull(t1c1#0)
    //   +- Relation[t1c1#0,t1c2#1,t1c3#2,t1c4#3] parquet

    t2ProjectNode = Project(Seq(t2c1, t2c3), t2FilterNode)
    // Project [t2c1#4, t2c3#6]
    //  +- Filter isnotnull(t2c1#4)
    //   +- Relation[t2c1#4,t2c2#5,t2c3#6,t2c4#7] parquet

    createIndex("t1i1", Seq(t1c1), Seq(t1c3), t1ProjectNode)
    createIndex("t1i2", Seq(t1c1, t1c2), Seq(t1c3), t1ProjectNode)
    createIndex("t1i3", Seq(t1c2), Seq(t1c3), t1ProjectNode)
    createIndex("t2i1", Seq(t2c1), Seq(t2c3), t2ProjectNode)
    createIndex("t2i2", Seq(t2c1, t2c2), Seq(t2c3), t2ProjectNode)
  }

  before {
    clearCache()
  }

  test("Join rule works if indexes exist and configs are set correctly") {
    val joinCondition = EqualTo(t1c1, t2c1)
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(!updatedPlan.equals(originalPlan))

    val indexPaths = Seq(getIndexDataFilesPath("t1i1"), getIndexDataFilesPath("t2i1"))
    verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths)
  }

  test("Join rule works if indexes exist for case insensitive index and query") {
    val t1c1Caps = t1c1.withName("T1C1")

    val joinCondition = EqualTo(t1c1Caps, t2c1)
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(!updatedPlan.equals(originalPlan))

    val indexPaths = Seq(getIndexDataFilesPath("t1i1"), getIndexDataFilesPath("t2i1"))
    verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths)
  }

  test("Join rule does not update plan if index location is not set") {
    withSQLConf(IndexConstants.INDEX_SYSTEM_PATH -> "") {
      val joinCondition = EqualTo(t1c1, t2c1)
      val originalPlan =
        Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
      val updatedPlan = JoinIndexRule(originalPlan)
      assert(updatedPlan.equals(originalPlan))
    }
  }

  test("Join rule does not update plan if join condition does not exist") {
    val originalPlan = Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), None, JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(updatedPlan.equals(originalPlan))
  }

  test("Join rule does not update plan if join condition is not equality") {
    val joinCondition = GreaterThan(t1c1, t2c1)
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(updatedPlan.equals(originalPlan))
  }

  test("Join rule does not update plan if join condition contains And or Or") {
    val joinCondition = And(EqualTo(t1c1, t2c1), EqualTo(t1c2, t2c2))
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(updatedPlan.equals(originalPlan))
  }

  test("Join rule does not update plan if join condition contains Literals") {
    val joinCondition = EqualTo(t1c2, Literal(10, IntegerType))
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(updatedPlan.equals(originalPlan))
  }

  test("Join rule does not update plan if index doesn't exist for either table") {
    val t1FilterNode = Filter(IsNotNull(t1c2), t1ScanNode)
    val t2FilterNode = Filter(IsNotNull(t2c2), t2ScanNode)

    val t1ProjectNode = Project(Seq(t1c2, t1c3), t1FilterNode)
    val t2ProjectNode = Project(Seq(t2c2, t2c3), t2FilterNode)

    // Index exists with t1c2 as indexed columns but not for t2c2. Plan should not update
    val joinCondition = EqualTo(t1c2, t2c2)
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(updatedPlan.equals(originalPlan))
  }

  test("Join rule does not update plan if index doesn't satisfy included columns from any side") {
    val t1FilterNode = Filter(IsNotNull(t1c1), t1ScanNode)
    val t2FilterNode = Filter(IsNotNull(t2c1), t2ScanNode)

    val t1ProjectNode = Project(Seq(t1c1, t1c4), t1FilterNode)
    val t2ProjectNode = Project(Seq(t2c1, t2c4), t2FilterNode)
    val joinCondition = EqualTo(t1c1, t2c1)
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    // The plan requires t1c4 and t4c4 columns for projection. These columns are not part of any
    // index. Since no index satisfies the requirement, the plan should not change.

    val updatedPlan = JoinIndexRule(originalPlan)
    assert(updatedPlan.equals(originalPlan))
  }

  test("Join rule correctly handles implicit output columns") {
    val t1FilterNode = Filter(IsNotNull(t1c1), t1ScanNode)
    val t2FilterNode = Filter(IsNotNull(t2c1), t2ScanNode)
    val joinCondition = EqualTo(t1c1, t2c1)

    // Implicit output set of columns containing all of the following columns
    // t1c1, t1c2, t1c3, t1c4, t2c1, t2c2, t2c3, t2c4.
    // The below query is same as
    // SELECT * FROM T1, T2 WHERE T1.C1 = T2.C1
    val originalPlan =
      Join(t1FilterNode, t2FilterNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)

    {
      // Test: should not update plan if no index exist to cover all implicit columns
      val updatedPlan = JoinIndexRule(originalPlan)
      assert(updatedPlan.equals(originalPlan))
    }

    {
      // Test: should update plan if index exists to cover all implicit columns
      val t1TestIndex = createIndex("t1Idx", Seq(t1c1), Seq(t1c2, t1c3, t1c4), t1FilterNode)
      val t2TestIndex = createIndex("t2Idx", Seq(t2c1), Seq(t2c2, t2c3, t2c4), t2FilterNode)

      // clear cache so the new indexes gets added to it
      clearCache()

      val updatedPlan = JoinIndexRule(originalPlan)
      assert(!updatedPlan.equals(originalPlan))

      val indexPaths = Seq(getIndexDataFilesPath("t1Idx"), getIndexDataFilesPath("t2Idx"))
      verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths)

      // Cleanup created indexes after test
      FileUtils.delete(getIndexRootPath(t1TestIndex.name))
      FileUtils.delete(getIndexRootPath(t2TestIndex.name))
    }
  }

  test("Join rule does not update plan if join condition contains aliased column names") {
    val t1c1Alias = Alias(t1c1, "t1c1Alias")()
    val t1ProjectNode = Project(Seq(t1c1Alias, t1c3), t1FilterNode)

    val joinCondition = EqualTo(t1c1Alias.toAttribute, t2c1)
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(updatedPlan.equals(originalPlan))
  }

  test(
    "Join rule does not update plan if join condition contains columns from " +
      "non-LogicalRelation leaf nodes") {
    // Creating a LocalRelation for join
    val localCol1 = AttributeReference("lc1", IntegerType)()
    val localCol2 = AttributeReference("lc2", StringType)()
    val localData: Seq[Row] = Seq((1, "a"), (2, "b"), (3, "c")).map(Row(_))
    val localRelation: LocalRelation =
      LocalRelation.fromExternalRows(Seq(localCol1, localCol2), localData)
    val localFilterNode = Filter(IsNotNull(localCol1), localRelation)
    val localProjectNode = Project(Seq(localCol1, localCol2), localFilterNode)

    // Here, join condition contains a column from a LocalRelation and one from a LogicalRelation
    val joinCondition = EqualTo(t1c1, localCol1)
    val originalPlan =
      Join(t1ProjectNode, localProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)

    val updatedPlan = JoinIndexRule(originalPlan)
    assert(updatedPlan.equals(originalPlan))
  }

  test("Join rule updates plan for composite query (AND based Equi-Join)") {
    val t1ProjectNode = Project(Seq(t1c1, t1c2, t1c3), t1FilterNode)
    val t2ProjectNode = Project(Seq(t2c1, t2c2, t2c3), t2FilterNode)

    // SELECT t1c1, t1c2, t1c3, t2c1, t2c2, t2c3
    // FROM t1, t2
    // WHERE t1c1 = t2c1 and t1c2 = t2c2
    val joinCondition = And(EqualTo(t1c1, t2c1), EqualTo(t1c2, t2c2))
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(!updatedPlan.equals(originalPlan))

    val indexPaths = Seq(getIndexDataFilesPath("t1i2"), getIndexDataFilesPath("t2i2"))
    verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths)
  }

  test("Join rule updates plan for composite query with order of predicates changed") {
    val t1ProjectNode = Project(Seq(t1c1, t1c2, t1c3), t1FilterNode)
    val t2ProjectNode = Project(Seq(t2c1, t2c2, t2c3), t2FilterNode)

    // SELECT t1c1, t1c2, t1c3, t2c1, t2c2, t2c3
    // FROM t1, t2
    // WHERE t1c2 = t2c2 and t1c1 = t2c1 >> order of predicates changed. The rule should make sure
    // if any usable index can be found irrespective of order of predicates
    val joinCondition = And(EqualTo(t1c2, t2c2), EqualTo(t1c1, t2c1))
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(!updatedPlan.equals(originalPlan))

    val indexPaths = Seq(getIndexDataFilesPath("t1i2"), getIndexDataFilesPath("t2i2"))
    verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths)
  }

  test("Join rule updates plan for composite query with swapped attributes") {
    val t1ProjectNode = Project(Seq(t1c1, t1c2, t1c3), t1FilterNode)
    val t2ProjectNode = Project(Seq(t2c1, t2c2, t2c3), t2FilterNode)

    // SELECT t1c1, t1c2, t1c3, t2c1, t2c2, t2c3
    // FROM t1, t2
    // WHERE t1c1 = t2c1 and t2c2 = t1c2 >> Swapped order of query columns
    val joinCondition = And(EqualTo(t1c1, t2c1), EqualTo(t2c2, t1c2))
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(!updatedPlan.equals(originalPlan))

    val indexPaths = Seq(getIndexDataFilesPath("t1i2"), getIndexDataFilesPath("t2i2"))
    verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths)
  }

  test("Join rule doesn't update plan if columns don't have one-to-one mapping") {
    val t1ProjectNode = Project(Seq(t1c1, t1c2, t1c3), t1FilterNode)
    val t2ProjectNode = Project(Seq(t2c1, t2c2, t2c3), t2FilterNode)

    {
      // SELECT t1c1, t1c2, t1c3, t2c1, t2c2, t2c3
      // FROM t1, t2
      // WHERE t1c1 = t2c1 and t1c1 = t2c2  >> t1c1 compared against both t2c1 and t2c2
      val joinCondition = And(EqualTo(t1c1, t2c1), EqualTo(t1c1, t2c2))
      val originalPlan =
        Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
      val updatedPlan = JoinIndexRule(originalPlan)
      assert(updatedPlan.equals(originalPlan))
    }
    {
      // SELECT t1c1, t1c2, t1c3, t2c1, t2c2, t2c3
      // FROM t1, t2
      // WHERE t1c1 = t2c1 and t1c2 = t2c1  >> t2c1 compared against both t1c1 and t1c2
      val joinCondition = And(EqualTo(t1c1, t2c1), EqualTo(t1c2, t2c1))
      val originalPlan =
        Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
      val updatedPlan = JoinIndexRule(originalPlan)
      assert(updatedPlan.equals(originalPlan))
    }
  }

  test(
    "Join rule updates plan if columns have one-to-one mapping with repeated " +
      "case-insensitive predicates") {
    val t1ProjectNode = Project(Seq(t1c1, t1c3), t1FilterNode)
    val t2ProjectNode = Project(Seq(t2c1, t2c3), t2FilterNode)

    val t1c1Caps = t1c1.withName("T1C1")
    val t2c1Caps = t2c1.withName("T2C1")

    val joinCondition = And(EqualTo(t1c1, t2c1), EqualTo(t1c1Caps, t2c1Caps))
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(!updatedPlan.equals(originalPlan))

    val indexPaths = Seq(getIndexDataFilesPath("t1i1"), getIndexDataFilesPath("t2i1"))
    verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths)
  }

  test("Join rule updates plan for composite query for repeated predicates") {
    val t1ProjectNode = Project(Seq(t1c1, t1c2, t1c3), t1FilterNode)
    val t2ProjectNode = Project(Seq(t2c1, t2c2, t2c3), t2FilterNode)

    // SELECT t1c1, t1c2, t1c3, t2c1, t2c2, t2c3
    // FROM t1, t2
    // WHERE t1c1 = t2c1 and t1c1 = t2c2 and t1c1 = t2c1 >> one predicate repeated twice
    val joinCondition = And(And(EqualTo(t1c1, t2c1), EqualTo(t1c2, t2c2)), EqualTo(t1c1, t2c1))
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(!updatedPlan.equals(originalPlan))

    val indexPaths = Seq(getIndexDataFilesPath("t1i2"), getIndexDataFilesPath("t2i2"))
    verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths)
  }

  test("Join rule doesn't update plan if columns don't belong to either side of join node") {
    val t1ProjectNode = Project(Seq(t1c1, t1c2, t1c3), t1FilterNode)
    val t2ProjectNode = Project(Seq(t2c1, t2c2, t2c3), t2FilterNode)

    // SELECT t1c1, t1c2, t1c3, t2c1, t2c2, t2c3
    // FROM t1, t2
    // WHERE t1c1 = t1c2 and t1c1 = t2c2  >> two columns of t1 compared against each other
    val joinCondition = And(EqualTo(t1c1, t1c2), EqualTo(t1c2, t2c2))
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(updatedPlan.equals(originalPlan))
  }

  test(
    "Join rule updates plan if condition attributes contain 'qualifier' " +
      "but base table attributes don't") {

    // Attributes same as the base data source, qualified with table names (e.g. from a table name
    // from the catalog)
    val t1c1Qualified = t1c1.copy()(t1c1.exprId, Seq("Table1"))
    val t2c1Qualified = t2c1.copy()(t2c1.exprId, Seq("Table2"))

    val joinCondition = EqualTo(t1c1Qualified, t2c1Qualified)
    val originalPlan =
      Join(t1ProjectNode, t2ProjectNode, JoinType("inner"), Some(joinCondition), JoinHint.NONE)
    val updatedPlan = JoinIndexRule(originalPlan)
    assert(!updatedPlan.equals(originalPlan))

    val indexPaths = Seq(getIndexDataFilesPath("t1i1"), getIndexDataFilesPath("t2i1"))
    verifyUpdatedIndex(originalPlan, updatedPlan, indexPaths)
  }

  private def verifyUpdatedIndex(
      originalPlan: Join,
      updatedPlan: LogicalPlan,
      indexPaths: Seq[Path]): Unit = {
    assert(treeStructureEquality(originalPlan, updatedPlan))
    assert(basePaths(updatedPlan) == indexPaths)
  }

  /** method to check if tree structures of two logical plans are the same. */
  private def treeStructureEquality(plan1: LogicalPlan, plan2: LogicalPlan): Boolean = {
    val originalNodeCount = plan1.treeString.split("\n").length
    val updatedNodeCount = plan2.treeString.split("\n").length

    if (originalNodeCount == updatedNodeCount) {
      (0 until originalNodeCount).forall { i =>
        plan1(i) match {
          // for LogicalRelation, we just check if the updated also has LogicalRelation. If the
          // updated plan uses index, the root paths will be different here
          case _: LogicalRelation => plan2(i).isInstanceOf[LogicalRelation]

          // for other node types, we compare exact matching between original and updated plans
          case node =>
            // The default is 25, and should be big enough for this purpose.
            val maxFields = SQLConf.get.maxToStringFields
            node.simpleString(maxFields).equals(plan2(i).simpleString(maxFields))
        }
      }
    } else {
      false
    }
  }

  /** Returns tuple of left and right base relation paths for a logical plan */
  private def basePaths(plan: LogicalPlan): Seq[Path] = {
    plan
      .collectLeaves()
      .collect {
        case LogicalRelation(HadoopFsRelation(location, _, _, Some(_), _, _), _, _, _) =>
          location.rootPaths
      }
      .flatten
  }
}
