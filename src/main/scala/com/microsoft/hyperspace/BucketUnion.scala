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

import scala.reflect.ClassTag

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{SparkPlan, SparkStrategy}

/*
  For now, these bucket union plan should be internal use only.
 */
private[hyperspace]
class BucketAwareUnionRDDPartition(@transient val rdds: Seq[RDD[_]], override val index: Int)
    extends Partition {
  var parents: Array[Partition] = rdds.map(_.partitions(index)).toArray

  override def hashCode(): Int = index

  override def equals(other: Any): Boolean = super.equals(other)

  /*
  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent partition at the time of task serialization
    parents = rdds.map(_.partitions(index)).toArray
    oos.defaultWriteObject()
  }
   */
}

private[hyperspace]
class BucketAwareUnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]],
    bucketSpec: BucketSpec)
    extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
  require(rdds.nonEmpty)
  require(rdds.forall(_.getNumPartitions == bucketSpec.numBuckets))

  override def getPartitions: Array[Partition] = {
    val numBuckets = bucketSpec.numBuckets
    (0 until numBuckets).map { index =>
      new BucketAwareUnionRDDPartition(rdds, index)
    }.toArray
  }

  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[BucketAwareUnionRDDPartition].parents
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context)
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}

private[hyperspace]
case class BucketUnionLogicalPlan(children: Seq[LogicalPlan], bucketSpec: BucketSpec)
    extends LogicalPlan {
  require(children.forall(_.output.equals(children.head.output)))
  override def output: Seq[Attribute] = children.head.output

  // from Spark code
  override def maxRows: Option[Long] = {
    if (children.exists(_.maxRows.isEmpty)) {
      None
    } else {
      Some(children.flatMap(_.maxRows).sum)
    }
  }

  // from Spark code
  override def maxRowsPerPartition: Option[Long] = {
    if (children.exists(_.maxRowsPerPartition.isEmpty)) {
      None
    } else {
      Some(children.flatMap(_.maxRowsPerPartition).sum)
    }
  }

  // from Spark code
  override lazy val resolved: Boolean = {
    // allChildrenCompatible needs to be evaluated after childrenResolved
    def allChildrenCompatible: Boolean =
      children.tail.forall(
        child =>
          // compare the attribute number with the first child
          child.output.length == children.head.output.length &&
            // compare the data types with the first child
            child.output.zip(children.head.output).forall {
              case (l, r) => l.dataType.equals(r.dataType)
          })
    children.length > 1 && childrenResolved && allChildrenCompatible
  }
}

private[hyperspace]
object BucketUnionStrategy extends SparkStrategy {
  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case bucketUnion: BucketUnionLogicalPlan =>
      BucketUnionExecutorExec(
        bucketUnion.children.map(p => planLater(p)),
        bucketUnion.bucketSpec) :: Nil
    case _ => Nil
  }
}

private[hyperspace]
case class BucketUnionExecutorExec(children: Seq[SparkPlan], bucketSpec: BucketSpec)
    extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    new BucketAwareUnionRDD[InternalRow](sparkContext, children.map(_.execute()), bucketSpec)
  }

  override def output: Seq[Attribute] = children.head.output
  override def outputPartitioning: Partitioning = children.head.outputPartitioning
  // override def outputOrdering: Seq[SortOrder] = children.head.outputOrdering
}
