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

package com.microsoft.hyperspace.index.execution

import scala.reflect.ClassTag

import org.apache.spark.{OneToOneDependency, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.PartitionerAwareUnionRDD
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.SparkPlan

import com.microsoft.hyperspace.index.plans.logical.BucketUnion

// scalastyle:off filelinelengthchecker
/**
 * [[BucketAwareUnionRDD]] is required for index HybridScan to merge index data and
 * appended data without re-shuffling of index data. Spark does not support Union using
 * Partition Spec, but just [[PartitionerAwareUnionRDD]] operation and even it does not
 * retain outputPartitioning of result. So we define a new Union operation complying with
 * the following conditions.
 *   - input RDDs must have the same number of partitions.
 *   - input RDDs must have the same partitioning keys.
 *   - input RDDs must have the same column schema.
 *
 * Unfortunately, there is no explicit API for Partitioning keys in RDD, we have to
 * assure that on the caller side. Therefore, [[BucketAwareUnionRDD]] is Hyperspace
 * internal use only.
 *
 * You can find more detailed information about Bucketing optimization in
 * Bucketing 2.0: Improve Spark SQL Performance by Removing Shuffle
 * [[https://databricks.com/session_na20/bucketing-2-0-improve-spark-sql-performance-by-removing-shuffle
 Bucketing 2.0: Improve Spark SQL Performance by Removing Shuffle]]
 */
// scalastyle:on filelinelengthchecker
private[hyperspace] class BucketAwareUnionRDD[T: ClassTag](
    sc: SparkContext,
    var rdds: Seq[RDD[T]],
    bucketSpec: BucketSpec)
    extends RDD[T](sc, rdds.map(x => new OneToOneDependency(x))) {
  require(rdds.nonEmpty)
  require(rdds.forall(_.getNumPartitions == bucketSpec.numBuckets))

  // copy from org.apache.spark.rdd.PartitionerAwareUnionRDD
  override def getPartitions: Array[Partition] = {
    val numBuckets = bucketSpec.numBuckets
    (0 until numBuckets).map { index =>
      new BucketAwareUnionRDDPartition(rdds, index)
    }.toArray
  }

  // copy from org.apache.spark.rdd.PartitionerAwareUnionRDD
  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[BucketAwareUnionRDDPartition].parents
    rdds.zip(parentPartitions).iterator.flatMap {
      case (rdd, p) => rdd.iterator(p, context)
    }
  }

  // copy from org.apache.spark.rdd.PartitionerAwareUnionRDD
  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }
}

/**
 * [[BucketAwareUnionRDDPartition]] keeps partitions for each partition index.
 * @param rdds  Input RDDs.
 * @param index Partition index.
 */
private[hyperspace] class BucketAwareUnionRDDPartition(
    @transient val rdds: Seq[RDD[_]],
    override val index: Int)
    extends Partition {
  val parents: Array[Partition] = rdds.map(_.partitions(index)).toArray

  override def hashCode(): Int = index
  override def equals(other: Any): Boolean = super.equals(other)
}

/**
 * [[BucketUnionExec]] is Spark Plan for [[BucketUnion]].
 *
 * @param children Child plans.
 * @param bucketSpec Bucket specification.
 */
private[hyperspace] case class BucketUnionExec(children: Seq[SparkPlan], bucketSpec: BucketSpec)
    extends SparkPlan {
  override protected def doExecute(): RDD[InternalRow] = {
    new BucketAwareUnionRDD[InternalRow](sparkContext, children.map(_.execute()), bucketSpec)
  }

  override def output: Seq[Attribute] = children.head.output
  override def outputPartitioning: Partitioning = children.head.outputPartitioning
}
