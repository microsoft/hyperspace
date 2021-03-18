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
import org.apache.spark.rdd.{PartitionerAwareUnionRDD, RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.physical.{HashPartitioning, Partitioning}
import org.apache.spark.sql.execution.SparkPlan

import com.microsoft.hyperspace.index.plans.logical.BucketUnion

/**
 * [[BucketUnionRDD]] is required for the hybrid scan operation which merges index data and
 * appended data without re-shuffling the index data. Spark does not support Union that retains
 * output partition specification (i.e., using PartitionSpecification). The default operation
 * [[PartitionerAwareUnionRDD]] does not retain outputPartitioning of result i.e., even if both
 * sides are bucketed in a compatible way, it will cause a shuffle.
 *
 * To avoid these issues, we define a new BucketUnion operation that avoids a shuffle when
 * the following conditions are satisfied:
 *   - input RDDs must have the same number of partitions.
 *   - input RDDs must have the same partitioning keys.
 *   - input RDDs must have the same column schema.
 *
 * Unfortunately, since there is no explicit API to check Partitioning keys in RDD, we have to
 * asset the partitioning keys on the caller side. Therefore, [[BucketUnionRDD]] is Hyperspace
 * internal use only.
 *
 * You can find more detailed information about Bucketing optimization in:
 * ''Bucketing 2.0: Improve Spark SQL Performance by Removing Shuffle''
 * Video: [[https://youtu.be/7cvaH33S7uc ]]
 */
private[hyperspace] class BucketUnionRDD[T: ClassTag](
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
      new BucketUnionRDDPartition(rdds, index)
    }.toArray
  }

  // copy from org.apache.spark.rdd.PartitionerAwareUnionRDD
  override def compute(s: Partition, context: TaskContext): Iterator[T] = {
    val parentPartitions = s.asInstanceOf[BucketUnionRDDPartition].parents
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
 * [[BucketUnionRDDPartition]] keeps partitions for each partition index.
 * @param rdds  Input RDDs.
 * @param index Partition index.
 */
private[hyperspace] class BucketUnionRDDPartition(
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
    new BucketUnionRDD[InternalRow](sparkContext, children.map(_.execute()), bucketSpec)
  }

  override def output: Seq[Attribute] = children.head.output

  override def outputPartitioning: Partitioning = {
    val parts = children.map(_.outputPartitioning).distinct
    assert(parts.forall(_.isInstanceOf[HashPartitioning]))
    assert(parts.forall(_.numPartitions == bucketSpec.numBuckets))

    val reduced = parts.reduceLeft { (a, b) =>
      val h1 = a.asInstanceOf[HashPartitioning]
      val h2 = b.asInstanceOf[HashPartitioning]
      val h1Name = h1.references.head.name
      val h2Name = h2.references.head.name
      val same = h1Name.contains(h2Name) || h2Name.contains(h1Name)
      assert(same)
      if (h1Name.length > h2Name.length) {
        h1
      } else {
        h2
      }
    }
    reduced
  }
}
