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

package com.microsoft.hyperspace.index.plans.logical

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan

/**
 * [[BucketUnion]] is logical plan for Bucket-aware Union operation which retains
 * outputPartitioning of result RDDs so as to avoid performing unnecessary shuffle after
 * Union operation per bucket.
 *
 * @param children Child plans.
 * @param bucketSpec Bucket Specification.
 */
private[hyperspace] case class BucketUnion(children: Seq[LogicalPlan], bucketSpec: BucketSpec)
    extends LogicalPlan {
  require(resolved)
  override def output: Seq[Attribute] = children.head.output

  // copy from org.apache.spark.sql.catalyst.plans.logical.Union
  override def maxRows: Option[Long] = {
    if (children.exists(_.maxRows.isEmpty)) {
      None
    } else {
      Some(children.flatMap(_.maxRows).sum)
    }
  }

  // copy from org.apache.spark.sql.catalyst.plans.logical.Union
  override def maxRowsPerPartition: Option[Long] = {
    if (children.exists(_.maxRowsPerPartition.isEmpty)) {
      None
    } else {
      Some(children.flatMap(_.maxRowsPerPartition).sum)
    }
  }

  // copy from org.apache.spark.sql.catalyst.plans.logical.Union
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
