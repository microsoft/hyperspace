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

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, EmptyRow, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{BucketingUtils, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.types.{DoubleType, FloatType}
import org.apache.spark.util.collection.BitSet

import com.microsoft.hyperspace.index.IndexLogEntry

/**
 * Utility functions for logical plan.
 */
object LogicalPlanUtils {

  /**
   * Check if a logical plan is a LogicalRelation.
   * @param logicalPlan logical plan to check.
   * @return true if a logical plan is a LogicalRelation or false.
   */
  def isLogicalRelation(logicalPlan: LogicalPlan): Boolean = {
    logicalPlan match {
      case _: LogicalRelation => true
      case _ => false
    }
  }

  /**
   * BucketSelector returns the selected buckets if bucket pruning is applicable for the given
   * query plan. The logic is extracted from [[FileSourceScanStrategy]] in Spark.
   */
  object BucketSelector {
    // should prune buckets iff num buckets is greater than 1 and there is only one bucket column
    private def shouldPruneBuckets(spec: BucketSpec): Boolean = {
      spec.bucketColumnNames.length == 1 && spec.numBuckets > 1
    }

    private def getExpressionBuckets(
        expr: Expression,
        bucketColumnName: String,
        numBuckets: Int): BitSet = {

      def getBucketNumber(attr: Attribute, v: Any): Int = {
        BucketingUtils.getBucketIdFromValue(attr, numBuckets, v)
      }

      def getBucketSetFromIterable(attr: Attribute, iter: Iterable[Any]): BitSet = {
        val matchedBuckets = new BitSet(numBuckets)
        iter
          .map(v => getBucketNumber(attr, v))
          .foreach(bucketNum => matchedBuckets.set(bucketNum))
        matchedBuckets
      }

      def getBucketSetFromValue(attr: Attribute, v: Any): BitSet = {
        val matchedBuckets = new BitSet(numBuckets)
        matchedBuckets.set(getBucketNumber(attr, v))
        matchedBuckets
      }

      expr match {
        case expressions.Equality(a: Attribute, Literal(v, _)) if a.name == bucketColumnName =>
          getBucketSetFromValue(a, v)
        case expressions.In(a: Attribute, list)
            if list.forall(_.isInstanceOf[Literal]) && a.name == bucketColumnName =>
          getBucketSetFromIterable(a, list.map(e => e.eval(EmptyRow)))
        case expressions.InSet(a: Attribute, hset)
            if hset.forall(_.isInstanceOf[Literal]) && a.name == bucketColumnName =>
          // NOTE: Spark bug - https://issues.apache.org/jira/browse/SPARK-33372
          // Bucket pruning is not applied for InSet without the fix.
          getBucketSetFromIterable(a, hset)
        case expressions.IsNull(a: Attribute) if a.name == bucketColumnName =>
          getBucketSetFromValue(a, null)
        case expressions.IsNaN(a: Attribute)
            if a.name == bucketColumnName && a.dataType == FloatType =>
          getBucketSetFromValue(a, Float.NaN)
        case expressions.IsNaN(a: Attribute)
            if a.name == bucketColumnName && a.dataType == DoubleType =>
          getBucketSetFromValue(a, Double.NaN)
        case expressions.And(left, right) =>
          getExpressionBuckets(left, bucketColumnName, numBuckets) &
            getExpressionBuckets(right, bucketColumnName, numBuckets)
        case expressions.Or(left, right) =>
          getExpressionBuckets(left, bucketColumnName, numBuckets) |
            getExpressionBuckets(right, bucketColumnName, numBuckets)
        case _ =>
          val matchedBuckets = new BitSet(numBuckets)
          matchedBuckets.setUntil(numBuckets)
          matchedBuckets
      }
    }

    private def genBucketSet(
        normalizedFilters: Seq[Expression],
        bucketSpec: BucketSpec): Option[BitSet] = {
      if (normalizedFilters.isEmpty) {
        return None
      }

      val bucketColumnName = bucketSpec.bucketColumnNames.head
      val numBuckets = bucketSpec.numBuckets

      val normalizedFiltersAndExpr = normalizedFilters
        .reduce(expressions.And)
      val matchedBuckets =
        getExpressionBuckets(normalizedFiltersAndExpr, bucketColumnName, numBuckets)

      val numBucketsSelected = matchedBuckets.cardinality()

      // None means all the buckets need to be scanned
      if (numBucketsSelected == numBuckets) {
        None
      } else {
        Some(matchedBuckets)
      }
    }

    def apply(plan: LogicalPlan, bucketSpec: BucketSpec): Option[BitSet] = plan match {
      case PhysicalOperation(
          projects,
          filters,
          l @ LogicalRelation(fsRelation: HadoopFsRelation, _, _, _)) =>
        // The attribute name of predicate could be different than the one in schema in case of
        // case insensitive, we should change them to match the one in schema, so we do not need to
        // worry about case sensitivity anymore.
        val normalizedFilters = filters.map { e =>
          e transform {
            case a: AttributeReference =>
              a.withName(l.output.find(_.semanticEquals(a)).get.name)
          }
        }
        // subquery expressions are filtered out because they can't be used to prune buckets or
        // pushed down as data filters, yet they would be executed
        val normalizedFiltersWithoutSubqueries =
          normalizedFilters.filterNot(SubqueryExpression.hasSubquery)

        val bucketSet = if (shouldPruneBuckets(bucketSpec)) {
          genBucketSet(normalizedFiltersWithoutSubqueries, bucketSpec)
        } else {
          None
        }
        bucketSet
      case _ => None
    }
  }
}
