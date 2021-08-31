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

package org.apache.spark.sql.hyperspace

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.StructType

package object utils {
  implicit class DataFrameUtils(df: DataFrame) {
    def showString(numRows: Int, truncate: Int, vertical: Boolean = false): String = {
      df.showString(numRows, truncate, vertical)
    }
  }

  def logicalPlanToDataFrame(spark: SparkSession, logicalPlan: LogicalPlan): DataFrame = {
    Dataset.ofRows(spark, logicalPlan)
  }

  implicit class StructTypeUtils(st: StructType) {
    // Expose package-private method
    def merge(that: StructType): StructType = st.merge(that)
    def sameType(that: StructType): Boolean = st.sameType(that)
  }
}
