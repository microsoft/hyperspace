/*
 * Copyright (2021) The Hyperspace Project Authors.
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

// To access package-private methods of RDD
package org.apache.spark.hyperspace

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.RDDInfo
import org.mockito.Mockito.{mock, spy, when}

object RDDTestUtils {
  def getMockDataFrameWithFakeSize(spark: SparkSession, size: Long): DataFrame = {
    val df = spy(spark.emptyDataFrame)
    val rdd = spy(df.rdd)
    val mockSparkContext = mock(classOf[SparkContext])
    val mockRddStorageInfo = mock(classOf[RDDInfo])
    when(df.rdd).thenReturn(rdd)
    when(rdd.id).thenReturn(42)
    when(rdd.context).thenReturn(mockSparkContext)
    when(mockSparkContext.getRDDStorageInfo).thenReturn(Array[RDDInfo](mockRddStorageInfo))
    when(mockRddStorageInfo.id).thenReturn(42)
    when(mockRddStorageInfo.memSize).thenReturn(size)
    df
  }
}
