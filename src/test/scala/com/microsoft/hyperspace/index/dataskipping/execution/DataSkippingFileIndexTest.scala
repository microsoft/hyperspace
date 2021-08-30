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

package com.microsoft.hyperspace.index.dataskipping.execution

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.datasources._
import org.apache.spark.sql.types._
import org.mockito.Mockito.{mock, verify, when}

import com.microsoft.hyperspace.index.dataskipping._
import com.microsoft.hyperspace.index.dataskipping.sketches._

class DataSkippingFileIndexTest extends DataSkippingSuite {
  test("DataSkippingFileIndex delegates methods to the FileIndex it is based on.") {
    val baseFileIndex = mock(classOf[FileIndex])
    val dsFileIndex = new DataSkippingFileIndex(
      spark,
      fileIdTracker,
      spark.emptyDataFrame,
      Literal.TrueLiteral,
      baseFileIndex)
    when(baseFileIndex.rootPaths).thenReturn(Seq(new Path("x")))
    assert(dsFileIndex.rootPaths === Seq(new Path("x")))
    when(baseFileIndex.inputFiles).thenReturn(Array("x/a", "x/b"))
    assert(dsFileIndex.inputFiles === Array("x/a", "x/b"))
    dsFileIndex.refresh()
    verify(baseFileIndex).refresh()
    when(baseFileIndex.sizeInBytes).thenReturn(12345)
    assert(dsFileIndex.sizeInBytes === 12345)
    val structType = StructType(StructField("A", IntegerType) :: Nil)
    when(baseFileIndex.partitionSchema).thenReturn(structType)
    assert(dsFileIndex.partitionSchema === structType)
    when(baseFileIndex.metadataOpsTimeNs).thenReturn(Some(100L))
    assert(dsFileIndex.metadataOpsTimeNs === Some(100L))
  }

  test("listFiles returns partition directories with filtered files.") {
    val dsFileIndex = createDataSkippingFileIndex(
      spark.range(100).toDF("A"),
      MinMaxSketch("A"),
      LessThanOrEqual(UnresolvedAttribute("MinMax_A__0"), Literal(1)))
    val selectedPartitions = dsFileIndex.listFiles(Nil, Nil)
    val allPartitions = dsFileIndex.baseFileIndex.listFiles(Nil, Nil)
    assert(partitionsSize(selectedPartitions) < partitionsSize(allPartitions))
    assert(partitionsContain(allPartitions, selectedPartitions))
  }

  def createDataSkippingFileIndex(
      df: DataFrame,
      sketch: Sketch,
      indexDataPred: Expression): DataSkippingFileIndex = {
    val sourceData = createSourceData(df)
    val baseFileIndex = sourceData.queryExecution.optimizedPlan.collectFirst {
      case LogicalRelation(HadoopFsRelation(location, _, _, _, _, _), _, _, _) => location
    }.get
    val (index, indexData) =
      DataSkippingIndexConfig("myind", sketch).createIndex(ctx, sourceData, Map.empty)
    new DataSkippingFileIndex(spark, fileIdTracker, indexData, indexDataPred, baseFileIndex)
  }

  def partitionsSize(partitions: Seq[PartitionDirectory]): Long = {
    partitions.flatMap(_.files.map(_.getLen)).sum
  }

  def partitionsContain(
      partitions: Seq[PartitionDirectory],
      others: Seq[PartitionDirectory]): Boolean = {
    val partitionsMap = partitions.map(pd => (pd.values, pd.files)).toMap
    others.forall { pd =>
      val files = partitionsMap.getOrElse(pd.values, Nil).toSet
      pd.files.forall(f => files.contains(f))
    }
  }
}
