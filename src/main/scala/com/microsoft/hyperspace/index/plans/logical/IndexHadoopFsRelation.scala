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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.execution.datasources.{FileFormat, FileIndex, HadoopFsRelation}
import org.apache.spark.sql.types.StructType

import com.microsoft.hyperspace.index.IndexLogEntry

/**
 * Wrapper class of HadoopFsRelation to indicate index application more explicitly in Plan string.
 */
class IndexHadoopFsRelation(
    location: FileIndex,
    partitionSchema: StructType,
    dataSchema: StructType,
    bucketSpec: Option[BucketSpec],
    fileFormat: FileFormat,
    options: Map[String, String])(spark: SparkSession, index: IndexLogEntry)
    extends HadoopFsRelation(
      location,
      partitionSchema,
      dataSchema,
      bucketSpec,
      fileFormat,
      options)(spark) {

  val indexPlanStr: String = {
    s"Hyperspace(Type: ${index.derivedDataset.kindAbbr}, " +
      s"Name: ${index.name}, LogVersion: ${index.id})"
  }

  def indexName: String = index.name

  override def toString(): String = indexPlanStr

}

object IndexHadoopFsRelation {
  def apply(
      rel: HadoopFsRelation,
      spark: SparkSession,
      index: IndexLogEntry): IndexHadoopFsRelation = {
    new IndexHadoopFsRelation(
      rel.location,
      rel.partitionSchema,
      rel.dataSchema,
      rel.bucketSpec,
      rel.fileFormat,
      rel.options)(spark, index)
  }
}
