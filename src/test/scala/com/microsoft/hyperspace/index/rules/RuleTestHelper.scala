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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{StructField, StructType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index._
import com.microsoft.hyperspace.index.serde.LogicalPlanSerDeUtils

object RuleTestHelper {
  class TestSignatureProvider extends LogicalPlanSignatureProvider {

    def signature(plan: LogicalPlan): Option[String] =
      plan
        .collectFirst {
          case LogicalRelation(HadoopFsRelation(location, _, _, _, _, _), _, _, _) =>
            location.hashCode()
        }
        .map(_.toString)
  }

  def createIndex(
      spark: SparkSession,
      systemPath: Path,
      name: String,
      indexCols: Seq[AttributeReference],
      includedCols: Seq[AttributeReference],
      plan: LogicalPlan): IndexLogEntry = {
    val signClass = new TestSignatureProvider().getClass.getName

    LogicalPlanSignatureProvider.create(signClass).signature(plan) match {
      case Some(s) =>
        val sourcePlanProperties = SparkPlan.Properties(
          LogicalPlanSerDeUtils.serialize(plan, spark),
          LogicalPlanFingerprint(
            LogicalPlanFingerprint.Properties(Seq(Signature(signClass, s)))))
        val sourceDataProperties =
          Hdfs.Properties(Content("", Seq(Content.Directory("", Seq(), NoOpFingerprint()))))

        val indexLogEntry = IndexLogEntry(
          name,
          CoveringIndex(
            CoveringIndex.Properties(
              CoveringIndex.Properties
                .Columns(indexCols.map(_.name), includedCols.map(_.name)),
              IndexLogEntry.schemaString(schemaFromAttributes(indexCols ++ includedCols: _*)),
              10)),
          Content(getIndexDataFilesPath(name, systemPath).toUri.toString, Seq()),
          Source(SparkPlan(sourcePlanProperties), Seq(Hdfs(sourceDataProperties))),
          Map())

        val logManager = new IndexLogManagerImpl(getIndexRootPath(name, systemPath))
        indexLogEntry.state = Constants.States.ACTIVE
        logManager.writeLog(0, indexLogEntry)

        indexLogEntry

      case None => throw HyperspaceException("Invalid plan for index dataFrame.")
    }
  }

  def schemaFromAttributes(attributes: Attribute*): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  def baseRelation(
      location: FileIndex,
      schema: StructType,
      spark: SparkSession): HadoopFsRelation =
    HadoopFsRelation(location, new StructType(), schema, None, new ParquetFileFormat, Map.empty)(
      spark)

  def getIndexDataFilesPath(indexName: String, systemPath: Path): Path =
    new Path(
      getIndexRootPath(indexName, systemPath),
      s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0")

  def getIndexRootPath(indexName: String, systemPath: Path): Path =
    new Path(systemPath, indexName)
}
