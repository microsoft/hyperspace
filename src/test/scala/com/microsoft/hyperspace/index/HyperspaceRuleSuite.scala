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

package com.microsoft.hyperspace.index

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.types.{StructField, StructType}

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.actions.Constants
import com.microsoft.hyperspace.index.Hdfs.Properties
import com.microsoft.hyperspace.index.covering.CoveringIndex
import com.microsoft.hyperspace.util.PathUtils

trait HyperspaceRuleSuite extends HyperspaceSuite {
  private val defaultFileNames = Seq("f1.parquet", "f2.parquet")
  def createIndexLogEntry(
      name: String,
      indexCols: Seq[AttributeReference],
      includedCols: Seq[AttributeReference],
      plan: LogicalPlan,
      numBuckets: Int = 10,
      inputFiles: Seq[FileInfo] = Seq(),
      writeLog: Boolean = true,
      filenames: Seq[String] = defaultFileNames): IndexLogEntry = {
    val signClass = new RuleTestHelper.TestSignatureProvider().getClass.getName

    LogicalPlanSignatureProvider.create(signClass).signature(plan) match {
      case Some(s) =>
        val sourcePlanProperties = SparkPlan.Properties(
          Seq(
            Relation(
              Seq("dummy"),
              Hdfs(Properties(Content(Directory("/", files = inputFiles)))),
              new StructType(),
              "format",
              Map())),
          null,
          null,
          LogicalPlanFingerprint(LogicalPlanFingerprint.Properties(Seq(Signature(signClass, s)))))

        val indexFiles = getIndexDataFilesPaths(name, filenames).map { path =>
          new FileStatus(10, false, 1, 10, 10, path)
        }

        val indexLogEntry = IndexLogEntry.create(
          name,
          CoveringIndex(
            indexCols.map(_.name),
            includedCols.map(_.name),
            schemaFromAttributes(indexCols ++ includedCols: _*),
            numBuckets,
            Map()),
          Content.fromLeafFiles(indexFiles, new FileIdTracker).get,
          Source(SparkPlan(sourcePlanProperties)),
          Map())

        val logManager = new IndexLogManagerImpl(getIndexRootPath(name))
        indexLogEntry.state = Constants.States.ACTIVE
        if (writeLog) {
          assert(logManager.writeLog(0, indexLogEntry))
        }
        indexLogEntry

      case None => throw HyperspaceException("Invalid plan for index dataFrame.")
    }
  }

  def getIndexDataFilesPaths(
      indexName: String,
      filenames: Seq[String] = defaultFileNames): Seq[Path] =
    filenames.map { f =>
      new Path(
        new Path(
          getIndexRootPath(indexName),
          s"${IndexConstants.INDEX_VERSION_DIRECTORY_PREFIX}=0"),
        f)
    }

  def schemaFromAttributes(attributes: Attribute*): StructType =
    StructType(attributes.map(a => StructField(a.name, a.dataType, a.nullable, a.metadata)))

  def baseRelation(location: FileIndex, schema: StructType): HadoopFsRelation =
    HadoopFsRelation(location, new StructType(), schema, None, new ParquetFileFormat, Map.empty)(
      spark)

  def getIndexRootPath(indexName: String): Path =
    PathUtils.makeAbsolute(new Path(systemPath, indexName), new Configuration)

  def setCommonSourceSizeInBytesTag(
      index: IndexLogEntry,
      plan: LogicalPlan,
      files: Seq[FileInfo]): Unit = {
    val commonSizeInBytes = files.map(_.size).sum
    index.setTagValue(plan, IndexLogEntryTags.COMMON_SOURCE_SIZE_IN_BYTES, commonSizeInBytes)
  }
}
