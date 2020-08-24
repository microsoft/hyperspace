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

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}
import org.apache.spark.sql.sources.DataSourceRegister

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.index.{Content, Hdfs, NoOpFingerprint, Relation}
import com.microsoft.hyperspace.index.Content.Directory.FileInfo

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

  def sourceRelations(plan: LogicalPlan): Seq[Relation] =
    plan.collect {
      case LogicalRelation(
          HadoopFsRelation(
            location: PartitioningAwareFileIndex,
            _,
            dataSchema,
            _,
            fileFormat,
            options),
          _,
          _,
          _) =>
        val files = location.allFiles.map(FileInfo(_))
        // Note that source files are currently fingerprinted when the optimized plan is
        // fingerprinted by LogicalPlanFingerprint.
        val sourceDataProperties =
          Hdfs.Properties(Content("", Seq(Content.Directory("", files, NoOpFingerprint()))))
        val fileFormatName = fileFormat match {
          case d: DataSourceRegister => d.shortName
          case other => throw HyperspaceException(s"Unsupported file format: $other")
        }
        // "path" key in options can incur multiple data read unexpectedly.
        val opts = options - "path"
        Relation(
          location.rootPaths.map(_.toString),
          Hdfs(sourceDataProperties),
          dataSchema.json,
          fileFormatName,
          opts)
    }
}
