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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.delta.files.TahoeLogFileIndex
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}

import com.microsoft.hyperspace.index.FileInfo

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

  def retrieveCurFileInfos(plan: LogicalPlan): Seq[FileInfo] = {
    val ret = plan.collect {
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        location.allFiles.map(f => FileInfo(f.getPath.toString, f.getLen, f.getModificationTime))
      case LogicalRelation(
          HadoopFsRelation(location: TahoeLogFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        location
          .getSnapshot(stalenessAcceptable = false)
          .filesForScan(Seq(), Seq(), keepStats = false)
          .files
          .map { f =>
            val path = new Path(location.path, f.path)
            FileInfo(path.toString, f.size, f.modificationTime)
          }
    }
    assert(ret.length == 1)
    ret.flatten
  }
}
