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

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, PartitioningAwareFileIndex}

import com.microsoft.hyperspace.util.HashingUtils

/**
 * [[FileBasedSignatureProvider]] provides the logical plan signature based on files in the
 * logical relation. File metadata, eg. size, modification time and path, of each file in the
 * FileIndex will be used to generate the signature.
 * If the logical plan does not have any LogicalRelation operator, no signature is provided.
 */
class FileBasedSignatureProvider extends LogicalPlanSignatureProvider {

  /**
   * Generate the signature of logical plan.
   *
   * @param logicalPlan logical plan of data frame.
   * @param targetFiles List of file paths used to calculate signature.
   * @return signature, if the logical plan has some LogicalRelation operator(s); Otherwise None.
   */
  override def signature(
      logicalPlan: LogicalPlan,
      targetFiles: Option[Set[Path]]): Option[String] = {
    fingerprintVisitor(logicalPlan, targetFiles).map(HashingUtils.md5Hex)
  }

  /**
   * Visit logical plan and collect info needed for fingerprint.
   *
   * @param logicalPlan logical plan of data frame.
   * @param targetFiles List of file paths used to calculate signature.
   * @return fingerprint, if the logical plan has some LogicalRelation operator(s); Otherwise None.
   */
  private def fingerprintVisitor(
      logicalPlan: LogicalPlan,
      targetFiles: Option[Set[Path]]): Option[String] = {
    var fingerprint = ""
    logicalPlan.foreachUp {
      // Currently we are only collecting plan fingerprint from hdfs file based scan nodes.
      case LogicalRelation(
          HadoopFsRelation(location: PartitioningAwareFileIndex, _, _, _, _, _),
          _,
          _,
          _) =>
        fingerprint += location.allFiles.foldLeft("")(
          (accumulate: String, fileStatus: FileStatus) =>
            if (targetFiles.isEmpty || targetFiles.get.contains(fileStatus.getPath)) {
              HashingUtils.md5Hex(accumulate + getFingerprint(fileStatus))
            } else {
              accumulate
          })
      case _ =>
    }

    fingerprint match {
      case "" => None
      case _ => Some(fingerprint)
    }
  }

  /**
   * Get the fingerprint of a file.
   *
   * @param fileStatus file status.
   * @return the fingerprint of a file.
   */
  private def getFingerprint(fileStatus: FileStatus): String = {
    fileStatus.getLen.toString + fileStatus.getModificationTime.toString +
      fileStatus.getPath.toString
  }
}
