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

package com.microsoft.hyperspace.logging

import org.apache.commons.lang.StringUtils
import org.apache.spark.SparkContext

import com.microsoft.hyperspace.index.IndexLogEntry

/**
 * Base class for all Hyperspace events.
 */
class HyperspaceEvent(
    sparkUser: String = StringUtils.EMPTY,
    appId: String = StringUtils.EMPTY,
    appName: String = StringUtils.EMPTY) {
  def dimensions: Seq[String] = Seq[String](sparkUser, appId, appName)
}

/**
 * General index CRUD event.
 *
 * @param sparkUser user of spark application.
 * @param appId spark application id.
 * @param appName spark application name.
 * @param indexes related indexes.
 * @param message message about event.
 */
class HyperspaceIndexCRUDEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    message: String = "")
    extends HyperspaceEvent(sparkUser, appId, appName) {

  override def dimensions: Seq[String] = {
    commonDimensions ++ Seq("", "") ++ Seq(message)
  }

  /**
   * Get the common fields of dimensions for [[HyperspaceIndexCRUDEvent]].
   */
  protected def commonDimensions(): Seq[String] = {
    Seq(
      this.getClass.getSimpleName,
      sparkUser,
      appId,
      appName,
      indexes.map(_.name).mkString("; "),
      indexes
        .map(
          index =>
            index.indexedColumns.mkString("[", ", ", "]") + "/" +
              index.includedColumns.mkString("[", ", ", "]"))
        .mkString("; "),
      indexes.map(_.content.root).mkString("; "))
  }
}

case class CreateActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    originalPlan: String,
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, indexes, message) {
  // TODO(570681): before implement scrubber, do not log logical plan.
  //  Once scrubber is implemented, override dimensions() here to include query plan information.
}

case class DeleteActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, indexes, message)

case class RestoreActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, indexes)

/**
 * IndexLogEntry vacuum started event.
 *
 * @param sparkUser user of spark application.
 * @param appId spark application id.
 * @param appName spark application name.
 * @param indexes related indexes.
 * @param message message about event.
 */
case class VacuumActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, indexes, message)

case class RefreshActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    message: String = "")
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, indexes, message)

case class CancelActionEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    message: String)
    extends HyperspaceIndexCRUDEvent(sparkUser, appId, appName, indexes, message)

class HyperspaceIndexUsageEvent(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    planBeforeRule: String,
    planAfterRule: String,
    message: String = "")
    extends HyperspaceEvent(sparkUser, appId, appName) {

  override def dimensions: Seq[String] = {
    // TODO: before implement scrubber, do not log logical plan but an empty string.
    commonDimensions() ++ Seq("", "") ++ Seq(message)
  }

  /**
   * Get the common fields of dimensions for [[HyperspaceIndexUsageEvent]].
   */
  protected def commonDimensions(): Seq[String] = {
    Seq(
      this.getClass.getSimpleName,
      sparkUser,
      appId,
      appName,
      indexes.map(_.name).mkString("; "),
      indexes
        .map(
          index =>
            index.indexedColumns.mkString("[", ", ", "]") + "/" +
              index.includedColumns.mkString("[", ", ", "]"))
        .mkString("; "),
      indexes.map(_.content.root).mkString("; "))
  }
}

/**
 * IndexLogEntry rule applied event.
 *
 * @param sparkUser user of spark application.
 * @param appId spark application id.
 * @param appName spark application name.
 * @param indexes related indexes.
 * @param planBeforeRule logical plan before rule applied.
 * @param planAfterRule logical plan after rule applied.
 * @param message message about event.
 */
case class HyperspaceIndexRuleApplied(
    sparkUser: String,
    appId: String,
    appName: String,
    indexes: Seq[IndexLogEntry],
    planBeforeRule: String,
    planAfterRule: String,
    message: String)
    extends HyperspaceIndexUsageEvent(
      sparkUser,
      appId,
      appName,
      indexes,
      planBeforeRule,
      planAfterRule,
      message) {

  def this(
      sparkContext: SparkContext,
      indexes: Seq[IndexLogEntry],
      planBeforeRule: String = "",
      planAfterRule: String = "",
      message: String = "") =
    this(
      sparkContext.sparkUser,
      sparkContext.applicationId,
      sparkContext.appName,
      indexes,
      planBeforeRule,
      planAfterRule,
      message)

  // TODO: before implement scrubber, do not log logical plan.
  // Once scrubber is implemented, override dimensions() here to include query plan information.
}
