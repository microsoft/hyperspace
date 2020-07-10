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

import scala.util.{Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.hyperspace.Utils

import com.microsoft.hyperspace.logging.Constants._

trait HyperspaceLogging extends Logging {
  def logEvent(event: HyperspaceEvent): Unit = HyperspaceLogging.logger.logEvent(event)
}

object HyperspaceLogging extends Logging {
  private val logger: HyperspaceLogging = {
    val spark = SparkSession.getActiveSession
    val className = if (spark.isDefined) {
      spark.get.conf.get(HYPERSPACE_LOGGER_CLASS, DEFAULT_HYPERSPACE_LOGGER_CLASS)
    } else {
      logDebug(s"$HYPERSPACE_LOGGER_CLASS is not set in spark conf, default logger used.")
      Constants.DEFAULT_HYPERSPACE_LOGGER_CLASS
    }

    Try(Utils.classForName(className).newInstance) match {
      case Success(logger: HyperspaceLogging) => logger
      case _ =>
        logDebug(s"Hyperspace logger is not appropriately set, using default logger.")
        Utils
          .classForName(DEFAULT_HYPERSPACE_LOGGER_CLASS)
          .newInstance()
          .asInstanceOf[HyperspaceLogging]
    }
  }
}

/**
 * Logger that logs Hyperspace events to spark logs.
 */
class GenericHyperspaceLogging extends HyperspaceLogging {
  override def logEvent(event: HyperspaceEvent): Unit = {
    logInfo(event.dimensions.mkString("; "))
  }
}
