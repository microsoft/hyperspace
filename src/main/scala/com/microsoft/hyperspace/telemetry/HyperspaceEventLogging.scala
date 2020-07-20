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
package com.microsoft.hyperspace.telemetry

import scala.util.{Success, Try}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.hyperspace.Utils

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.telemetry.Constants._

/**
 * Hyperspace event logging interface. Extend this to enable emitting hyperspace events.
 */
trait HyperspaceEventLogging {
  def logEvent(event: HyperspaceEvent): Unit = EventLogger.logger.logEvent(event)
}

/**
 * Event Logger interface. Concrete implementations of this class implement event handling logic
 * for hyperspace events.
 */
trait EventLogger {
  def logEvent(event: HyperspaceEvent): Unit
}

private[telemetry] object EventLogger extends Logging {
  // Singleton logger instance for event logging.
  lazy val logger: EventLogger = {
    SparkSession.getActiveSession
      .flatMap(_.conf.getOption(HYPERSPACE_EVENT_LOGGER_CLASS_KEY))
      .map { className =>
        Try(Utils.classForName(className).newInstance) match {
          case Success(logger: EventLogger) =>
            logInfo(s"Setting event logger to $className")
            logger
          case _ =>
            logError(s"Unable to instantiate event logger from provided class $className")
            throw HyperspaceException(
              s"Unable to instantiate event logger from provided class $className")
        }
      }
      .getOrElse {
        logInfo(
          s"Defaulting to the ${NoOpEventLogger.getClass.getName.stripSuffix("$")} event logger")
        NoOpEventLogger
      }
  }
}

object NoOpEventLogger extends EventLogger {
  override def logEvent(event: HyperspaceEvent): Unit = {}
}
