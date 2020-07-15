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

import com.microsoft.hyperspace.telemetry.Constants._

/**
 * Hyperspace event logging interface. Extend this to enable emitting hyperspace events.
 */
trait HyperspaceEventLogging {
  def logEvent(event: HyperspaceEvent): Unit = EventLogger.getLogger.logEvent(event)
}

/**
 * Event Logger interface. Concrete implementations of this class implement event handling logic
 * for hyperspace events.
 */
trait EventLogger {
  def logEvent(event: HyperspaceEvent): Unit
}

object EventLogger extends Logging {
  // Singleton logger instance for event logging.
  private var logger: EventLogger = _

  def getLogger: EventLogger = {
    if (logger != null) {
      return logger
    }

    val className: String = SparkSession.getActiveSession
      .map { spark =>
        spark.conf.get(HYPERSPACE_EVENT_LOGGER_CLASS_KEY, DEFAULT_HYPERSPACE_EVENT_LOGGER_CLASS)
      }
      .getOrElse(DEFAULT_HYPERSPACE_EVENT_LOGGER_CLASS)

    logger = Try(Utils.classForName(className).newInstance) match {
      case Success(emitter: EventLogger) => emitter
      case _ =>
        logDebug(s"Hyperspace logger is not appropriately set, using default logger.")
        Utils
          .classForName(DEFAULT_HYPERSPACE_EVENT_LOGGER_CLASS)
          .newInstance()
          .asInstanceOf[EventLogger]
    }
    logger
  }
}

class NoOpEventLogger extends EventLogger {
  override def logEvent(event: HyperspaceEvent): Unit = {}
}
