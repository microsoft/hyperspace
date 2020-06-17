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

import com.microsoft.hyperspace.HyperspaceException
import com.microsoft.hyperspace.util.JsonUtils

abstract class LogEntry(val version: String) {
  var id: Long = 0

  var state: String = ""

  var timestamp: Long = System.currentTimeMillis()

  var enabled: Boolean = true
}

object LogEntry {
  def fromJson(json: String): LogEntry = {
    val jsonMap = JsonUtils.jsonToMap(json)
    val version = jsonMap.get("version")

    // TODO: Currently, the following validates the version to generate IndexLogEntry.
    //   It should evolve to generate each node with "kind" key differently once
    //   those nodes are first abstracted out as traits.
    version match {
      case Some(IndexLogEntry.VERSION) =>
        JsonUtils.fromJson[IndexLogEntry](json)
      case _ =>
        throw HyperspaceException(s"Unsupported log entry found: version = $version")
    }
  }
}
