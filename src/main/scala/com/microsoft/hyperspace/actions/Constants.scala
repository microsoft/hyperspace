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

package com.microsoft.hyperspace.actions

object Constants {
  object States {
    val ACTIVE = "ACTIVE"
    val CREATING = "CREATING"
    val DELETING = "DELETING"
    val DELETED = "DELETED"
    val REFRESHING = "REFRESHING"
    val VACUUMING = "VACUUMING"
    val RESTORING = "RESTORING"
    val OPTIMIZING = "OPTIMIZING"
    val DOESNOTEXIST = "DOESNOTEXIST"
    val CANCELLING = "CANCELLING"
  }

  val STABLE_STATES: Set[String] = Set(States.ACTIVE, States.DELETED, States.DOESNOTEXIST)
}
