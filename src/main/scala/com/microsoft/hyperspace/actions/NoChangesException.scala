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

/**
 * This exception represents a No-op required from Hyperspace. Use this exception when a
 * hyperspace action is not necessary for index maintenance.
 * For example, if the data source has not changed since the last time an index was created on it,
 * we don't need to do anything when user calls a `refreshIndex()`.
 *
 * [[Action.run]] will silently catch this exception and will not fail the application.
 *
 * @param msg Error message.
 */
private[actions] case class NoChangesException(msg: String) extends Exception(msg)
