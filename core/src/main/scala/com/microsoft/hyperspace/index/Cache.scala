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

/**
 * This trait defines interface APIs for a Cache.
 * @tparam T Type of cache entry
 */
trait Cache[T] {

  /**
   * Return cached entry.
   * @return if cache content is valid, current entry; Otherwise None
   */
  def get(): Option[T]

  /**
   * Replace cache content with new entry.
   * @param entry new entry to cache
   */
  def set(entry: T): Unit

  /**
   * Remove current cache entry.
   */
  def clear(): Unit
}
