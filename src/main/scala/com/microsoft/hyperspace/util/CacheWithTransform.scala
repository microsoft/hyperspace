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

/**
 * A cache object that stores both initial and final value.
 * The initial value is produced by 'init' and the final value is produced by 'transform'
 * using the initial value.
 *
 * The cached initial/final values will be updated only if the initial value is modified.
 *
 * @param init Function to produce an initial value.
 * @param transform Function to transform the initial value.
 * @tparam A Type of the initial value.
 * @tparam B Type of the final value.
 */
class CacheWithTransform[A, B](val init: () => A, val transform: A => B) {
  private var initValue: A = _
  private var finalValue: B = _

  def load(): B = {
    val newInitValue = init()
    if (initValue == newInitValue) {
      finalValue
    } else {
      initValue = newInitValue
      finalValue = transform(initValue)
      finalValue
    }
  }
}
