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

import org.apache.commons.codec.digest.DigestUtils

/**
 * HashingUtils supplies different hashing functions.
 */
object HashingUtils {

  /**
   * MD5-based hashing function.
   *
   * @param input the input to be hashed, for Any type of data.
   * @return the hash code string.
   */
  def md5Hex(input: Any): String = {
    DigestUtils.md5Hex(input.toString)
  }
}
