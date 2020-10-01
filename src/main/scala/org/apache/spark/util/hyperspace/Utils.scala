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

package org.apache.spark.util.hyperspace

import java.io.File

/**
 * This class is used to expose package private methods from org.apache.spark.util.Utils.
 */
object Utils {
  def classForName(className: String): Class[_] = {
    org.apache.spark.util.Utils.classForName(className)
  }

  def createTempDir(): File = {
    org.apache.spark.util.Utils.createTempDir()
  }

  def deleteRecursively(file: File): Unit = {
    org.apache.spark.util.Utils.deleteRecursively(file)
  }
}
