/*
 * Copyright (2021) The Hyperspace Project Authors.
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

package com.microsoft.hyperspace.shim

import org.apache.spark.SparkFunSuite

import com.microsoft.hyperspace.BuildInfo

class MacroTest extends SparkFunSuite {
  import scala.math.Ordering.Implicits._

  private val Array(major, minor) = BuildInfo.sparkShortVersion.split('.').map(_.toInt)
  assert((major, minor) <= (2, 4) || (major, minor) >= (3, 0))

  private val untilSpark24String = "Until Spark 2.4"
  private val sinceSpark30String = "Since Spark 3.0"
  private val expectedString = if ((major, minor) <= (2, 4)) {
    untilSpark24String
  } else {
    sinceSpark30String
  }

  test("UntilSpark24 and SinceSpark30 macros work as expected for methods") {
    class Annotated {
      @UntilSpark24
      def f: String = {
        // Test if we can use Spark 2 only things without compilation errors.
        assert(classOf[Spark2Only].getName === "com.microsoft.hyperspace.shim.Spark2Only")
        untilSpark24String
      }

      @SinceSpark30
      def f: String = {
        // Test if we can use Spark 3 only things without compilation errors.
        assert(classOf[Spark3Only].getName === "com.microsoft.hyperspace.shim.Spark3Only")
        sinceSpark30String
      }
    }
    assert(classOf[Annotated].getDeclaredMethods.count(_.getName == "f") === 1)
    assert(new Annotated().f === expectedString)
  }
}
