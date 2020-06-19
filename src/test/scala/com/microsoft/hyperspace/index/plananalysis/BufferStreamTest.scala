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

package com.microsoft.hyperspace.index.plananalysis

import org.apache.spark.SparkFunSuite

class BufferStreamTest extends SparkFunSuite {
  test("Testing buffer stream highlight with spaces.") {
    val s = "   Please highlight me.  "
    val testDisplayMode = new TestDisplayMode
    // Note: Ignores spaces in beginning and end while appending highlighting characters.
    val expectedOutput = "   " + testDisplayMode.highlightTag.open + "Please highlight me." +
      testDisplayMode.highlightTag.close + "  "
    val buffer = BufferStream(testDisplayMode)
    buffer.highlight(s)
    assert(buffer.toString.equals(expectedOutput))
    assert(
      buffer.withTag.equals(
        testDisplayMode.beginEndTag.open + expectedOutput + testDisplayMode.beginEndTag.close))
  }

  test("Testing buffer stream highlight without spaces.") {
    val s = "Please highlight me."
    val testDisplayMode = new TestDisplayMode
    val expectedOutput = testDisplayMode.highlightTag.open + s +
      testDisplayMode.highlightTag.close
    val buffer = BufferStream(testDisplayMode)
    buffer.highlight(s)
    assert(buffer.toString.equals(expectedOutput))
    assert(
      buffer.withTag.equals(
        testDisplayMode.beginEndTag.open + expectedOutput + testDisplayMode.beginEndTag.close))
  }

  test("Testing write stream operation.") {
    val s1 = "This is statement 1."
    val s2 = "This is statement 2."
    val testDisplayMode = new TestDisplayMode
    val buffer = BufferStream(testDisplayMode)
    buffer.writeLine(s1)
    assert(buffer.toString().equals(s1 + testDisplayMode.newLine))
    buffer.write(s2)
    assert(buffer.toString().equals(s1 + testDisplayMode.newLine + s2))
  }

  private class TestDisplayMode extends DisplayMode {
    override val highlightTag: Tag = Tag("<h1>", "</h1>")
    override def beginEndTag: Tag = Tag("<begin>", "</end>")
    override def newLine: String = "<br>"
  }
}
