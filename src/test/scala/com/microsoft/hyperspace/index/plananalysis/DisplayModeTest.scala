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

import com.microsoft.hyperspace.index.IndexConstants

class DisplayModeTest extends SparkFunSuite {
  test("Testing default tags in Display Mode.") {
    val htmlMode = new HTMLMode(Map.empty)
    assert(htmlMode.beginEndTag.open.equals("<pre>"))
    assert(htmlMode.beginEndTag.close.equals("</pre>"))
    assert(htmlMode.highlightTag.open.equals("""<b style="background:LightGreen">"""))
    assert(htmlMode.highlightTag.close.equals("""</b>"""))

    val consoleMode = new ConsoleMode(Map.empty)
    assert(consoleMode.beginEndTag.open.equals(""))
    assert(consoleMode.beginEndTag.close.equals(""))
    assert(consoleMode.highlightTag.open.equals(Console.GREEN_B))
    assert(consoleMode.highlightTag.close.equals(Console.RESET))

    val plainTextMode = new PlainTextMode(Map.empty)
    assert(plainTextMode.beginEndTag.open.equals(""))
    assert(plainTextMode.beginEndTag.close.equals(""))
    assert(plainTextMode.highlightTag.open equals ("<----"))
    assert(plainTextMode.highlightTag.close.equals("---->"))
  }

  test("Testing highlight tags override in Display Mode.") {
    val htmlMode = new HTMLMode(getHighlightConf("<u>", "</u>"))
    assert(htmlMode.beginEndTag.open.equals("<pre>"))
    assert(htmlMode.beginEndTag.close.equals("</pre>"))
    assert(htmlMode.highlightTag.open.equals("""<u>"""))
    assert(htmlMode.highlightTag.close.equals("""</u>"""))

    val consoleMode = new ConsoleMode(getHighlightConf(Console.YELLOW_B, Console.RESET))
    assert(consoleMode.beginEndTag.open.equals(""))
    assert(consoleMode.beginEndTag.close.equals(""))
    assert(consoleMode.highlightTag.open.equals(Console.YELLOW_B))
    assert(consoleMode.highlightTag.close.equals(Console.RESET))

    val plainTextMode = new PlainTextMode(getHighlightConf("****", "****"))
    assert(plainTextMode.beginEndTag.open.equals(""))
    assert(plainTextMode.beginEndTag.close.equals(""))
    assert(plainTextMode.highlightTag.open.equals("****"))
    assert(plainTextMode.highlightTag.close.equals("****"))
  }

  private def getHighlightConf(
      highlightBegin: String,
      highlightEnd: String): Map[String, String] = {
    Map[String, String](
      IndexConstants.HIGHLIGHT_BEGIN_TAG -> highlightBegin,
      IndexConstants.HIGHLIGHT_END_TAG -> highlightEnd)
  }
}
