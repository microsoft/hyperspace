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

import com.microsoft.hyperspace.index.IndexConstants

/**
 * Interface to support different display modes like HTML, Console for explain API.
 */
abstract class DisplayMode {

  /**
   * Indicates starting and ending position of string to be highlighted.
   *
   * @return highlight tags.
   */
  val highlightTag: Tag = Tag("", "")

  /**
   * Beginning and ending tag for the output.
   *
   * @return beginning and ending tag for the output.
   */
  def beginEndTag: Tag = Tag("", "")

  /**
   * Indicates new line character in display mode.
   *
   * @return new line character in display mode.
   */
  def newLine: String

  protected def getHighlightTagOrElse(displayConf: Map[String, String], defaultTag: Tag): Tag = {
    val highlightBegin = displayConf.getOrElse(IndexConstants.HIGHLIGHT_BEGIN_TAG, "")
    val highlightEnd = displayConf.getOrElse(IndexConstants.HIGHLIGHT_END_TAG, "")
    if (highlightBegin.nonEmpty && highlightEnd.nonEmpty) {
      Tag(highlightBegin, highlightEnd)
    } else {
      defaultTag
    }
  }
}

/**
 * HTML display mode for displaying HTML formatted output.
 */
class HTMLMode(displayConf: Map[String, String]) extends DisplayMode {
  override val beginEndTag: Tag = Tag("<pre>", "</pre>")

  override val highlightTag: Tag =
    getHighlightTagOrElse(displayConf, Tag("""<b style="background:LightGreen">""", "</b>"))

  override val newLine: String = "<br>"
}

/**
 * Plain text display mode which uses plain text characters to highlight output of explain API.
 */
class PlainTextMode(displayConf: Map[String, String]) extends DisplayMode {
  override val highlightTag: Tag = getHighlightTagOrElse(displayConf, Tag("<----", "---->"))

  override val newLine: String = System.lineSeparator
}

/**
 * Console display mode used for displaying output of explain API on Console.
 */
class ConsoleMode(displayConf: Map[String, String]) extends DisplayMode {
  override val highlightTag: Tag =
    getHighlightTagOrElse(displayConf, Tag(Console.GREEN_B, Console.RESET))

  override val newLine: String = System.lineSeparator
}

case class Tag(open: String, close: String)
