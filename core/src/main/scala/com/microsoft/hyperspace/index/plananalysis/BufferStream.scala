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

/**
 * BufferStream class that provides functionality to write string to buffer using
 * the given display mode.
 */
case class BufferStream(private val displayMode: DisplayMode) {
  private val stringBuilder = new StringBuilder

  /**
   * Writes string with newline to the buffer.
   *
   * @param s string.
   * @return this [[BufferStream]] object.
   */
  def writeLine(s: String = ""): BufferStream = {
    write(s)
    stringBuilder.append(displayMode.newLine)
    this
  }

  /**
   * Writes string to the buffer.
   *
   * @param s string.
   * @return this [[BufferStream]] object.
   */
  def write(s: String): BufferStream = {
    stringBuilder.append(s)
    this
  }

  /**
   * Writes string with highlight option to the buffer.
   *
   * @param s string.
   * @return this [[BufferStream]] object.
   */
  def highlight(s: String): BufferStream = {
    // Appends highlighting start character after leading whitespaces if any.
    val highlightStart =
      """(\A\s+|\A)""".r.replaceFirstIn(s, "$1" + displayMode.highlightTag.open)
    // Appends highlighting end character before beginning trailing whitespaces if any.
    val highlightEnd =
      """((\s+\Z)|$)""".r.replaceFirstIn(highlightStart, displayMode.highlightTag.close + "$1")
    stringBuilder.append(highlightEnd)
    this
  }

  /**
   * Returns buffered string with begin and end tag.
   *
   * @return buffered string with begin and end tag.
   */
  def withTag(): String = {
    displayMode.beginEndTag.open + this.toString + displayMode.beginEndTag.close
  }

  /**
   * Returns buffered string.
   *
   * @return constructed string in the buffer.
   */
  override def toString: String = {
    stringBuilder.toString
  }
}
