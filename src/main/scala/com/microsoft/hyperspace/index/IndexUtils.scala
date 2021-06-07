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

package com.microsoft.hyperspace.index

object IndexUtils {

  /**
   * Returns whether the lineage feature is enabled for the index by looking at
   * the index properties.
   *
   * If the index has no corresponding key in the property map,
   * [[IndexConstants.INDEX_LINEAGE_ENABLED_DEFAULT]] will be returned.
   *
   * @param properties Index properties
   */
  def hasLineageColumn(properties: Map[String, String]): Boolean = {
    properties
      .getOrElse(IndexConstants.LINEAGE_PROPERTY, IndexConstants.INDEX_LINEAGE_ENABLED_DEFAULT)
      .toBoolean
  }
}
