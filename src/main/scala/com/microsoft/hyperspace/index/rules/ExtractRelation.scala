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

package com.microsoft.hyperspace.index.rules

import org.apache.spark.sql.catalyst.plans.logical.LeafNode

import com.microsoft.hyperspace.{ActiveSparkSession, Hyperspace}
import com.microsoft.hyperspace.index.sources.FileBasedRelation

object ExtractRelation extends ActiveSparkSession {
  def unapply(plan: LeafNode): Option[FileBasedRelation] = {
    val provider = Hyperspace.getContext(spark).sourceProviderManager
    if (provider.isSupportedRelation(plan)) {
      Some(provider.getRelation(plan))
    } else {
      None
    }
  }
}
