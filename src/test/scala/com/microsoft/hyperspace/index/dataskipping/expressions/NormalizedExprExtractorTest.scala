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

package com.microsoft.hyperspace.index.dataskipping.expressions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

import com.microsoft.hyperspace.index.HyperspaceSuite

class NormalizedExprExtractorTest extends HyperspaceSuite {
  val extractor = NormalizedExprExtractor(
    AttributeReference("A", IntegerType)(ExpressionUtils.nullExprId, Nil),
    Map(ExprId(42) -> "A"))

  test("apply returns true if the expression matches.") {
    assert(
      extractor.unapply(AttributeReference("a", IntegerType)(ExprId(42), Nil)) ===
        Some(extractor.expr))
  }

  test("apply returns false if the expression does not match") {
    assert(extractor.unapply(Literal(42)) === None)
  }
}
