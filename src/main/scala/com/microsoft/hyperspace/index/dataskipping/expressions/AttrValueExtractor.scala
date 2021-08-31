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

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, SubqueryExpression}

case class AttrValueExtractor(attrMap: Map[Attribute, Expression]) extends ExpressionExtractor {
  override def unapply(e: Expression): Option[Expression] = {
    if (canTransform(e)) Some(transform(e)) else None
  }

  private def canTransform(e: Expression): Boolean = {
    e.deterministic &&
    e.references.forall(attrMap.contains) &&
    !SubqueryExpression.hasSubquery(e)
  }

  private def transform(e: Expression): Expression = {
    e.transform { case a: Attribute => attrMap(a) }
  }
}
