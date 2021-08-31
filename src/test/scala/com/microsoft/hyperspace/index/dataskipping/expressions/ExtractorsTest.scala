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
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{mock, when}

import com.microsoft.hyperspace.index.HyperspaceSuite

class ExtractorsTest extends HyperspaceSuite {
  val a = AttributeReference("a", IntegerType)()
  val b = AttributeReference("b", IntegerType)()
  val c = AttributeReference("c", IntegerType)()
  val d = AttributeReference("d", BooleanType)()
  val e = AttributeReference("e", BooleanType)()

  val aa = Literal(0)
  val bb = Literal(1)
  val dd = Literal(true)

  def getExtractor(x: Expression, y: Expression): ExpressionExtractor = {
    val extractor = mock(classOf[ExpressionExtractor])
    when(extractor.unapply(any())).thenReturn(None)
    when(extractor.unapply(x)).thenReturn(Some(y))
    extractor
  }

  val aExtractor = getExtractor(a, aa)
  val bExtractor = getExtractor(b, bb)
  val dExtractor = getExtractor(d, dd)

  test("EqualToExtractor matches EqualTo(a, b).") {
    assert(EqualToExtractor(aExtractor, bExtractor).unapply(EqualTo(a, b)) === Some((aa, bb)))
  }

  test("EqualToExtractor matches EqualTo(b, a).") {
    assert(EqualToExtractor(aExtractor, bExtractor).unapply(EqualTo(b, a)) === Some((aa, bb)))
  }

  test("EqualToExtractor does not match EqualTo(a, c).") {
    assert(EqualToExtractor(aExtractor, bExtractor).unapply(EqualTo(a, c)) === None)
  }

  test("LessThanExtractor matches LessThan(a, b).") {
    assert(LessThanExtractor(aExtractor, bExtractor).unapply(LessThan(a, b)) === Some((aa, bb)))
  }

  test("LessThanExtractor matches GreaterThan(b, a).") {
    assert(
      LessThanExtractor(aExtractor, bExtractor).unapply(GreaterThan(b, a)) ===
        Some((aa, bb)))
  }

  test("LessThanExtractor does not match LessThan(b, a).") {
    assert(LessThanExtractor(aExtractor, bExtractor).unapply(LessThan(b, a)) === None)
  }

  test("LessThanOrEqualExtractor matches LessThanOrEqual(a, b).") {
    assert(
      LessThanOrEqualExtractor(aExtractor, bExtractor).unapply(LessThanOrEqual(a, b)) ===
        Some((aa, bb)))
  }

  test("LessThanOrEqualExtractor matches GreaterThanOrEqual(b, a).") {
    assert(
      LessThanOrEqualExtractor(aExtractor, bExtractor).unapply(GreaterThanOrEqual(b, a)) ===
        Some((aa, bb)))
  }

  test("LessThanOrEqualExtractor does not match LessThanOrEqual(b, a).") {
    assert(
      LessThanOrEqualExtractor(aExtractor, bExtractor).unapply(LessThanOrEqual(b, a)) ===
        None)
  }

  test("IsNullExtractor matches IsNull(a).") {
    assert(IsNullExtractor(aExtractor).unapply(IsNull(a)) === Some(aa))
  }

  test("IsNullExtractor matches EqualNullSafe(a, null).") {
    assert(IsNullExtractor(aExtractor).unapply(EqualNullSafe(a, Literal(null))) === Some(aa))
  }

  test("IsNullExtractor matches EqualNullSafe(null, a).") {
    assert(IsNullExtractor(aExtractor).unapply(EqualNullSafe(Literal(null), a)) === Some(aa))
  }

  test("IsNullExtractor does not match IsNull(c).") {
    assert(IsNullExtractor(aExtractor).unapply(IsNull(c)) === None)
  }

  test("IsNotNullExtractor matches IsNotNull(a).") {
    assert(IsNotNullExtractor(aExtractor).unapply(IsNotNull(a)) === Some(aa))
  }

  test("IsNotNullExtractor matches Not(IsNull(a)).") {
    assert(IsNotNullExtractor(aExtractor).unapply(Not(IsNull(a))) === Some(aa))
  }

  test("IsNotNullExtractor matches Not(EqualNullSafe(a, null)).") {
    assert(
      IsNotNullExtractor(aExtractor).unapply(Not(EqualNullSafe(a, Literal(null)))) ===
        Some(aa))
  }

  test("IsNotNullExtractor matches Not(EqualNullSafe(null, a)).") {
    assert(
      IsNotNullExtractor(aExtractor).unapply(Not(EqualNullSafe(Literal(null), a))) ===
        Some(aa))
  }

  test("IsNotNullExtractor does not match IsNotNull(c).") {
    assert(IsNotNullExtractor(aExtractor).unapply(IsNotNull(c)) === None)
  }

  test("IsTrueExtractor matches d.") {
    assert(IsTrueExtractor(dExtractor).unapply(d) === Some(dd))
  }

  test("IsTrueExtractor matches EqualTo(d, true).") {
    assert(IsTrueExtractor(dExtractor).unapply(EqualTo(d, Literal(true))) === Some(dd))
  }

  test("IsTrueExtractor matches EqualTo(true, d).") {
    assert(IsTrueExtractor(dExtractor).unapply(EqualTo(Literal(true), d)) === Some(dd))
  }

  test("IsTrueExtractor does not match e.") {
    assert(IsTrueExtractor(dExtractor).unapply(e) === None)
  }

  test("IsTrueExtractor does not match a.") {
    assert(IsTrueExtractor(dExtractor).unapply(a) === None)
  }

  test("IsFalseExtractor matches Not(d).") {
    assert(IsFalseExtractor(dExtractor).unapply(Not(d)) === Some(dd))
  }

  test("IsFalseExtractor matches EqualTo(d, false).") {
    assert(IsFalseExtractor(dExtractor).unapply(EqualTo(d, Literal(false))) === Some(dd))
  }

  test("IsFalseExtractor matches EqualTo(false, d).") {
    assert(IsFalseExtractor(dExtractor).unapply(EqualTo(Literal(false), d)) === Some(dd))
  }

  test("IsFalseExtractor does not match Not(e).") {
    assert(IsFalseExtractor(dExtractor).unapply(Not(e)) === None)
  }

  test("IsFalseExtractor does not match a.") {
    assert(IsFalseExtractor(dExtractor).unapply(a) === None)
  }

  test("InExtractor matches In(a, Seq()).") {
    assert(InExtractor(aExtractor, bExtractor).unapply(In(a, Seq())) === Some((aa, Seq())))
  }

  test("InExtractor matches In(a, Seq(b)).") {
    assert(InExtractor(aExtractor, bExtractor).unapply(In(a, Seq(b))) === Some((aa, Seq(bb))))
  }

  test("InExtractor matches In(a, Seq(b, b)).") {
    assert(
      InExtractor(aExtractor, bExtractor).unapply(In(a, Seq(b, b))) ===
        Some((aa, Seq(bb, bb))))
  }

  test("InExtractor does not match In(a, Seq(b, c)).") {
    assert(InExtractor(aExtractor, bExtractor).unapply(In(a, Seq(b, c))) === None)
  }

  test("InExtractor does not match In(c, Seq(b, b)).") {
    assert(InExtractor(aExtractor, bExtractor).unapply(In(c, Seq(b, b))) === None)
  }

  test("InSetExtractor matches InSet(a, Set()).") {
    assert(InSetExtractor(aExtractor).unapply(InSet(a, Set[Any]())) === Some((aa, Set[Any]())))
  }

  test("InSetExtractor matches InSet(a, Set(1)).") {
    assert(InSetExtractor(aExtractor).unapply(InSet(a, Set[Any](1))) === Some((aa, Set[Any](1))))
  }

  test("InSetExtractor does not match InSet(c, Set(1)).") {
    assert(InSetExtractor(aExtractor).unapply(InSet(c, Set[Any](1))) === None)
  }
}
