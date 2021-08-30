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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.mockito.Mockito.{mock, when}

import com.microsoft.hyperspace.index.HyperspaceSuite

class ExtractorsTest extends HyperspaceSuite {
  val a = AttributeReference("A", IntegerType)()
  val b = AttributeReference("B", BooleanType)()
  val zero = Literal(0, IntegerType)
  val one = Literal(1, IntegerType)
  val two = Literal(2, IntegerType)
  val trueLit = Literal(true, BooleanType)
  val falseLit = Literal(false, BooleanType)
  val nullInt = Literal(null, IntegerType)
  val nullBool = Literal(null, BooleanType)

  val aMatcher = {
    val matcher = mock(classOf[ExprMatcher])
    when(matcher.apply(a)).thenReturn(true)
    matcher
  }
  val bMatcher = {
    val matcher = mock(classOf[ExprMatcher])
    when(matcher.apply(b)).thenReturn(true)
    matcher
  }
  val nonMatcher = mock(classOf[ExprMatcher])

  // EqualToExtractor
  {
    val AEqualTo = EqualToExtractor(aMatcher)
    val NoneEqualTo = EqualToExtractor(nonMatcher)

    test("EqualToExtractor matches EqualTo(<expr>, <lit>) if matcher(<expr>).") {
      val value = EqualTo(a, zero) match {
        case AEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("EqualToExtractor does not match EqualTo(<expr>, null).") {
      val value = EqualTo(a, nullInt) match {
        case AEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("EqualToExtractor does not match EqualTo(<expr>, <expr>).") {
      val value = EqualTo(a, a) match {
        case AEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("EqualToExtractor does not match EqualTo(<expr>, <lit>) if !matcher(<expr>).") {
      val value = EqualTo(a, zero) match {
        case NoneEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("EqualToExtractor matches EqualTo(<lit>, <expr>) if matcher(<expr>).") {
      val value = EqualTo(zero, a) match {
        case AEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("EqualToExtractor does not match EqualTo(null, <expr>).") {
      val value = EqualTo(nullInt, a) match {
        case AEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("EqualToExtractor does not match EqualTo(<lit>, <expr>) if !matcher(<expr>).") {
      val value = EqualTo(zero, a) match {
        case NoneEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("EqualToExtractor matches EqualNullSafe(<expr>, <lit>) if matcher(<expr>).") {
      val value = EqualNullSafe(a, zero) match {
        case AEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("EqualToExtractor does not match EqualNullSafe(<expr>, null).") {
      val value = EqualNullSafe(a, nullInt) match {
        case AEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("EqualToExtractor does not match EqualNullSafe(<expr>, <lit>) if !matcher(<expr>).") {
      val value = EqualNullSafe(a, zero) match {
        case NoneEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("EqualToExtractor matches EqualNullSafe(<lit>, <expr>) if matcher(<expr>).") {
      val value = EqualNullSafe(zero, a) match {
        case AEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("EqualToExtractor does not match EqualNullSafe(null, <expr>).") {
      val value = EqualNullSafe(nullInt, a) match {
        case AEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("EqualToExtractor does not match EqualNullSafe(<lit>, <expr>) if !matcher(<expr>).") {
      val value = EqualNullSafe(zero, a) match {
        case NoneEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("EqualToExtractor does not match expressions other than EqualTo/EqualNullSafe.") {
      val value = zero match {
        case AEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }
  }

  // LessThanExtractor
  {
    val ALessThan = LessThanExtractor(aMatcher)
    val NoneLessThan = LessThanExtractor(nonMatcher)

    test("LessThanExtractor matches LessThan(<expr>, <lit>) if matcher(<expr>).") {
      val value = LessThan(a, zero) match {
        case ALessThan(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("LessThanExtractor does not match LessThan(<expr>, null).") {
      val value = LessThan(a, nullInt) match {
        case ALessThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanExtractor does not match LessThan(<expr>, <expr>).") {
      val value = LessThan(a, a) match {
        case ALessThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanExtractor does not match LessThan(<expr>, <lit>) if !matcher(<expr>).") {
      val value = LessThan(a, zero) match {
        case NoneLessThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanExtractor does not match LessThan(<lit>, <expr>).") {
      val value = LessThan(zero, a) match {
        case ALessThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanExtractor matches GreaterThan(<lit>, <expr>) if matcher(<expr>).") {
      val value = GreaterThan(zero, a) match {
        case ALessThan(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("LessThanExtractor does not match GreaterThan(null, <expr>).") {
      val value = GreaterThan(nullInt, a) match {
        case ALessThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanExtractor does not match GreaterThan(<lit>, <expr>) if !matcher(<expr>).") {
      val value = GreaterThan(zero, a) match {
        case NoneLessThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanExtractor does not match GreaterThan(<expr>, <lit>).") {
      val value = GreaterThan(a, zero) match {
        case ALessThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanExtractor does not match expressions other than LessThan.") {
      val value = zero match {
        case ALessThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }
  }

  // LessThanOrEqualToExtractor
  {
    val ALessThanOrEqualTo = LessThanOrEqualToExtractor(aMatcher)
    val NoneLessThanOrEqualTo = LessThanOrEqualToExtractor(nonMatcher)

    test(
      "LessThanOrEqualToExtractor matches LessThanOrEqual(<expr>, <lit>) if matcher(<expr>).") {
      val value = LessThanOrEqual(a, zero) match {
        case ALessThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("LessThanOrEqualToExtractor does not match LessThanOrEqual(<expr>, null).") {
      val value = LessThanOrEqual(a, nullInt) match {
        case ALessThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanOrEqualToExtractor does not match LessThanOrEqual(<expr>, <expr>).") {
      val value = LessThanOrEqual(a, a) match {
        case ALessThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test(
      "LessThanOrEqualToExtractor does not match LessThanOrEqual(<expr>, <lit>) " +
        "if !matcher(<expr>).") {
      val value = LessThanOrEqual(a, zero) match {
        case NoneLessThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanOrEqualToExtractor does not match LessThanOrEqual(<lit>, <expr>).") {
      val value = LessThanOrEqual(zero, a) match {
        case ALessThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test(
      "LessThanOrEqualToExtractor matches GreaterThanOrEqual(<lit>, <expr>) if matcher(<expr>).") {
      val value = GreaterThanOrEqual(zero, a) match {
        case ALessThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("LessThanOrEqualToExtractor does not match GreaterThanOrEqual(null, <expr>).") {
      val value = GreaterThanOrEqual(nullInt, a) match {
        case ALessThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test(
      "LessThanOrEqualToExtractor does not match GreaterThanOrEqual(<lit>, <expr>) " +
        "if !matcher(<expr>).") {
      val value = GreaterThanOrEqual(zero, a) match {
        case NoneLessThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanOrEqualToExtractor does not match GreaterThanOrEqual(<expr>, <lit>).") {
      val value = GreaterThanOrEqual(a, zero) match {
        case ALessThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("LessThanOrEqualToExtractor does not match expressions other than LessThanOrEqual.") {
      val value = zero match {
        case ALessThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }
  }

  // GreaterThanExtractor
  {
    val AGreaterThan = GreaterThanExtractor(aMatcher)
    val NoneGreaterThan = GreaterThanExtractor(nonMatcher)

    test("GreaterThanExtractor matches GreaterThan(<expr>, <lit>) if matcher(<expr>).") {
      val value = GreaterThan(a, zero) match {
        case AGreaterThan(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("GreaterThanExtractor does not match GreaterThan(<expr>, null).") {
      val value = GreaterThan(a, nullInt) match {
        case AGreaterThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("GreaterThanExtractor does not match GreaterThan(<expr>, <expr>).") {
      val value = GreaterThan(a, a) match {
        case AGreaterThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("GreaterThanExtractor does not match GreaterThan(<expr>, <lit>) if !matcher(<expr>).") {
      val value = GreaterThan(a, zero) match {
        case NoneGreaterThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("GreaterThanExtractor does not match GreaterThan(<lit>, <expr>).") {
      val value = GreaterThan(zero, a) match {
        case AGreaterThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("GreaterThanExtractor matches LessThan(<lit>, <expr>) if matcher(<expr>).") {
      val value = LessThan(zero, a) match {
        case AGreaterThan(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("GreaterThanExtractor does not match LessThan(null, <expr>).") {
      val value = LessThan(nullInt, a) match {
        case AGreaterThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("GreaterThanExtractor does not match LessThan(<lit>, <expr>) if !matcher(<expr>).") {
      val value = LessThan(zero, a) match {
        case NoneGreaterThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("GreaterThanExtractor does not match LessThan(<expr>, <lit>).") {
      val value = LessThan(a, zero) match {
        case AGreaterThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("GreaterThanExtractor does not match expressions other than GreaterThan.") {
      val value = zero match {
        case AGreaterThan(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }
  }

  // GreaterThanOrEqualToExtractor
  {
    val AGreaterThanOrEqualTo = GreaterThanOrEqualToExtractor(aMatcher)
    val NoneGreaterThanOrEqualTo = GreaterThanOrEqualToExtractor(nonMatcher)

    test(
      "GreaterThanOrEqualToExtractor matches GreaterThanOrEqual(<expr>, <lit>) " +
        "if matcher(<expr>).") {
      val value = GreaterThanOrEqual(a, zero) match {
        case AGreaterThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("GreaterThanOrEqualToExtractor does not match GreaterThanOrEqual(<expr>, null).") {
      val value = GreaterThanOrEqual(a, nullInt) match {
        case AGreaterThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("GreaterThanOrEqualToExtractor does not match GreaterThanOrEqual(<expr>, <expr>).") {
      val value = GreaterThanOrEqual(a, a) match {
        case AGreaterThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test(
      "GreaterThanOrEqualToExtractor does not match GreaterThanOrEqual(<expr>, <lit>) " +
        "if !matcher(<expr>).") {
      val value = GreaterThanOrEqual(a, zero) match {
        case NoneGreaterThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("GreaterThanOrEqualToExtractor does not match GreaterThanOrEqual(<lit>, <expr>).") {
      val value = GreaterThanOrEqual(zero, a) match {
        case AGreaterThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test(
      "GreaterThanOrEqualToExtractor matches LessThanOrEqual(<lit>, <expr>) if matcher(<expr>).") {
      val value = LessThanOrEqual(zero, a) match {
        case AGreaterThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === Some(zero))
    }

    test("GreaterThanOrEqualToExtractor does not match LessThanOrEqual(null, <expr>).") {
      val value = LessThanOrEqual(nullInt, a) match {
        case AGreaterThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test(
      "GreaterThanOrEqualToExtractor does not match LessThanOrEqual(<lit>, <expr>) " +
        "if !matcher(<expr>).") {
      val value = LessThanOrEqual(zero, a) match {
        case NoneGreaterThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test("GreaterThanOrEqualToExtractor does not match LessThanOrEqual(<expr>, <lit>).") {
      val value = LessThanOrEqual(a, zero) match {
        case AGreaterThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }

    test(
      "GreaterThanOrEqualToExtractor does not match expressions other than GreaterThanOrEqual.") {
      val value = zero match {
        case AGreaterThanOrEqualTo(value) => Some(value)
        case _ => None
      }
      assert(value === None)
    }
  }

  // IsNullExtractor
  {
    val AIsNull = IsNullExtractor(aMatcher)
    val NoneIsNull = IsNullExtractor(nonMatcher)

    test("IsNullExtractor matches IsNull(<expr>) if matcher(<expr>).") {
      val value = IsNull(a) match {
        case AIsNull() => true
        case _ => false
      }
      assert(value)
    }

    test("IsNullExtractor does not match IsNull(<expr>) if !matcher(<expr>).") {
      val value = IsNull(a) match {
        case NoneIsNull() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsNullExtractor matches EqualNullSafe(<expr>, null).") {
      val value = EqualNullSafe(a, nullInt) match {
        case AIsNull() => true
        case _ => false
      }
      assert(value)
    }

    test("IsNullExtractor does not match EqualNullSafe(<expr>, <lit>).") {
      val value = EqualNullSafe(a, zero) match {
        case AIsNull() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsNullExtractor matches EqualNullSafe(null, <expr>).") {
      val value = EqualNullSafe(nullInt, a) match {
        case AIsNull() => true
        case _ => false
      }
      assert(value)
    }

    test("IsNullExtractor does not match EqualNullSafe(<lit>, <expr>).") {
      val value = EqualNullSafe(zero, a) match {
        case AIsNull() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsNullExtractor does not match expressions other than IsNull/EqualNullSafe.") {
      val value = zero match {
        case AIsNull() => true
        case _ => false
      }
      assert(!value)
    }
  }

  // IsNotNullExtractor
  {
    val AIsNotNull = IsNotNullExtractor(aMatcher)
    val NoneIsNotNull = IsNotNullExtractor(nonMatcher)

    test("IsNotNullExtractor matches IsNotNull(<expr>) if matcher(<expr>).") {
      val value = IsNotNull(a) match {
        case AIsNotNull() => true
        case _ => false
      }
      assert(value)
    }

    test("IsNotNullExtractor does not match IsNotNull(<expr>) if !matcher(<expr>).") {
      val value = IsNotNull(a) match {
        case NoneIsNotNull() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsNotNullExtractor matches Not(IsNull(<expr>)) if matcher(<expr>).") {
      val value = Not(IsNull(a)) match {
        case AIsNotNull() => true
        case _ => false
      }
      assert(value)
    }

    test("IsNotNullExtractor matches Not(IsNull(<expr>)) if !matcher(<expr>).") {
      val value = Not(IsNull(a)) match {
        case NoneIsNotNull() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsNotNullExtractor matches Not(EqualNullSafe(<expr>, null)).") {
      val value = Not(EqualNullSafe(a, nullInt)) match {
        case AIsNotNull() => true
        case _ => false
      }
      assert(value)
    }

    test("IsNotNullExtractor does not match Not(EqualNullSafe(<expr>, <lit>)).") {
      val value = Not(EqualNullSafe(a, zero)) match {
        case AIsNotNull() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsNotNullExtractor matches Not(EqualNullSafe(null, <expr>)).") {
      val value = Not(EqualNullSafe(nullInt, a)) match {
        case AIsNotNull() => true
        case _ => false
      }
      assert(value)
    }

    test("IsNotNullExtractor does not match Not(EqualNullSafe(<lit>, <expr>)).") {
      val value = Not(EqualNullSafe(zero, a)) match {
        case AIsNotNull() => true
        case _ => false
      }
      assert(!value)
    }

    test(
      "IsNotNullExtractor does not match expressions other than IsNotNull/Not(EqualNullSafe).") {
      val value = zero match {
        case AIsNotNull() => true
        case _ => false
      }
      assert(!value)
    }
  }

  // IsTrueExtractor
  {
    val AIsTrue = IsTrueExtractor(aMatcher)
    val BIsTrue = IsTrueExtractor(bMatcher)
    val NoneIsTrue = IsTrueExtractor(nonMatcher)

    test("IsTrueExtractor matches <expr> if matcher(<expr>).") {
      val value = b match {
        case BIsTrue() => true
        case _ => false
      }
      assert(value)
    }

    test("IsTrueExtractor does not match <expr> if !matcher(<expr>).") {
      val value = b match {
        case NoneIsTrue() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsTrueExtractor does not match <expr> if type is not boolean.") {
      val value = a match {
        case AIsTrue() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsTrueExtractor matches EqualTo(<expr>, true) if matcher(<expr>).") {
      val value = EqualTo(b, trueLit) match {
        case BIsTrue() => true
        case _ => false
      }
      assert(value)
    }

    test("IsTrueExtractor does not match EqualTo(<expr>, false).") {
      val value = EqualTo(b, falseLit) match {
        case BIsTrue() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsTrueExtractor matches EqualTo(true, <expr>) if matcher(<expr>).") {
      val value = EqualTo(trueLit, b) match {
        case BIsTrue() => true
        case _ => false
      }
      assert(value)
    }

    test("IsTrueExtractor does not match EqualTo(false, <expr>).") {
      val value = EqualTo(falseLit, b) match {
        case BIsTrue() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsTrueExtractor matches EqualNullSafe(<expr>, true) if matcher(<expr>).") {
      val value = EqualNullSafe(b, trueLit) match {
        case BIsTrue() => true
        case _ => false
      }
      assert(value)
    }

    test("IsTrueExtractor does not match EqualNullSafe(<expr>, false).") {
      val value = EqualNullSafe(b, falseLit) match {
        case BIsTrue() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsTrueExtractor matches EqualNullSafe(true, <expr>) if matcher(<expr>).") {
      val value = EqualNullSafe(trueLit, b) match {
        case BIsTrue() => true
        case _ => false
      }
      assert(value)
    }

    test("IsTrueExtractor does not match EqualNullSafe(false, <expr>).") {
      val value = EqualNullSafe(falseLit, b) match {
        case BIsTrue() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsTrueExtractor does not match <lit>.") {
      val value = zero match {
        case BIsTrue() => true
        case _ => false
      }
      assert(!value)
    }
  }

  // IsFalseExtractor
  {
    val BIsFalse = IsFalseExtractor(bMatcher)
    val NoneIsFalse = IsFalseExtractor(nonMatcher)

    test("IsFalseExtractor matches Not(<expr>) if matcher(<expr>).") {
      val value = Not(b) match {
        case BIsFalse() => true
        case _ => false
      }
      assert(value)
    }

    test("IsFalseExtractor does not match Not(<expr>) if !matcher(<expr>).") {
      val value = Not(b) match {
        case NoneIsFalse() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsFalseExtractor matches EqualTo(<expr>, false) if matcher(<expr>).") {
      val value = EqualTo(b, falseLit) match {
        case BIsFalse() => true
        case _ => false
      }
      assert(value)
    }

    test("IsFalseExtractor does not match EqualTo(<expr>, true).") {
      val value = EqualTo(b, trueLit) match {
        case BIsFalse() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsFalseExtractor matches EqualTo(false, <expr>) if matcher(<expr>).") {
      val value = EqualTo(falseLit, b) match {
        case BIsFalse() => true
        case _ => false
      }
      assert(value)
    }

    test("IsFalseExtractor does not match EqualTo(true, <expr>).") {
      val value = EqualTo(trueLit, b) match {
        case BIsFalse() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsFalseExtractor matches EqualNullSafe(<expr>, false) if matcher(<expr>).") {
      val value = EqualNullSafe(b, falseLit) match {
        case BIsFalse() => true
        case _ => false
      }
      assert(value)
    }

    test("IsFalseExtractor does not match EqualNullSafe(<expr>, true).") {
      val value = EqualNullSafe(b, trueLit) match {
        case BIsFalse() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsFalseExtractor matches EqualNullSafe(false, <expr>) if matcher(<expr>).") {
      val value = EqualNullSafe(falseLit, b) match {
        case BIsFalse() => true
        case _ => false
      }
      assert(value)
    }

    test("IsFalseExtractor does not match EqualNullSafe(true, <expr>).") {
      val value = EqualNullSafe(trueLit, b) match {
        case BIsFalse() => true
        case _ => false
      }
      assert(!value)
    }

    test("IsFalseExtractor does not match <lit>.") {
      val value = zero match {
        case BIsFalse() => true
        case _ => false
      }
      assert(!value)
    }
  }

  // InExtractor
  {
    val AIn = InExtractor(aMatcher)
    val NoneIn = InExtractor(nonMatcher)

    test("InExtractor matches In(<expr>, <lit>*) if matcher(<expr>).") {
      val value = In(a, Seq(zero, one, two)) match {
        case AIn(values) => Some(values)
        case _ => None
      }
      assert(value === Some(Seq(zero, one, two)))
    }

    test("InExtractor matches In(<expr>, <lit>*) if !matcher(<expr>).") {
      val value = In(a, Seq(zero, one, two)) match {
        case NoneIn(values) => Some(values)
        case _ => None
      }
      assert(value === None)
    }

    test("InExtractor does not match In(<expr>, <lit>*) if <lit>* is empty.") {
      val value = In(a, Nil) match {
        case AIn(values) => Some(values)
        case _ => None
      }
      assert(value === None)
    }

    test("InExtractor matches In(<expr>, <lit>*) if some <lit>s are null.") {
      val value = In(a, Seq(zero, nullInt, nullInt, two)) match {
        case AIn(values) => Some(values)
        case _ => None
      }
      assert(value === Some(Seq(zero, two)))
    }

    test("InExtractor does not match In(<expr>, <expr>*).") {
      val value = In(a, Seq(zero, two, a)) match {
        case AIn(values) => Some(values)
        case _ => None
      }
      assert(value === None)
    }

    test("InExtractor does not match other than In.") {
      val value = a match {
        case AIn(values) => Some(values)
        case _ => None
      }
      assert(value === None)
    }
  }

  // InSetExtractor
  {
    val AInSet = InSetExtractor(aMatcher)
    val NoneInSet = InSetExtractor(nonMatcher)

    test("InSetExtractor matches InSet(<expr>, <set>) if matcher(<expr>).") {
      val value = InSet(a, Set(0, 1, 2)) match {
        case AInSet(values) => Some(values)
        case _ => None
      }
      assert(value === Some(Set(0, 1, 2)))
    }

    test("InSetExtractor matches InSet(<expr>, <lit>*) if !matcher(<expr>).") {
      val value = InSet(a, Set(0, 1, 2)) match {
        case NoneInSet(values) => Some(values)
        case _ => None
      }
      assert(value === None)
    }

    test("InSetExtractor does not match other than InSet.") {
      val value = a match {
        case AInSet(values) => Some(values)
        case _ => None
      }
      assert(value === None)
    }
  }
}
