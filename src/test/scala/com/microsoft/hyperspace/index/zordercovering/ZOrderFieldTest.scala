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

package com.microsoft.hyperspace.index.zordercovering

import java.util

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types._

import com.microsoft.hyperspace.HyperspaceException

class ZOrderFieldTest extends SparkFunSuite {

  test("Test null values in ZOrderField.") {
    {
      val minVal = -11L
      val maxVal = 10L
      val result = ZOrderField.build("LongColumnName", LongType, minVal, maxVal)
      assert(result.isInstanceOf[LongMinMaxZOrderField])
      val col = result.asInstanceOf[LongMinMaxZOrderField]
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 5)

      // null value is not considered as min or max value.
      // So it's possible to get null value in getBitSet.
      // ZOrderField should return empty bitset for null value.
      val res = col.getBitSet(null).stream().toArray.toSeq
      assert(res.equals(Seq()), res)
    }

    {
      // If all values are null, min and max can be null.
      val minVal = null
      val maxVal = null
      val result = ZOrderField.build("LongColumnName", LongType, minVal, maxVal)
      assert(result.isInstanceOf[LongMinMaxZOrderField])
      val col = result.asInstanceOf[LongMinMaxZOrderField]
      // we don't consider this column if minVal == maxVal
      assert(col.bitLen == 0)

      {
        val res = col.getBitSet(null).stream().toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(10L).stream().toArray.toSeq
        assert(res.equals(Seq()), res)
      }
    }

    // Test same case for LongPercentileZOrderField.
    {
      val minVal = -11.0
      val maxVal = 10.0
      val result =
        ZOrderField.build("LongColumnName", LongType, minVal, maxVal, quantileEnabled = true)
      assert(result.isInstanceOf[LongPercentileZOrderField])
      val col = result.asInstanceOf[LongPercentileZOrderField]
      assert(col.bitLen == 4)

      // null value is not considered as min or max value.
      // So it's possible to get null value in getBitSet.
      // ZOrderField should return empty bitset for null value.
      val res = col.getBitSet(null).stream().toArray.toSeq
      assert(res.equals(Seq()), res)
    }

    {
      // If all values are null, min and max can be null.
      val minVal = null
      val maxVal = null
      val result =
        ZOrderField.build("LongColumnName", LongType, minVal, maxVal, quantileEnabled = true)
      assert(result.isInstanceOf[LongPercentileZOrderField])
      val col = result.asInstanceOf[LongPercentileZOrderField]
      // we don't consider this column if minVal == maxVal
      assert(col.bitLen == 0)

      {
        val res = col.getBitSet(null).stream().toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(10L).stream().toArray.toSeq
        assert(res.equals(Seq()), res)
      }
    }
  }

  test("Test LongMinMaxZOrderField.") {
    {
      val minVal = -11L
      val maxVal = 10L
      val result = ZOrderField.build("LongColumnName", LongType, minVal, maxVal)
      assert(result.isInstanceOf[LongMinMaxZOrderField])
      val col = result.asInstanceOf[LongMinMaxZOrderField]
      assert(col.name.equals("LongColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 5)

      // value - minValue = 9 - (-11) = 20 = 0b10100
      val res = col.getBitSet(9L).stream.toArray.toSeq
      assert(res.equals(Seq(2, 4)))
      val e = intercept[HyperspaceException](col.getBitSet((-12L)))
      assert(e.msg.contains("value=-12, min=-11, max=10"))
      val e2 = intercept[HyperspaceException](col.getBitSet(11L))
      assert(e2.msg.contains("value=11, min=-11, max=10"))
    }
    {
      val minVal = Long.MinValue
      val maxVal = Long.MaxValue
      val result = ZOrderField.build("LongColumnName", LongType, minVal, maxVal)
      assert(result.isInstanceOf[LongMinMaxZOrderField])
      val col = result.asInstanceOf[LongMinMaxZOrderField]
      assert(col.name.equals("LongColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 64)

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals((0 to 63).toArray.toSeq), res)
      }
      {
        val res = col.getBitSet(0L).stream.toArray.toSeq
        assert(res.equals(Seq(63)), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
    }
    {
      val minVal = 10000000000L
      val maxVal = 10000001024L
      val result = ZOrderField.build("LongColumnName", LongType, minVal, maxVal)
      assert(result.isInstanceOf[LongMinMaxZOrderField])
      val col = result.asInstanceOf[LongMinMaxZOrderField]
      assert(col.name.equals("LongColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 11)

      // Only use 0~1024 value for z-address.
      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(Seq(10).toArray.toSeq), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
      {
        val res = col.getBitSet((minVal + 1L)).stream.toArray.toSeq
        assert(res.equals(Seq(0)), res)
      }
    }
  }

  test("Test LongPercentileZOrderField.") {
    {
      val minVal = 2211231L
      val maxVal = 3122345L
      val result =
        ZOrderField.build(
          "LongColumnName",
          LongType,
          minVal.toDouble,
          maxVal.toDouble,
          quantileEnabled = true)
      assert(result.isInstanceOf[LongPercentileZOrderField])
      val col = result.asInstanceOf[LongPercentileZOrderField]
      assert(col.name.equals("LongColumnName"))
      assert(col.rawPercentile.head.equals(minVal.toDouble))
      assert(col.rawPercentile.last.equals(maxVal.toDouble))
      assert(col.rawPercentile.size == 2)
      assert(col.bitLen == (32768 - 1).toBinaryString.length)

      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(0 to 14), res)
      }

      {
        val p50 = (maxVal - minVal) / 2.0 + minVal
        val res = col.getBitSet(p50.toLong).stream.toArray.toSeq
        assert(res.equals(Seq(14)), res)
      }
      {
        val p10 = (maxVal - minVal) / 10.0 + minVal
        val res = col.getBitSet(p10.toLong).stream.toArray.toSeq
        val expected = util.BitSet.valueOf(Array((32768 * 0.1).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val p90 = (maxVal - minVal) * 0.9 + minVal
        val res = col.getBitSet(p90.toLong).stream.toArray.toSeq
        val expected = util.BitSet.valueOf(Array((32768 * 0.9).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
    }
    {
      val minVal = Long.MinValue
      val maxVal = Long.MaxValue
      val result =
        ZOrderField.build(
          "LongColumnName",
          LongType,
          minVal.toDouble,
          maxVal.toDouble,
          quantileEnabled = true)
      assert(result.isInstanceOf[LongPercentileZOrderField])
      val col = result.asInstanceOf[LongPercentileZOrderField]
      assert(col.name.equals("LongColumnName"))
      assert(col.rawPercentile.head.equals(minVal.toDouble))
      assert(col.rawPercentile.last.equals(maxVal.toDouble))
      assert(col.rawPercentile.size == 2)
      assert(col.bitLen == (32768 - 1).toBinaryString.length)

      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(0 to 14), res)
      }

      {
        val p50 = 0L
        val res = col.getBitSet(p50).stream.toArray.toSeq
        assert(res.equals(Seq(14)), res)
      }
      {
        val p10 = minVal * 0.8
        val res = col.getBitSet(p10.toLong).stream.toArray.toSeq
        val expected = util.BitSet.valueOf(Array((32768 * 0.1).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val p90 = maxVal * 0.8f
        val res = col.getBitSet(p90.toLong).stream.toArray.toSeq
        val expected = util.BitSet.valueOf(Array((32768 * 0.9).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
    }

    {
      val minVal = 0.0
      val maxVal = 1000.0
      val result =
        ZOrderField.build(
          "LongColumnName",
          LongType,
          minVal,
          maxVal,
          Seq(10.0, 100.0, 200.0, 500.0),
          quantileEnabled = true)
      assert(result.isInstanceOf[LongPercentileZOrderField])
      val col = result.asInstanceOf[LongPercentileZOrderField]
      assert(col.name.equals("LongColumnName"))
      assert(col.rawPercentile.head.equals(minVal))
      assert(col.rawPercentile.last.equals(maxVal))
      assert(col.rawPercentile.size == 6)
      val bucketSize = (maxVal / 2.0 - minVal / 2.0 + 1).toLong
      // For LongType, use min(32768, maxVal - minVal + 1). "/ 2.0" for normalize.
      assert(col.bitLen == (bucketSize - 1).toBinaryString.length)

      {
        val res = col.getBitSet(minVal.toLong).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(maxVal.toLong).stream.toArray.toSeq
        val expected =
          util.BitSet.valueOf(Array(bucketSize - 1)).stream.toArray.toSeq
        assert(res.equals(expected), expected)
      }

      {
        val value = 500L
        val res = col.getBitSet(value).stream.toArray.toSeq
        // value is not 50% percentile, 80% based on the given quantiles.
        val expected =
          util.BitSet.valueOf(Array((bucketSize * 0.8).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val value = 750L
        val res = col.getBitSet(value).stream.toArray.toSeq
        val expected =
          util.BitSet.valueOf(Array((bucketSize * 0.9).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val p90 = 150L
        val res = col.getBitSet(p90).stream.toArray.toSeq
        val expected =
          util.BitSet.valueOf(Array((bucketSize * 0.5).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
    }
  }

  test("Test IntMinMaxZOrderField.") {
    {
      val minVal = -11
      val maxVal = 10
      val result = ZOrderField.build("IntColumnName", IntegerType, minVal, maxVal)
      assert(result.isInstanceOf[IntMinMaxZOrderField])
      val col = result.asInstanceOf[IntMinMaxZOrderField]
      assert(col.name.equals("IntColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 5)

      val res = col.getBitSet(9).stream.toArray.toSeq
      // value - minValue = 9 - (-11) = 20 = 0b10100
      assert(res.equals(Seq(2, 4)), res)
      val e = intercept[HyperspaceException](col.getBitSet((-12)))
      assert(e.msg.contains("value=-12, min=-11, max=10"))
      val e2 = intercept[HyperspaceException](col.getBitSet(11))
      assert(e2.msg.contains("value=11, min=-11, max=10"))
    }
    {
      val minVal = Int.MinValue
      val maxVal = Int.MaxValue
      val result = ZOrderField.build("IntColumnName", IntegerType, minVal, maxVal)
      assert(result.isInstanceOf[IntMinMaxZOrderField])
      val col = result.asInstanceOf[IntMinMaxZOrderField]
      assert(col.name.equals("IntColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 32)

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals((0 to 31).toArray.toSeq), res)
      }
      {
        val res = col.getBitSet(0).stream.toArray.toSeq
        assert(res.equals(Seq(31)), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
    }
    {
      val minVal = 1000000000
      val maxVal = 1000001024
      val result = ZOrderField.build("IntColumnName", IntegerType, minVal, maxVal)
      assert(result.isInstanceOf[IntMinMaxZOrderField])
      val col = result.asInstanceOf[IntMinMaxZOrderField]
      assert(col.name.equals("IntColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 11)

      // Only use 0~1024 value for z-address.
      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(Seq(10).toArray.toSeq), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
      {
        val res = col.getBitSet((minVal + 1)).stream.toArray.toSeq
        assert(res.equals(Seq(0)), res)
      }
    }
  }

  test("Test ShortMinMaxZOrderField.") {
    {
      val minVal = -11.toShort
      val maxVal = 10.toShort
      val result = ZOrderField.build("ShortColumnName", ShortType, minVal, maxVal)
      assert(result.isInstanceOf[ShortMinMaxZOrderField])
      val col = result.asInstanceOf[ShortMinMaxZOrderField]
      assert(col.name.equals("ShortColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 5)

      val res = col.getBitSet(9.toShort).stream.toArray.toSeq
      // value - minValue = 9 - -11 = 20 = 0b10100
      assert(res.equals(Seq(2, 4)), res)
      val e = intercept[HyperspaceException](col.getBitSet((-12).toShort))
      assert(e.msg.contains("value=-12, min=-11, max=10"))
      val e2 = intercept[HyperspaceException](col.getBitSet(11.toShort))
      assert(e2.msg.contains("value=11, min=-11, max=10"))
    }
    {
      val minVal = Short.MinValue
      val maxVal = Short.MaxValue
      val result = ZOrderField.build("ShortColumnName", ShortType, minVal, maxVal)
      assert(result.isInstanceOf[ShortMinMaxZOrderField])
      val col = result.asInstanceOf[ShortMinMaxZOrderField]
      assert(col.name.equals("ShortColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 16)

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals((0 to 15).toArray.toSeq), res)
      }
      {
        val res = col.getBitSet(0.toShort).stream.toArray.toSeq
        assert(res.equals(Seq(15)), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
    }
    {
      val minVal = 10000.toShort
      val maxVal = 11024.toShort
      val result = ZOrderField.build("ShortColumnName", ShortType, minVal, maxVal)
      assert(result.isInstanceOf[ShortMinMaxZOrderField])
      val col = result.asInstanceOf[ShortMinMaxZOrderField]
      assert(col.name.equals("ShortColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 11)

      // Only use 0~1024 value for z-address.
      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(Seq(10).toArray.toSeq), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
      {
        val res = col.getBitSet((minVal + 1).toShort).stream.toArray.toSeq
        assert(res.equals(Seq(0)), res)
      }
    }
  }

  test("Test ByteMinMaxZOrderField.") {
    {
      val minVal = (-11).toByte
      val maxVal = (10).toByte
      val result = ZOrderField.build("ByteColumnName", ByteType, minVal, maxVal)
      assert(result.isInstanceOf[ByteMinMaxZOrderField])
      val col = result.asInstanceOf[ByteMinMaxZOrderField]
      assert(col.name.equals("ByteColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 5)

      val res = col.getBitSet(9.toByte).stream.toArray.toSeq
      // value - minValue = 9 - -11 = 20 = 0b10100
      assert(res.equals(Seq(2, 4)), res)
      val e = intercept[HyperspaceException](col.getBitSet((-12).toByte))
      assert(e.msg.contains("value=-12, min=-11, max=10"))
      val e2 = intercept[HyperspaceException](col.getBitSet(11.toByte))
      assert(e2.msg.contains("value=11, min=-11, max=10"))
    }
    {
      val minVal = Byte.MinValue
      val maxVal = Byte.MaxValue
      val result = ZOrderField.build("ByteColumnName", ByteType, minVal, maxVal)
      assert(result.isInstanceOf[ByteMinMaxZOrderField])
      val col = result.asInstanceOf[ByteMinMaxZOrderField]
      assert(col.name.equals("ByteColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 8)

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals((0 to 7).toArray.toSeq), res)
      }
      {
        val res = col.getBitSet(0.toByte).stream.toArray.toSeq
        assert(res.equals(Seq(7)), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
    }
    {
      val minVal = 200.toByte
      val maxVal = 216.toByte
      val result = ZOrderField.build("ByteColumnName", ByteType, minVal, maxVal)
      assert(result.isInstanceOf[ByteMinMaxZOrderField])
      val col = result.asInstanceOf[ByteMinMaxZOrderField]
      assert(col.name.equals("ByteColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 5)

      // Only use 0~16 value for z-address.
      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(Seq(4).toArray.toSeq), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
      {
        val res = col.getBitSet((minVal + 1).toByte).stream.toArray.toSeq
        assert(res.equals(Seq(0)), res)
      }
    }
  }

  test("Test BooleanZOrderField.") {
    {
      val minVal = false
      val maxVal = true
      val result = ZOrderField.build("BooleanColumnName", BooleanType, minVal, maxVal)
      assert(result.isInstanceOf[BooleanZOrderField])
      val col = result.asInstanceOf[BooleanZOrderField]
      assert(col.name.equals("BooleanColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 1)

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(Seq(0)), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
    }
  }

  test("Test DecimalZOrderField.") {
    def getBigDecimal(v: Long): java.math.BigDecimal = {
      new java.math.BigDecimal(v).setScale(0, java.math.RoundingMode.FLOOR)
    }

    {
      val minVal = getBigDecimal(-11)
      val maxVal = getBigDecimal(10)
      val result = ZOrderField.build("DecimalColumnName", DecimalType(1, 1), minVal, maxVal)
      assert(result.isInstanceOf[DecimalZOrderField])
      val col = result.asInstanceOf[DecimalZOrderField]
      assert(col.name.equals("DecimalColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 5)

      // value - minValue = 9 - (-11) = 20 = 0b10100
      val res = col.getBitSet(getBigDecimal(9)).stream.toArray.toSeq
      assert(res.equals(Seq(2, 4)), res)
      val e = intercept[HyperspaceException](col.getBitSet(getBigDecimal(-12)))
      assert(e.msg.contains("value=-12, min=-11, max=10"))
      val e2 = intercept[HyperspaceException](col.getBitSet(getBigDecimal(11)))
      assert(e2.msg.contains("value=11, min=-11, max=10"))
    }

    {
      // Test 2 bytes
      val minVal = getBigDecimal(-256)
      val maxVal = getBigDecimal(256)
      val result = ZOrderField.build("DecimalColumnName", DecimalType(1, 1), minVal, maxVal)
      assert(result.isInstanceOf[DecimalZOrderField])
      val col = result.asInstanceOf[DecimalZOrderField]
      assert(col.name.equals("DecimalColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 10)

      {
        // value - minValue = 0 - (-256) = 256 = 0b100000000
        val res = col.getBitSet(getBigDecimal(0)).stream.toArray.toSeq
        assert(res.equals(Seq(8)), res)
      }
      {
        // value - minValue = 256 - (-256) = 512 = 0b1000000000
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(Seq(9)), res)
      }

      {
        // value - minValue = -256 - (-256) = 0
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        // value - minValue = 8 - (-256) = 264 = 0b0100001000
        val res = col.getBitSet(getBigDecimal(8)).stream.toArray.toSeq
        assert(res.equals(Seq(3, 8)), res)
      }

      val e =
        intercept[HyperspaceException](col.getBitSet(getBigDecimal(-277)))
      assert(e.msg.contains("value=-277, min=-256, max=256"))
      val e2 =
        intercept[HyperspaceException](col.getBitSet(getBigDecimal(277)))
      assert(e2.msg.contains("value=277, min=-256, max=256"))
    }
    {
      val minVal = new java.math.BigDecimal("-98765432109876543210987654321098765432").setScale(0)
      val maxVal = new java.math.BigDecimal("98765432109876543210987654321098765432").setScale(0)
      val result = ZOrderField.build("DecimalColumnName", DecimalType(1, 1), minVal, maxVal)
      assert(result.isInstanceOf[DecimalZOrderField])
      val col = result.asInstanceOf[DecimalZOrderField]
      assert(col.name.equals("DecimalColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 128)

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        val expected = Seq(4, 5, 6, 7, 11, 13, 14, 16, 22, 23, 25, 26, 32, 33, 37, 39, 40, 41, 42,
          45, 48, 49, 50, 51, 52, 55, 59, 60, 61, 63, 64, 67, 71, 72, 73, 75, 78, 86, 91, 92, 93,
          95, 96, 98, 103, 104, 105, 106, 107, 112, 113, 115, 116, 119, 122, 124, 127)
        assert(res.equals(expected), res)
      }
      {
        val res = col.getBitSet(getBigDecimal(0)).stream.toArray.toSeq
        val expected = Seq(3, 4, 5, 6, 10, 12, 13, 15, 21, 22, 24, 25, 31, 32, 36, 38, 39, 40, 41,
          44, 47, 48, 49, 50, 51, 54, 58, 59, 60, 62, 63, 66, 70, 71, 72, 74, 77, 85, 90, 91, 92,
          94, 95, 97, 102, 103, 104, 105, 106, 111, 112, 114, 115, 118, 121, 123, 126)
        assert(res.equals(expected), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
    }
    {
      val minVal = getBigDecimal(10000000000L)
      val maxVal = getBigDecimal(10000001024L)
      val result = ZOrderField.build("DecimalColumnName", DecimalType(1, 1), minVal, maxVal)
      assert(result.isInstanceOf[DecimalZOrderField])
      val col = result.asInstanceOf[DecimalZOrderField]
      assert(col.name.equals("DecimalColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 11)

      // Only use 0~1024 value for z-address.
      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(Seq(10).toArray.toSeq), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
      {
        val res = col
          .getBitSet(getBigDecimal(10000000000L + 1L))
          .stream
          .toArray
          .toSeq
        assert(res.equals(Seq(0)), res)
      }
    }
  }

  test("Test DoublePercentileZOrderField.") {
    {
      val minVal = -2.211231
      val maxVal = 3.122345
      val result = ZOrderField.build("DoubleColumnName", DoubleType, minVal, maxVal)
      assert(result.isInstanceOf[DoublePercentileZOrderField])
      val col = result.asInstanceOf[DoublePercentileZOrderField]
      assert(col.name.equals("DoubleColumnName"))
      assert(col.rawPercentile.head.equals(minVal))
      assert(col.rawPercentile.last.equals(maxVal))
      assert(col.rawPercentile.size == 2)
      assert(col.bitLen == (32768 - 1).toBinaryString.length)

      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(0 to 14), res)
      }

      {
        val p50 = (maxVal - minVal) / 2.0 + minVal
        val res = col.getBitSet(p50).stream.toArray.toSeq
        assert(res.equals(Seq(14)), res)
      }
      {
        val p10 = (maxVal - minVal) / 10.0 + minVal
        val res = col.getBitSet(p10).stream.toArray.toSeq
        val expected = util.BitSet.valueOf(Array((32768 * 0.1).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val p90 = (maxVal - minVal) * 0.9 + minVal
        val res = col.getBitSet(p90).stream.toArray.toSeq
        val expected =
          util.BitSet.valueOf(Array((32768 * 0.9).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
    }

    {
      val minVal = Double.MinValue
      val maxVal = Double.MaxValue
      val result = ZOrderField.build("DoubleColumnName", DoubleType, minVal, maxVal)
      assert(result.isInstanceOf[DoublePercentileZOrderField])
      val col = result.asInstanceOf[DoublePercentileZOrderField]
      assert(col.name.equals("DoubleColumnName"))
      assert(col.rawPercentile.head.equals(minVal))
      assert(col.rawPercentile.last.equals(maxVal))
      assert(col.rawPercentile.size == 2)
      assert(col.bitLen == (32768 - 1).toBinaryString.length)

      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(0 to 14), res)
      }

      {
        val p50 = 0.0
        val res = col.getBitSet(p50).stream.toArray.toSeq
        assert(res.equals(Seq(14)), res)
      }
      {
        val p10 = minVal * 0.8
        val res = col.getBitSet(p10).stream.toArray.toSeq
        val expected = util.BitSet.valueOf(Array((32768 * 0.1).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val p90 = maxVal * 0.8
        val res = col.getBitSet(p90).stream.toArray.toSeq
        val expected =
          util.BitSet.valueOf(Array((32768 * 0.9).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
    }

    {
      val minVal = 0.0
      val maxVal = 1000.0
      val result =
        ZOrderField.build(
          "DoubleColumnName",
          DoubleType,
          minVal,
          maxVal,
          Seq(10.0, 100.0, 200.0, 500.0),
          quantileEnabled = true)
      assert(result.isInstanceOf[DoublePercentileZOrderField])
      val col = result.asInstanceOf[DoublePercentileZOrderField]
      assert(col.name.equals("DoubleColumnName"))
      assert(col.rawPercentile.head.equals(minVal))
      assert(col.rawPercentile.last.equals(maxVal))
      assert(col.rawPercentile.size == 6)
      assert(col.bitLen == (32768 - 1).toBinaryString.length)

      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(0 to 14), res)
      }

      {
        val value = 500.0
        val res = col.getBitSet(value).stream.toArray.toSeq
        // value is not 50% percentile, 80% based on the given quantiles.
        val expected =
          util.BitSet.valueOf(Array((32768 * 0.8).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val value = 750.0
        val res = col.getBitSet(value).stream.toArray.toSeq
        val expected =
          util.BitSet.valueOf(Array((32768 * 0.9).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val p90 = 150.0
        val res = col.getBitSet(p90).stream.toArray.toSeq
        val expected =
          util.BitSet.valueOf(Array((32768 * 0.5).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
    }
  }

  test("Test FloatPercentileZOrderField.") {
    {
      val minVal = -2.211231f
      val maxVal = 3.122345f
      val result =
        ZOrderField.build("FloatColumnName", FloatType, minVal.toDouble, maxVal.toDouble)
      assert(result.isInstanceOf[FloatPercentileZOrderField])
      val col = result.asInstanceOf[FloatPercentileZOrderField]
      assert(col.name.equals("FloatColumnName"))
      assert(col.rawPercentile.head.equals(minVal.toDouble))
      assert(col.rawPercentile.last.equals(maxVal.toDouble))
      assert(col.rawPercentile.size == 2)
      assert(col.bitLen == (16384 - 1).toBinaryString.length)

      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(0 to 13), res)
      }

      {
        val p50 = (maxVal - minVal) / 2.0f + minVal
        val res = col.getBitSet(p50).stream.toArray.toSeq
        assert(res.equals(Seq(13)), res)
      }
      {
        val p10 = (maxVal - minVal) / 10.0f + minVal
        val res = col.getBitSet(p10).stream.toArray.toSeq
        val expected = util.BitSet.valueOf(Array((16384 * 0.1).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val p90 = (maxVal - minVal) * 9.0f / 10.0f + minVal
        val res = col.getBitSet(p90).stream.toArray.toSeq
        val expected = util.BitSet.valueOf(Array((16384 * 0.9).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
    }
    {
      val minVal = Float.MinValue
      val maxVal = Float.MaxValue
      val result =
        ZOrderField.build("FloatColumnName", FloatType, minVal.toDouble, maxVal.toDouble)
      assert(result.isInstanceOf[FloatPercentileZOrderField])
      val col = result.asInstanceOf[FloatPercentileZOrderField]
      assert(col.name.equals("FloatColumnName"))
      assert(col.rawPercentile.head.equals(minVal.toDouble))
      assert(col.rawPercentile.last.equals(maxVal.toDouble))
      assert(col.rawPercentile.size == 2)
      assert(col.bitLen == (16384 - 1).toBinaryString.length)

      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(0 to 13), res)
      }

      {
        val p50 = 0.0f
        val res = col.getBitSet(p50).stream.toArray.toSeq
        assert(res.equals(Seq(13)), res)
      }
      {
        val p10 = minVal * 0.8f
        val res = col.getBitSet(p10).stream.toArray.toSeq
        val expected = util.BitSet.valueOf(Array((16384 * 0.1).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val p90 = maxVal * 0.8f
        val res = col.getBitSet(p90).stream.toArray.toSeq
        val expected = util.BitSet.valueOf(Array((16384 * 0.9).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
    }

    {
      val minVal = 0.0f
      val maxVal = 1000.0f
      val result =
        ZOrderField.build(
          "FloatColumnName",
          FloatType,
          minVal,
          maxVal,
          Seq(10.0, 100.0, 200.0, 500.0),
          quantileEnabled = true)
      assert(result.isInstanceOf[FloatPercentileZOrderField])
      val col = result.asInstanceOf[FloatPercentileZOrderField]
      assert(col.name.equals("FloatColumnName"))
      assert(col.rawPercentile.head.equals(minVal.toDouble))
      assert(col.rawPercentile.last.equals(maxVal.toDouble))
      assert(col.rawPercentile.size == 6)
      assert(col.bitLen == (16384 - 1).toBinaryString.length)

      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(0 to 13), res)
      }

      {
        val value = 500.0f
        val res = col.getBitSet(value).stream.toArray.toSeq
        // value is not 50% percentile, 80% based on the given quantiles.
        val expected =
          util.BitSet.valueOf(Array((16384 * 0.8).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val value = 750.0f
        val res = col.getBitSet(value).stream.toArray.toSeq
        val expected =
          util.BitSet.valueOf(Array((16384 * 0.9).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
      {
        val p90 = 150.0f
        val res = col.getBitSet(p90).stream.toArray.toSeq
        val expected =
          util.BitSet.valueOf(Array((16384 * 0.5).toLong)).stream.toArray.toSeq
        assert(res.equals(expected), res)
      }
    }
  }

  test("Test StringZOrderField.") {
    {
      val minVal = "cat"
      val maxVal = "zookeeper"
      val result = ZOrderField.build("StringColumnName", StringType, minVal, maxVal)
      assert(result.isInstanceOf[StringZOrderField])
      val col = result.asInstanceOf[StringZOrderField]
      assert(col.name.equals("StringColumnName"))
      assert(col.minVal.equals(minVal))
      assert(col.maxVal.equals(maxVal))
      assert(col.bitLen == 29)

      {
        val res = col.getBitSet("dog").stream.toArray.toSeq
        // d - c = 100 - 99 = 0b1 = (0) + 24
        // o = 111 = 0b1101111 = (0, 1, 2, 3, 5, 6) + 16
        // g = 103 = 0b1100111 = (0, 1, 2, 5, 6) + 8
        assert(res.equals(Seq(8, 9, 10, 13, 14, 16, 17, 18, 19, 21, 22, 24)), res)
      }

      {
        val res = col.getBitSet("z").stream.toArray.toSeq
        // z - c = 122 - 99 = 0b10111 = (0, 1, 2, 4) + 24
        assert(res.equals(Seq(24, 25, 26, 28)), res)
      }

      {
        val res = col.getBitSet("zookeeper").stream.toArray.toSeq
        // z - c = 122 - 99 = 0b10111 = (0, 1, 2, 4) + 24
        // o = 111 = 0b1101111 = (0, 1, 2, 3, 5, 6) + 16
        // o = 111 = 0b1101111 = (0, 1, 2, 3, 5, 6) + 8
        // k = 107 = 0b1101011 = (0, 1, 3, 5, 6) + 0
        assert(
          res.equals(
            Seq(0, 1, 3, 5, 6, 8, 9, 10, 11, 13, 14, 16, 17, 18, 19, 21, 22, 24, 25, 26, 28)),
          res)
      }

      {
        // Result should be same as only first 4 bytes are used.
        val res1 = col.getBitSet("deadbeef").stream.toArray.toSeq
        val res2 = col.getBitSet("deadwalking").stream.toArray.toSeq
        assert(res1.equals(res2))
      }

      val e = intercept[HyperspaceException](col.getBitSet("Z"))
      assert(e.msg.contains("value=Z, min=cat, max=zookeeper"))
    }
  }

  test("Test DateMinMaxZOrderField.") {
    def date(str: String): java.sql.Date = {
      java.sql.Date.valueOf(str)
    }

    {
      val minVal = date("2021-03-01")
      val maxVal = date("2021-04-30")
      val result = ZOrderField.build("DateColumnName", DateType, minVal, maxVal)
      assert(result.isInstanceOf[DateMinMaxZOrderField])
      val col = result.asInstanceOf[DateMinMaxZOrderField]
      assert(col.name.equals("DateColumnName"))
      assert(col.bitLen == 6)

      val res = col.getBitSet(date("2021-03-02")).stream.toArray.toSeq
      assert(res.equals(Seq(0)), res)

      val e =
        intercept[HyperspaceException](col.getBitSet(date("2020-02-02")))
      assert(e.msg.contains("value=2020-02-02, min=2021-03-01, max=2021-04-30"))
      val e2 =
        intercept[HyperspaceException](col.getBitSet(date("2022-02-02")))
      assert(e2.msg.contains("value=2022-02-02, min=2021-03-01, max=2021-04-30"))
    }
    {
      val minVal = date("1970-01-01")
      val maxVal = date("2100-01-01")
      val result = ZOrderField.build("DateColumnName", DateType, minVal, maxVal)
      assert(result.isInstanceOf[DateMinMaxZOrderField])
      val col = result.asInstanceOf[DateMinMaxZOrderField]
      assert(col.name.equals("DateColumnName"))
      assert(col.bitLen == 16)

      {
        // 47482 days = 0b1011100101111010
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(Seq(1, 3, 4, 5, 6, 8, 11, 12, 13, 15)), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
      {
        // 365 days = 0b101101101
        val res = col.getBitSet(date("1971-01-01")).stream.toArray.toSeq
        assert(res.equals(Seq(0, 2, 3, 5, 6, 8)), res)
      }
    }
  }

  test("Test TimestampMinMaxZOrderField.") {
    def ts(str: String): java.sql.Timestamp = {
      java.sql.Timestamp.valueOf(str)
    }

    {
      val minVal = ts("2021-03-01 00:00:00")
      val maxVal = ts("2021-03-02 00:00:00")
      val result = ZOrderField.build("TimestampColumnName", TimestampType, minVal, maxVal)
      assert(result.isInstanceOf[TimestampMinMaxZOrderField])
      val col = result.asInstanceOf[TimestampMinMaxZOrderField]
      assert(col.name.equals("TimestampColumnName"))
      // in millis = 1000000 * 60 * 60 * 24 = 86400000000 = 0b1010000011101110101110110000000000000
      assert(col.bitLen == 37)

      val res =
        col.getBitSet(ts("2021-03-01 00:00:00.001")).stream.toArray.toSeq

      // 1000 = 0b1111101000
      assert(res.equals(Seq(3, 5, 6, 7, 8, 9)), res)

      val e =
        intercept[HyperspaceException](col.getBitSet(ts("2020-02-02 00:00:00")))
      assert(
        e.msg.contains(
          "value=2020-02-02 00:00:00.0, min=2021-03-01 00:00:00.0, max=2021-03-02 00:00:00.0"))
      val e2 =
        intercept[HyperspaceException](col.getBitSet(ts("2022-02-02 00:00:00")))
      assert(
        e2.msg.contains(
          "value=2022-02-02 00:00:00.0, min=2021-03-01 00:00:00.0, max=2021-03-02 00:00:00.0"))
    }
    {
      val minVal = ts("1970-01-01 00:00:00.0")
      val maxVal = ts("1970-01-01 00:01:00.0")
      val result = ZOrderField.build("TimestampColumnName", TimestampType, minVal, maxVal)
      assert(result.isInstanceOf[TimestampMinMaxZOrderField])
      val col = result.asInstanceOf[TimestampMinMaxZOrderField]
      assert(col.name.equals("TimestampColumnName"))
      // 1000 * 1000 * 60 = 60000000 = 0b11100100111000011100000000
      assert(col.bitLen == 26)

      {
        val res = col.getBitSet(maxVal).stream.toArray.toSeq
        assert(res.equals(Seq(8, 9, 10, 15, 16, 17, 20, 23, 24, 25)), res)
      }
      {
        val res = col.getBitSet(minVal).stream.toArray.toSeq
        assert(res.equals(Seq()), res)
      }
      {
        val res =
          col.getBitSet(ts("1970-01-01 00:00:00.000001")).stream.toArray.toSeq
        assert(res.equals(Seq(0)), res)
      }
    }
  }
}
