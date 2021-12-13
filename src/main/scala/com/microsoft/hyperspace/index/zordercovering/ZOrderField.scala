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

import java.util.BitSet

import org.apache.spark.sql.catalyst.util
import org.apache.spark.sql.types._

import com.microsoft.hyperspace.HyperspaceException

abstract class ZOrderField extends Serializable {
  val name: String
  val bitLen: Int
  def getBitSet(valueAny: Any): BitSet
  def throwOutOfRangeException(minVal: Any, maxVal: Any, value: Any): Unit = {
    throw HyperspaceException(
      "Unexpected operation on ZOrderField. Value out of range: " +
        s"name=$name, value=$value, min=$minVal, max=$maxVal")
  }

  protected val emptyBitSet = new BitSet
  protected def checkEmptySetEligible(valueAny: Any): Boolean = {
    bitLen == 0 || valueAny == null
  }
}

/**
 * ZOrderField for numeric types based on percentile values of the original column values.
 *
 * If the data is skewed, using the original value for z-address cannot significantly affect
 * the final z-order thus the column values might not be distributed well compared to other
 * evenly distributed z-order columns. To mitigate the issue, we use the percentile based
 * bucket index for z-address.
 *
 * We assign a bucket index (0 to numDistBuckets - 1) for each value based on its percentile
 * and use the index value for z-address. Unlike bucket sorting, all values in a bucket will
 * just have the same bucket index.
 *
 * Steps to calculate the bucket index:
 * 1) define range subsets based on the given approximate percentiles.
 * 2) get the range subset index that a value is belong to.
 * 3) get the ratio where the value is located in the range.
 * 4) return the bucket id using subset index and the ratio.
 *
 * The following example would help to understand why using the approx percentiles.
 * Assume we have double values of
 *   [-10, -9.9, -9.8, -9.7, -1, 0, 1000, 1500, 2000, 3000, 10000, 20000]
 *   - Min: -10
 *   - 25% percentile: -9.8
 *   - 50% percentile: 0
 *   - 75% percentile: 2000
 *   - Max: 20000
 *
 * With the info, value 10000 is belong to the 4th group, [2000, 20000], so we interpolate it as
 * (10000 - 2000) / (20000 - 2000) * 0.25 + 0.75 = 0.44 * 0.25 + 0.75 = 0.86 and give
 * the corresponding index by 0.86 * numDistBuckets. If we used only min/max, then we would end up
 * with (10000 - (-10)) / (20000 - (-10)) * 1.0 = 0.5, which may cause worse result ordering of the
 * column values.
 *
 * In order to figure out the result ordering along with the index distribution, consider
 *   ColA => [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
 *   ColB => [1, 2, 3, 4, 5, 1025, 1026, 1027, 1028, 1029]
 * When z-ordering by colA and colB, 1025 to 1029 values of ColB cannot affect the order.
 * Since their upper bits (0b10000000XXX) are the same while ColA values have all different
 * bits. As Z-ordering defines the order by interleaving bit sequence of z-addresses,
 * ColA would have better result ordering than ColB.
 */
trait PercentileBasedZOrderField[T] extends ZOrderField {
  // Use Seq[Double] as Spark's approxQuantile function returns the result in double type
  // regardless of the original numeric type.
  val rawPercentile: Seq[Double] // [minVal, approx_percentile1, approx_percentile2, ..., maxVal]
  def toDouble(v: T): Double

  // The function is not idempotent.
  private def normalizeVal(d: Double): Double = {
    val v = d match {
      case Double.PositiveInfinity =>
        Double.MaxValue
      case Double.NegativeInfinity =>
        Double.MinValue
      case Double.NaN =>
        Double.MaxValue
      case -0.0 =>
        0.0
      case _ =>
        d
    }
    // Use / 2.0 value since we need to calculate length of range between two doubles.
    // If maxVal is Double.MaxValue and minVal is negative, the length will be inf,
    // so we cannot calculate the percentile of a value properly.
    v / 2.0
  }

  val numDistBuckets: Int
  protected val percentile = rawPercentile.map(normalizeVal)
  private val lengthOfPercentile =
    percentile.zipWithIndex.tail.map(vi => vi._1 - percentile(vi._2 - 1))

  override lazy val bitLen: Int = {
    if (rawPercentile.head == rawPercentile.last) {
      0
    } else {
      (numDistBuckets - 1).toBinaryString.length
    }
  }

  private def getPercentileIdx(value: Double): Int = {
    percentile.tail.indexWhere(p => value <= p)
  }

  // Return the index of buckets that rawValue is belong to.
  private def getBucketId(value: Double): Long = {
    val pIdx = getPercentileIdx(value)

    val diffInPercentile = value - percentile(pIdx) // value - minValInPercentile
    val ratioInPercentile = diffInPercentile / lengthOfPercentile(pIdx)
    val globalPercentile = (pIdx.toDouble + ratioInPercentile) / (percentile.length - 1)
    (globalPercentile * numDistBuckets).toLong.min(numDistBuckets - 1)
  }

  override def getBitSet(valueAny: Any): BitSet = {
    if (checkEmptySetEligible(valueAny)) {
      emptyBitSet
    } else {
      val value = valueAny.asInstanceOf[T]
      // Since Long.toDouble might lose some precision, we don't check the value range here.
      // Instead take min/max to calculate the bucket id properly.
      val doubleValue = normalizeVal(toDouble(value)).max(percentile.head).min(percentile.last)
      BitSet.valueOf(Array(getBucketId(doubleValue)))
    }
  }
}

/**
 * Min/max based z-address calculator for Integer type.
 *
 * Get z-address bits using (value - minValue), after casting to Long.
 * In this way, we could achieve
 *   1) smaller value means less number of bits to represent.
 *   2) (value - min) can't be negative value which requires special handling.
 *   3) Remove meaningless upper bits which can make a better result ordering.
 *
 * For 3), the following example would help to understand:
 *   ColA => [1, 2, 3, 4, 5, 6, 7]
 *   ColB => [1024, 1026, 1027, 1028, 1029, 1030, 1031]
 *
 * If we use the original integer value for z-address, 1024 to 1031 values of ColB cannot
 * affect the order. Since their upper bits (0b1000000XXXX) are the same while ColA values
 * have all different bits. As a result, ColA would have better result ordering than ColB
 * because Z-ordering defines the order by interleaving bit sequence of z-addresses.
 * The result cannot satisfy the purpose of z-ordering; the data of both columns are clustered
 * in some degree, so that we could skip uninterested data for the both columns.
 *
 * Using (value - min) can mitigate the issue:
 *   ColA => [0, 1, 2, 3, 4, 5, 6]
 *   ColB => [0, 2, 3, 4, 5, 6, 7]
 *
 * Now both column don't have the meaningless bits, they can contribute the order fairly.
 */
trait IntegerMinMaxBasedZOrderField[T] extends ZOrderField {
  val minVal: T
  val maxVal: T
  implicit val n: Numeric[T]

  override lazy val bitLen: Int = {
    if (minVal == maxVal) {
      0
    } else {
      (n.toLong(maxVal) - n.toLong(minVal)).toBinaryString.length
    }
  }
  private lazy val minLong = n.toLong(minVal)

  override def getBitSet(valueAny: Any): BitSet = {
    if (checkEmptySetEligible(valueAny)) {
      emptyBitSet
    } else {
      val value = valueAny.asInstanceOf[T]
      if (n.lt(value, minVal) || n.gt(value, maxVal)) {
        throwOutOfRangeException(minVal, maxVal, value)
      }
      BitSet.valueOf(Array(n.toLong(value) - minLong))
    }
  }
}

/**
 * For Integer types (Long, Int, Short, Byte), use (value - minValue) for z-address.
 * See [[IntegerMinMaxBasedZOrderField]] for detail.
 */
case class LongMinMaxZOrderField(override val name: String, minVal: Long, maxVal: Long)(implicit
    val n: Numeric[Long])
    extends IntegerMinMaxBasedZOrderField[Long] {}

case class IntMinMaxZOrderField(override val name: String, minVal: Int, maxVal: Int)(implicit
    val n: Numeric[Int])
    extends IntegerMinMaxBasedZOrderField[Int] {}

case class ShortMinMaxZOrderField(override val name: String, minVal: Short, maxVal: Short)(
    implicit val n: Numeric[Short])
    extends IntegerMinMaxBasedZOrderField[Short] {}

case class ByteMinMaxZOrderField(override val name: String, minVal: Byte, maxVal: Byte)(implicit
    val n: Numeric[Byte])
    extends IntegerMinMaxBasedZOrderField[Byte] {}

/**
 * For Numeric types (Long, Int, Short, Byte, Double, Float), if optimize.zorder.quantile.enabled
 * is true, divide the values into buckets based on approximate percentiles.
 * Use the bucket idx for z-address for better distribution in case of skewed data.
 */
case class LongPercentileZOrderField(override val name: String, rawPercentile: Seq[Double])
    extends PercentileBasedZOrderField[Long] {
  override def toDouble(n: Long): Double = n.doubleValue()
  // Set the max of bucket number as (1L << 15) to reduce the length of z-address.
  // We need to select a number that is larger than the total number of files or parquet row groups.
  // It could be a larger value, but it doesn't make much difference on the quality of result.
  private val defaultNumDistBuckets = 1 << 15
  override lazy val numDistBuckets = {
    // min(32768, maxVal - minVal + 1)
    (percentile.last - percentile.head + 1).toLong.min(defaultNumDistBuckets).toInt
  }
}

case class IntPercentileZOrderField(override val name: String, rawPercentile: Seq[Double])
    extends PercentileBasedZOrderField[Int] {
  override def toDouble(n: Int): Double = n.doubleValue()
  private val defaultNumDistBuckets = 1 << 14
  override lazy val numDistBuckets = {
    // min(16384, maxVal - minVal + 1)
    (percentile.last - percentile.head + 1).toLong.min(defaultNumDistBuckets).toInt
  }
}

case class ShortPercentileZOrderField(override val name: String, rawPercentile: Seq[Double])
    extends PercentileBasedZOrderField[Short] {
  override def toDouble(n: Short): Double = n.doubleValue()
  private val defaultNumDistBuckets = 1 << 10
  override lazy val numDistBuckets = {
    // min(2048, maxVal - minVal + 1)
    (percentile.last - percentile.head + 1).toInt.min(defaultNumDistBuckets)
  }
}

case class BytePercentileZOrderField(override val name: String, rawPercentile: Seq[Double])
    extends PercentileBasedZOrderField[Byte] {
  override def toDouble(n: Byte): Double = n.doubleValue()
  private val defaultNumDistBuckets = 1 << 5
  override lazy val numDistBuckets = {
    // min(64, maxVal - minVal + 1)
    (percentile.last.toByte - percentile.head.toByte + 1).min(defaultNumDistBuckets)
  }
}

case class DoublePercentileZOrderField(override val name: String, rawPercentile: Seq[Double])
    extends PercentileBasedZOrderField[Double] {
  private val defaultNumDistBuckets = 1 << 15
  override def toDouble(n: Double): Double = n.doubleValue()
  // For Double, do not take (maxVal - minVal + 1) as it can be less than 1.
  override lazy val numDistBuckets = defaultNumDistBuckets
}

case class FloatPercentileZOrderField(override val name: String, rawPercentile: Seq[Double])
    extends PercentileBasedZOrderField[Float] {
  override def toDouble(n: Float): Double = n.doubleValue()
  private val defaultNumDistBuckets = 1 << 14
  // For Float, do not take (maxVal - minVal + 1) as it can be less than 1.
  override val numDistBuckets = defaultNumDistBuckets
}

/**
 * For Boolean, use 1 bit for true or false.
 */
case class BooleanZOrderField(override val name: String, minVal: Boolean, maxVal: Boolean)
    extends ZOrderField {
  override val bitLen: Int = {
    1
  }

  private val one = {
    val b = new BitSet()
    b.set(0)
    b
  }

  override def getBitSet(valueAny: Any): BitSet = {
    if (checkEmptySetEligible(valueAny)) {
      emptyBitSet
    } else {
      val value = valueAny.asInstanceOf[Boolean]
      if (value) {
        one
      } else {
        emptyBitSet
      }
    }
  }
}

/**
 * For DecimalType, only use the unscaled value of (value - minValue) as
 * assume that scale will be the same for all values.
 */
case class DecimalZOrderField(
    override val name: String,
    minVal: java.math.BigDecimal,
    maxVal: java.math.BigDecimal)
    extends ZOrderField {

  override val bitLen: Int = {
    maxVal.subtract(minVal).unscaledValue.bitLength()
  }

  override def getBitSet(valueAny: Any): BitSet = {
    if (checkEmptySetEligible(valueAny)) {
      emptyBitSet
    } else {
      val value = valueAny.asInstanceOf[java.math.BigDecimal]
      if (value.unscaledValue().compareTo(maxVal.unscaledValue()) == 1 ||
        value.unscaledValue().compareTo(minVal.unscaledValue()) == -1) {
        throwOutOfRangeException(minVal, maxVal, value)
      }
      BitSet.valueOf(value.subtract(minVal).unscaledValue().toByteArray.reverse)
    }
  }
}

/**
 * For String,
 *   1) get first diff character between minVal and maxVal.
 *   2) from that char, use 4 char to construct z-address.
 *   3) keep the first diff char of minVal, so that we can remove
 *     the same upper bit sequence efficiently.
 */
case class StringZOrderField(override val name: String, minVal: String, maxVal: String)
    extends ZOrderField {

  // Find first diff index between minVal and maxVal.
  val firstDiffIdx = minVal.zip(maxVal).takeWhile(Function.tupled(_ == _)).length

  // Compare 4 chars from firstDiffIdx.
  val byteLen: Int = 4

  val minFirstDiff: Byte = if (minVal.length == firstDiffIdx) {
    0.toByte
  } else {
    minVal(firstDiffIdx).toByte
  }

  override val bitLen: Int = {
    // TODO Add a config for byteLen to construct z-address. If all other values
    //  except for maxVal begins with "asdf" and maxVal is "zoo", the result layout
    //  cannot be affected by the order of the string values.

    if (maxVal.equals(minVal)) {
      0
    } else {
      (maxVal(firstDiffIdx).toByte - minFirstDiff).toBinaryString.length + ((byteLen - 1) * 8)
    }
  }

  override def getBitSet(valueAny: Any): BitSet = {
    if (checkEmptySetEligible(valueAny)) {
      emptyBitSet
    } else {
      val valueStr = valueAny.asInstanceOf[String]

      if (valueStr > maxVal || valueStr < minVal) {
        throwOutOfRangeException(minVal, maxVal, valueStr)
      }
      val value = valueStr.substring(firstDiffIdx.min(valueStr.length))
      val len = math.min(byteLen, value.length)
      val padArr = Seq.fill(byteLen - len) { 0.toByte }
      // For the first diff char, use (the char - minFirstDiff)
      // to remove same upper bit sequence.
      val firstByte = if (value.nonEmpty) {
        (value.head - minFirstDiff).toByte
      } else {
        0.toByte
      }
      BitSet.valueOf((firstByte +: (value.slice(1, len).getBytes ++ padArr)).reverse)
    }
  }
}

/**
 * For Timestamp type, handle it as Long type.
 * See [[LongMinMaxZOrderField]].
 *
 * TODO: Support PercentileBasedZOrderField for Timestamp and DateType.
 *   Spark cannot calculate quantiles for non-numeric type, need to cast to Long type
 *   before collecting stats.
 */
case class TimestampMinMaxZOrderField(
    override val name: String,
    minVal: java.sql.Timestamp,
    maxVal: java.sql.Timestamp)
    extends ZOrderField {

  val longField =
    LongMinMaxZOrderField(
      name,
      util.DateTimeUtils.fromJavaTimestamp(minVal),
      util.DateTimeUtils.fromJavaTimestamp(maxVal))

  override val bitLen: Int = {
    longField.bitLen
  }

  override def getBitSet(valueAny: Any): BitSet = {
    if (checkEmptySetEligible(valueAny)) {
      emptyBitSet
    } else {
      val value = valueAny.asInstanceOf[java.sql.Timestamp]
      if (value.compareTo(minVal) < 0 || value.compareTo(maxVal) > 0) {
        throwOutOfRangeException(minVal, maxVal, value)
      }
      val valueTime = util.DateTimeUtils.fromJavaTimestamp(value)
      longField.getBitSet(valueTime)
    }
  }
}

/**
 * For Date type, handle it as Int type after converting it as the number of days.
 * See [[IntZOrderField]].
 */
case class DateMinMaxZOrderField(
    override val name: String,
    minVal: java.sql.Date,
    maxVal: java.sql.Date)
    extends ZOrderField {

  val intField =
    IntMinMaxZOrderField(
      name,
      util.DateTimeUtils.fromJavaDate(minVal),
      util.DateTimeUtils.fromJavaDate(maxVal))

  override val bitLen: Int = {
    intField.bitLen
  }

  override def getBitSet(valueAny: Any): BitSet = {
    if (checkEmptySetEligible(valueAny)) {
      emptyBitSet
    } else {
      val value = valueAny.asInstanceOf[java.sql.Date]
      if (value.compareTo(minVal) < 0 || value.compareTo(maxVal) > 0) {
        throwOutOfRangeException(minVal, maxVal, value)
      }
      val valueDay = util.DateTimeUtils.fromJavaDate(value)
      intField.getBitSet(valueDay)
    }
  }
}

object ZOrderField {
  def build(
      name: String,
      dataType: DataType,
      minVal: Any,
      maxVal: Any,
      quantiles: Seq[Any] = Nil,
      quantileEnabled: Boolean = false): ZOrderField = {
    dataType match {
      case LongType =>
        if (quantileEnabled) {
          LongPercentileZOrderField(
            name,
            (minVal +: quantiles :+ maxVal).asInstanceOf[Seq[Double]])
        } else {
          LongMinMaxZOrderField(name, minVal.asInstanceOf[Long], maxVal.asInstanceOf[Long])
        }
      case IntegerType =>
        if (quantileEnabled) {
          IntPercentileZOrderField(
            name,
            (minVal +: quantiles :+ maxVal).asInstanceOf[Seq[Double]])
        } else {
          IntMinMaxZOrderField(name, minVal.asInstanceOf[Int], maxVal.asInstanceOf[Int])
        }
      case ShortType =>
        if (quantileEnabled) {
          ShortPercentileZOrderField(
            name,
            (minVal +: quantiles :+ maxVal).asInstanceOf[Seq[Double]])
        } else {
          ShortMinMaxZOrderField(name, minVal.asInstanceOf[Short], maxVal.asInstanceOf[Short])
        }
      case ByteType =>
        if (quantileEnabled) {
          BytePercentileZOrderField(
            name,
            (minVal +: quantiles :+ maxVal).asInstanceOf[Seq[Double]])
        } else {
          ByteMinMaxZOrderField(name, minVal.asInstanceOf[Byte], maxVal.asInstanceOf[Byte])
        }
      case DoubleType =>
        if (quantileEnabled) {
          DoublePercentileZOrderField(
            name,
            (minVal +: quantiles :+ maxVal).asInstanceOf[Seq[Double]])
        } else {
          DoublePercentileZOrderField(
            name,
            Seq(minVal.asInstanceOf[Double], maxVal.asInstanceOf[Double]))
        }
      case FloatType =>
        // minVal, maxVal can be Float.
        val (minValue, maxValue) = if (minVal.isInstanceOf[Float]) {
          (minVal.asInstanceOf[Float].toDouble, maxVal.asInstanceOf[Float].toDouble)
        } else {
          (minVal.asInstanceOf[Double], maxVal.asInstanceOf[Double])
        }
        if (quantileEnabled) {
          FloatPercentileZOrderField(
            name,
            (minValue +: quantiles :+ maxValue).asInstanceOf[Seq[Double]])
        } else {
          FloatPercentileZOrderField(name, Seq(minValue, maxValue))
        }
      case DecimalType() =>
        DecimalZOrderField(
          name,
          minVal.asInstanceOf[java.math.BigDecimal],
          maxVal.asInstanceOf[java.math.BigDecimal])
      case StringType =>
        StringZOrderField(name, minVal.asInstanceOf[String], maxVal.asInstanceOf[String])
      case BooleanType =>
        BooleanZOrderField(name, minVal.asInstanceOf[Boolean], maxVal.asInstanceOf[Boolean])
      case TimestampType =>
        TimestampMinMaxZOrderField(
          name,
          minVal.asInstanceOf[java.sql.Timestamp],
          maxVal.asInstanceOf[java.sql.Timestamp])
      case DateType =>
        DateMinMaxZOrderField(
          name,
          minVal.asInstanceOf[java.sql.Date],
          maxVal.asInstanceOf[java.sql.Date])
      case _ =>
        throw HyperspaceException("Unsupported data type: " + maxVal.getClass)
    }
  }

  /**
   * Return true for numeric type.
   */
  def percentileApplicableType(dataType: DataType): Boolean = {
    Seq(DoubleType, FloatType, LongType, IntegerType, ShortType, ByteType)
      .contains(dataType)
  }
}
