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

package com.microsoft.hyperspace.recommendation.cost

import org.apache.spark.sql.catalyst.optimizer.Cost

import com.microsoft.hyperspace.recommendation.RecommendationConstants

/**
 * An interface that all costs should implement.
 */
trait GenericCost {

  // Invalid cost.
  val INVALID_COST: Double = -1.0

  /**
   * Get cost as a double. Subclasses should override this.
   *
   * @return The cost as a double.
   */
  def getCost: Double = INVALID_COST
}

/**
 * Simple cost based on the given cost value (i.e., just a wrapper).
 *
 * @param cost The cost value in double.
 */
case class SimpleCost(cost: Double) extends GenericCost {

  override def getCost: Double = {
    // if (cost < 0) INVALID_COST else cost
    cost
  }
}

/**
 * Cost based on the "size" given by the default Spark [[Cost]].
 *
 * @param cost The default Spark [[Cost]].
 */
case class SizeBasedCost(cost: Cost) extends GenericCost {

  override def getCost: Double = {
    // Simply cast the "size" as a double.
    cost.size.toDouble
  }
}

/**
 * Cost based on the "card" given by the default Spark [[Cost]].
 *
 * @param cost The default Spark [[Cost]].
 */
case class CardBasedCost(cost: Cost) extends GenericCost {

  override def getCost: Double = {
    // Simply cast the "card" as a double.
    cost.card.toDouble
  }
}

/**
 * Improvement of costs.
 */
case class CostImprovement(oldCost: Option[GenericCost], newCost: Option[GenericCost]) {

  override def toString: String = {
    if (oldCost.isDefined && newCost.isDefined) {
      s"[old cost: ${oldCost.get.getCost}, new cost: ${newCost.get.getCost}, " +
        s"improvement: $getImprovement]"
    } else {
      s"[old cost: N/A, new cost: N/A, improvement: N/A"
    }
  }

  /**
   * Compute improvement in terms of estimated costs.
   *
   * @return improvement in percentage.
   */
  def getImprovement: Option[Double] = {
    if (oldCost.isDefined && newCost.isDefined) {
      if (oldCost.get.getCost == 0) {
        return Some(0.0)
      }
      // Compute improvement with respect to the old cost.
      return Some(
        100 * (oldCost.get.getCost - newCost.get.getCost) / Math.abs(oldCost.get.getCost))
    }
    None
  }
}

/**
 * A factory for making costs.
 */
object CostFactory {

  def makeCost(costType: String, cost: Cost): GenericCost = {
    costType match {
      case RecommendationConstants.COST_METRIC_CARD => CardBasedCost(cost)
      case _ => SizeBasedCost(cost)
    }
  }

  def makeCost(cost: Double): GenericCost = {
    SimpleCost(cost)
  }
}
