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

package com.microsoft.hyperspace.index

import org.apache.spark.SparkFunSuite

import com.microsoft.hyperspace.{Hyperspace, SparkInvolvedSuite}

trait HyperspaceSuite extends SparkFunSuite with SparkInvolvedSuite {
  override def beforeAll(): Unit = {
    super.beforeAll()
    clearCache()
  }

  override def afterAll(): Unit = {
    clearCache()
    super.afterAll()
  }

  protected def clearCache(): Unit = {
    Hyperspace.getContext(spark).indexCollectionManager match {
      case cachingManager: CachingIndexCollectionManager =>
        cachingManager.clearCache()
      case _ =>
    }
  }

  /**
   * Drops table `tableName` after calling `f`.
   */
  protected def withTable(tableNames: String*)(f: => Unit): Unit = {
    try f
    finally {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
      }
    }
  }

  /**
   * Drops view `viewName` after calling `f`.
   */
  protected def withView(viewNames: String*)(f: => Unit): Unit = {
    try f
    finally {
      viewNames.foreach { name =>
        spark.sql(s"DROP VIEW IF EXISTS $name")
      }
    }
  }

  protected def withSparkConf(confName: String, confValue: Any)(f: => Unit): Unit = {
    val original = spark.conf.get(confName)
    try {
      spark.conf.set(confName, confValue.toString)
      f
    } finally {
      spark.conf.set(confName, original)
    }
  }
}
