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

package com.microsoft.hyperspace.goldstandard

import java.io.File

import org.apache.hadoop.fs.Path

import com.microsoft.hyperspace._
import com.microsoft.hyperspace.goldstandard.IndexLogEntryCreator.createIndex
import com.microsoft.hyperspace.index.IndexConstants.INDEX_SYSTEM_PATH
import com.microsoft.hyperspace.util.FileUtils

class TPCDS_Hyperspace extends PlanStabilitySuite {

  override val goldenFilePath: String =
    new File(baseResourcePath, "hyperspace/approved-plans-v1_4").getAbsolutePath

  val indexSystemPath = new File(baseResourcePath, "hyperspace/indexes").toString

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(INDEX_SYSTEM_PATH, indexSystemPath)
    spark.enableHyperspace()
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(indexSystemPath))
    super.afterAll()
  }

  tpcdsQueries.foreach { q =>
    test(s"check simplified (tpcds-v1.4/$q)") {

      val indexes = Seq(
        "dtindex;date_dim;d_date_sk;d_year",
        "ssIndex;store_sales;ss_sold_date_sk;ss_customer_sk")
      indexes.foreach(createIndex(_, spark))

      // Enable cross join because some queries fail during query optimization phase.
      withSQLConf(
        ("spark.sql.crossJoin.enabled" -> "true"),
        ("spark.sql.autoBroadcastJoinThreshold" -> "-1")) {
        testQuery("tpcds/queries", q)
      }
    }
  }
}
