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

    val indexes = Seq(
      "dtindex;date_dim;d_date_sk;d_year",
      "ssIndex;store_sales;ss_sold_date_sk;ss_customer_sk")
    indexes.foreach(i => createIndex(IndexDefinition.fromString(i), spark))
  }

  override def afterAll(): Unit = {
    FileUtils.delete(new Path(indexSystemPath))
    super.afterAll()
  }

  tpcdsQueries.foreach { q =>
    test(s"check simplified (tpcds-v1.4/$q)") {
      // Enable cross join because some queries fail during query optimization phase.
      withSQLConf(
        ("spark.sql.crossJoin.enabled" -> "true"),
        ("spark.sql.autoBroadcastJoinThreshold" -> "-1")) {
        testQuery("tpcds/queries", q)
      }
    }
  }
}

case class IndexDefinition(
    name: String,
    indexedCols: Seq[String],
    includedCols: Seq[String],
    tableName: String) {
}

object IndexDefinition {
  /**
   * Index definition from conf files should be provided in the following format:
   * "index-name;table-name;comma-separated-indexed-cols;comma-separated-included-cols"
   * @param definition: Index definition in string representation mentioned above.
   * @return IndexDefinition.
   */
  def fromString(definition: String): IndexDefinition = {
    val splits = definition.split(";")
    val indexName = splits(0)
    val tableName = splits(1)
    val indexCols = splits(2).split(",").toSeq
    val includedCols = splits(3).split(",").toSeq
    IndexDefinition(indexName, indexCols, includedCols, tableName)
  }
}
