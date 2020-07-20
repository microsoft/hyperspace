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

import org.apache.spark.sql.{DataFrame, SparkSession}

import com.microsoft.hyperspace._
import com.microsoft.hyperspace.Hyperspace
import com.microsoft.hyperspace.index.IndexConfig

object App {
  def main(args: Array[String]): Unit = {
    // Create Spark session
    val spark = SparkSession
      .builder()
      .appName("Hyperspace example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()

    // Sample department records
    val departments = Seq(
      (10, "Accounting", "New York"),
      (20, "Research", "Dallas"),
      (30, "Sales", "Chicago"),
      (40, "Operations", "Boston"))

    // Sample employee records
    val employees = Seq(
      (7369, "SMITH", 20),
      (7499, "ALLEN", 30),
      (7521, "WARD", 30),
      (7566, "JONES", 20),
      (7698, "BLAKE", 30),
      (7782, "CLARK", 10),
      (7788, "SCOTT", 20),
      (7839, "KING", 10),
      (7844, "TURNER", 30),
      (7876, "ADAMS", 20),
      (7900, "JAMES", 30),
      (7934, "MILLER", 10),
      (7902, "FORD", 20),
      (7654, "MARTIN", 30))

    // Save example data records as Parquet
    import spark.implicits._
    val deptLocation = "departments"
    val empLocation = "employees"
    departments
      .toDF("deptId", "deptName", "location")
      .write
      .mode("overwrite")
      .parquet(deptLocation)

    employees
      .toDF("empId", "empName", "deptId")
      .write
      .mode("overwrite")
      .parquet(empLocation)

    // Create Hyperspace indexes
    val hyperspace = new Hyperspace(spark)

    val deptDF: DataFrame = spark.read.parquet(deptLocation)
    val empDF: DataFrame = spark.read.parquet(empLocation)

    val deptIndexConfig: IndexConfig = IndexConfig("deptIndex", Seq("deptId"), Seq("deptName"))
    val empIndexConfig: IndexConfig = IndexConfig("empIndex", Seq("deptId"), Seq("empName"))
    hyperspace.createIndex(deptDF, deptIndexConfig)
    hyperspace.createIndex(empDF, empIndexConfig)


    // List all indexes
    hyperspace.indexes.show

    // Enable Hyperspace to leverage indexes
    spark.enableHyperspace()

    // Example of index usage for filtered selection
    val eqFilter: DataFrame = deptDF.filter("deptId = 20").select("deptName")
    eqFilter.show()
    hyperspace.explain(eqFilter)

    // Example of index usage for join
    val eqJoin: DataFrame =
      empDF
        .join(deptDF, empDF("deptId") === deptDF("deptId"))
        .select(empDF("empName"), deptDF("deptName"))
    eqJoin.show()
    hyperspace.explain(eqJoin)
  }
}
