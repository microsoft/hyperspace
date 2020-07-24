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

using Microsoft.Spark.Extensions.Hyperspace;
using Microsoft.Spark.Extensions.Hyperspace.Index;
using Microsoft.Spark.Sql;
using Microsoft.Spark.Sql.Types;
using System.Collections.Generic;

namespace HyperspaceApp
{
    public class Program
    {
        public static void Main(string[] args)
        {
            // Create Spark session.
            SparkSession spark = SparkSession
                .Builder()
                .AppName("Hyperspace example")
                .Config("spark.some.config.option", "some-value")
                .GetOrCreate();

            // Sample department records.
            var departments = new List<GenericRow>() {
                new GenericRow(new object[] { 10, "Accounting", "New York" }),
                new GenericRow(new object[] { 20, "Research", "Dallas"}),
                new GenericRow(new object[] { 30, "Sales", "Chicago"}),
                new GenericRow(new object[] { 40, "Operations", "Boston"})
            };

            // Sample employee records.
            var employees = new List<GenericRow>() {
                new GenericRow(new object[] { 7369, "SMITH", 20 }),
                new GenericRow(new object[] { 7499, "ALLEN", 30 }),
                new GenericRow(new object[] { 7521, "WARD", 30 }),
                new GenericRow(new object[] { 7566, "JONES", 20 }),
                new GenericRow(new object[] { 7698, "BLAKE", 30 }),
                new GenericRow(new object[] { 7782, "CLARK", 10 }),
                new GenericRow(new object[] { 7788, "SCOTT", 20 }),
                new GenericRow(new object[] { 7839, "KING", 10 }),
                new GenericRow(new object[] { 7844, "TURNER", 30 }),
                new GenericRow(new object[] { 7876, "ADAMS", 20 }),
                new GenericRow(new object[] { 7900, "JAMES", 30 }),
                new GenericRow(new object[] { 7934, "MILLER", 10 }),
                new GenericRow(new object[] { 7902, "FORD", 20 }),
                new GenericRow(new object[] { 7654, "MARTIN", 30 })
            };

            // Save example data records as Parquet.
            string deptLocation = "departments";
            spark.CreateDataFrame(departments, new StructType(new List<StructField>()
                {
                    new StructField("deptId", new IntegerType()),
                    new StructField("deptName", new StringType()),
                    new StructField("location", new StringType())
                }))
                .Write()
                .Mode("overwrite")
                .Parquet(deptLocation);

            string empLocation = "employees";
            spark.CreateDataFrame(employees, new StructType(new List<StructField>()
                {
                    new StructField("empId", new IntegerType()),
                    new StructField("empName", new StringType()),
                    new StructField("deptId", new IntegerType())
                }))
                .Write()
                .Mode("overwrite")
                .Parquet(empLocation);

            // Create Hyperspace indexes.
            var hyperspace = new Hyperspace(spark);

            DataFrame deptDF = spark.Read().Parquet(deptLocation);
            DataFrame empDF = spark.Read().Parquet(empLocation);

            var deptIndexConfig = new IndexConfig(
                "deptIndex",
                new[] { "deptId" },
                new[] { "deptName" });
            var empIndexConfig = new IndexConfig("empIndex",
                new[] { "deptId" },
                new[] { "empName" });
            hyperspace.CreateIndex(deptDF, deptIndexConfig);
            hyperspace.CreateIndex(empDF, empIndexConfig);

            // List all indexes.
            hyperspace.Indexes().Show();

            // Enable Hyperspace to leverage indexes.
            spark.EnableHyperspace();

            // Example of index usage for filtered selection.
            DataFrame eqFilter = deptDF.Filter("deptId = 20").Select("deptName");
            eqFilter.Show();
            hyperspace.Explain(eqFilter, false);

            // Example of index usage for join.
            DataFrame eqJoin = empDF
                .Join(deptDF, "deptId")
                .Select(empDF.Col("empName"), deptDF.Col("deptName"));
            eqJoin.Show();
            hyperspace.Explain(eqJoin, false);

            // Stop Spark session.
            spark.Stop();
        }
    }
}
