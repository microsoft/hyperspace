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

import sbt._

object Dependencies {
  def deps(sparkVersion: Version) = {
    val sv = sparkVersion.toString
    Seq(
      "org.apache.spark" %% "spark-sql" % sv % "provided" withSources (),
      "org.apache.spark" %% "spark-core" % sv % "provided" withSources (),
      "org.apache.spark" %% "spark-catalyst" % sv % "provided" withSources (),
      // Test dependencies
      "org.scalatest" %% "scalatest" % "3.0.5" % "test",
      "org.mockito" %% "mockito-scala" % "0.4.0" % "test",
      "org.apache.spark" %% "spark-catalyst" % sv % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sv % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sv % "test" classifier "tests") ++
      (sparkVersion match {
        case Version(2, _, _) =>
          Seq(
            "io.delta" %% "delta-core" % "0.6.1" % "provided" withSources (),
            "org.apache.iceberg" % "iceberg-spark-runtime" % "0.11.0" % "provided" withSources ())
        case Version(3, _, _) =>
          Seq(
            "io.delta" %% "delta-core" % "0.8.0" % "provided" withSources (),
            "org.apache.iceberg" % "iceberg-spark3-runtime" % "0.11.1" % "provided" withSources (),
            "org.apache.hive" % "hive-metastore" % "2.3.8" % "test")
        case _ =>
          throw new Exception("Only Spark 2 or 3 is supported")
      })
  }
}
