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
      "org.apache.spark" %% "spark-catalyst" % sv % "provided" withSources (),
      "org.apache.spark" %% "spark-core" % sv % "provided" withSources (),
      "org.apache.spark" %% "spark-sql" % sv % "provided" withSources (),
      // Test dependencies
      "org.mockito" %% "mockito-scala" % "0.4.0" % "test",
      "org.scalacheck" %% "scalacheck" % "1.14.2" % "test",
      "org.apache.spark" %% "spark-catalyst" % sv % "test" classifier "tests",
      "org.apache.spark" %% "spark-core" % sv % "test" classifier "tests",
      "org.apache.spark" %% "spark-sql" % sv % "test" classifier "tests") ++
      (if (sparkVersion < Version(3, 1, 0))
         Seq("org.scalatest" %% "scalatest" % "3.0.8" % "test")
       else
         Seq(
           "org.scalatest" %% "scalatest" % "3.2.3" % "test",
           "org.scalatestplus" %% "scalatestplus-scalacheck" % "3.1.0.0-RC2" % "test")) ++
      (if (sparkVersion < Version(3, 0, 0))
         Seq(
           "io.delta" %% "delta-core" % "0.6.1" % "provided" withSources (),
           "org.apache.iceberg" % "iceberg-spark-runtime" % "0.11.0" % "provided" withSources ())
       else
         Seq(
           "io.delta" %% "delta-core" % "0.8.0" % "provided" withSources (),
           "org.apache.iceberg" % "iceberg-spark3-runtime" % "0.11.1" % "provided" withSources (),
           "org.apache.hive" % "hive-metastore" % "2.3.8" % "test"))
  }
}
