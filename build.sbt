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

name := "hyperspace"

organization in ThisBuild := "com.microsoft"

lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"
lazy val supportedScalaVersions = List(scala212, scala211)

lazy val sparkVersion = "2.4.2"

scalaVersion in ThisBuild := scala212

crossScalaVersions in ThisBuild := supportedScalaVersions

lazy val core = project
  .settings(
    libraryDependencies ++= commonDependencies
  )

lazy val commonDependencies = Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided",

  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.mockito" %% "mockito-scala" % "0.4.0" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion % "test" classifier "tests",
  "org.apache.spark" %% "spark-hive" % sparkVersion % "test" classifier "tests"
)

scalacOptions in ThisBuild ++= Seq(
  "-target:jvm-1.8"
)

javaOptions in ThisBuild += "-Xmx1024m"

/********************************
 * Tests related configurations *
 ********************************/
// Tests cannot be run in parallel since mutiple Spark contexts cannot run in the same JVM.
parallelExecution in (ThisBuild, Test) := false

fork in (ThisBuild, Test) := true

javaOptions in (ThisBuild, Test) ++= Seq(
  "-Dspark.ui.enabled=false",
  "-Dspark.ui.showConsoleProgress=false",
  "-Xmx1024m"
)

