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

name := "hyperspace-core"

sparkVersion := "3.0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "provided" withSources (),
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "provided" withSources (),
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "provided" withSources (),
  "io.delta" %% "delta-core" % "0.8.0" % "provided" withSources (),
  "org.apache.iceberg" % "iceberg-spark3-runtime" % "0.11.0" % "provided" withSources (),
  // Test dependencies
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "org.mockito" %% "mockito-scala" % "0.4.0" % "test",
  "org.apache.spark" %% "spark-catalyst" % sparkVersion.value % "test" classifier "tests",
  "org.apache.spark" %% "spark-core" % sparkVersion.value % "test" classifier "tests",
  "org.apache.spark" %% "spark-sql" % sparkVersion.value % "test" classifier "tests")

assemblyMergeStrategy in assembly := {
  case PathList("run-tests.py") => MergeStrategy.first
  case x => (assemblyMergeStrategy in assembly).value(x)
}

scalacOptions ++= Seq("-target:jvm-1.8")

javaOptions += "-Xmx1024m"

// The following creates target/scala-2.*/src_managed/main/sbt-buildinfo/BuildInfo.scala.
lazy val root = (project in file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "com.microsoft.hyperspace")

/**
 * ScalaStyle configurations
 */
scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

// Run as part of compile task.
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")
compileScalastyle := scalastyle.in(Compile).toTask("").value
(compile in Compile) := ((compile in Compile) dependsOn compileScalastyle).value

// Run as part of test task.
lazy val testScalastyle = taskKey[Unit]("testScalastyle")
testScalastyle := scalastyle.in(Test).toTask("").value
(test in Test) := ((test in Test) dependsOn testScalastyle).value

/**
 * Spark Packages settings
 */
spName := "microsoft/hyperspace-core"

spAppendScalaVersion := true

spIncludeMaven := true

spIgnoreProvided := true

packageBin in Compile := spPackage.value

/**
 * Test configurations
 */
// Tests cannot be run in parallel since mutiple Spark contexts cannot run in the same JVM.
parallelExecution in Test := false

fork in Test := true

javaOptions in Test ++= Seq(
  "-Dspark.ui.enabled=false",
  "-Dspark.ui.showConsoleProgress=false",
  "-Dspark.databricks.delta.snapshotPartitions=2",
  "-Dspark.sql.shuffle.partitions=5",
  "-Ddelta.log.cacheSize=3",
  "-Dspark.sql.sources.parallelPartitionDiscovery.parallelism=5",
  "-Xmx1024m")

/**
 * Release configurations
 */
organization := "com.microsoft.hyperspace"
organizationName := "Microsoft"
organizationHomepage := Some(url("http://www.microsoft.com/"))

releaseCrossBuild := true

scmInfo := Some(
  ScmInfo(
    url("https://github.com/microsoft/hyperspace"),
    "scm:git@github.com:microsoft/hyperspace.git"))

developers := List(
  Developer(
    id = "rapoth",
    name = "Rahul Potharaju",
    email = "",
    url = url("https://github.com/rapoth")),
  Developer(
    id = "imback82",
    name = "Terry Kim",
    email = "",
    url = url("https://github.com/imback82")),
  Developer(
    id = "apoorvedave1",
    name = "Apoorve Dave",
    email = "",
    url = url("https://github.com/apoorvedave1")),
  Developer(
    id = "AFFogarty",
    name = "Andrew Fogarty",
    email = "",
    url = url("https://github.com/AFFogarty")),
  Developer(
    id = "laserljy",
    name = "Jiying Li",
    email = "",
    url = url("https://github.com/laserljy")),
  Developer(
    id = "sezruby",
    name = "Eunjin Song",
    email = "",
    url = url("https://github.com/sezruby")),
  Developer(
    id = "thugsatbay",
    name = "Gurleen Singh",
    email = "",
    url = url("https://github.com/thugsatbay")))

description := "Hyperspace: An Indexing Subsystem for Apache Spark"
licenses := List("Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
homepage := Some(url("https://github.com/microsoft/hyperspace"))

// Remove all additional repository other than Maven Central from POM
pomIncludeRepository := { _ =>
  false
}
publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) Some("snapshots" at nexus + "content/repositories/snapshots")
  else Some("releases" at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

import ReleaseTransformations._

releasePublishArtifactsAction := PgpKeys.publishSigned.value

releaseProcess := Seq[ReleaseStep](
  checkSnapshotDependencies,
  inquireVersions,
  runClean,
  runTest,
  setReleaseVersion,
  commitReleaseVersion,
  tagRelease,
  publishArtifacts,
  setNextVersion,
  commitNextVersion)
