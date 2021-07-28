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

import Dependencies._
import Path.relativeTo

lazy val scala212 = "2.12.8"
lazy val scala211 = "2.11.12"

ThisBuild / scalaVersion := scala212

ThisBuild / scalacOptions ++= Seq("-target:jvm-1.8")

ThisBuild / javaOptions += "-Xmx1024m"

// The root project is a virtual project aggregating the other projects.
// It cannot compile, as necessary utility code is only in those projects.
lazy val root = (project in file("."))
  .aggregate(spark2_4, spark3_0, spark3_1)
  .settings(
    compile / skip := true,
    publish / skip := true,
    Keys.`package` := { new File("") }, // skip package
    Keys.`packageBin` := { new File("") } // skip packageBin
  )

lazy val spark2_4 = (project in file("spark2.4"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    sparkVersion := Version(2, 4, 2),
    crossScalaVersions := List(scala212, scala211),
    inConfig(Compile)(addSparkVersionSpecificSourceDirectories),
    inConfig(Test)(addSparkVersionSpecificSourceDirectories))

lazy val spark3_0 = (project in file("spark3.0"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    sparkVersion := Version(3, 0, 1),
    crossScalaVersions := List(scala212), // Spark 3 doesn't support Scala 2.11
    inConfig(Compile)(addSparkVersionSpecificSourceDirectories),
    inConfig(Test)(addSparkVersionSpecificSourceDirectories))

lazy val spark3_1 = (project in file("spark3.1"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    commonSettings,
    sparkVersion := Version(3, 1, 1),
    crossScalaVersions := List(scala212), // Spark 3 doesn't support Scala 2.11
    inConfig(Compile)(addSparkVersionSpecificSourceDirectories),
    inConfig(Test)(addSparkVersionSpecificSourceDirectories))

lazy val sparkVersion = settingKey[Version]("sparkVersion")

// In addition to the usual scala/ and scala-<version>/ source directories,
// add the following source directories for different Spark versions:
// * scala-spark<spark major version>
// * scala-spark<spark major version>.<spark minor version>
// * scala-<scala version>-spark<spark major version>
// * scala-<scala version>-spark<spark major version>.<spark minor version>
lazy val addSparkVersionSpecificSourceDirectories = unmanagedSourceDirectories ++= Seq(
  sourceDirectory.value / s"scala-spark${sparkVersion.value.major}",
  sourceDirectory.value / s"scala-spark${sparkVersion.value.short}",
  sourceDirectory.value / s"scala-${scalaBinaryVersion.value}-spark${sparkVersion.value.major}",
  sourceDirectory.value / s"scala-${scalaBinaryVersion.value}-spark${sparkVersion.value.short}")

lazy val commonSettings = Seq(
  // The following creates target/scala-2.*/src_managed/main/sbt-buildinfo/BuildInfo.scala.
  buildInfoKeys := Seq[BuildInfoKey](
    version,
    sparkVersion,
    "sparkShortVersion" -> sparkVersion.value.short),
  buildInfoPackage := "com.microsoft.hyperspace",

  name := "hyperspace-core",
  moduleName := name.value + s"-spark${sparkVersion.value.short}",
  libraryDependencies ++= deps(sparkVersion.value),

  // Scalastyle
  scalastyleConfig := (ThisBuild / scalastyleConfig).value,
  compileScalastyle := (Compile / scalastyle).toTask("").value,
  Compile / compile := ((Compile / compile) dependsOn compileScalastyle).value,
  testScalastyle := (Test / scalastyle).toTask("").value,
  Test / test := ((Test / test) dependsOn testScalastyle).value,

  // Package Python files
  (Compile / packageBin / mappings) := (Compile / packageBin / mappings).value ++ listPythonFiles.value,
  listPythonFiles := {
    val pythonBase = (ThisBuild / baseDirectory).value / "python"
    pythonBase ** "*.py" pair relativeTo(pythonBase)
  })

lazy val listPythonFiles = taskKey[Seq[(File, String)]]("listPythonFiles")

/**
 * ScalaStyle configurations
 */
ThisBuild / scalastyleConfig := baseDirectory.value / "scalastyle-config.xml"

// Run as part of compile task.
lazy val compileScalastyle = taskKey[Unit]("compileScalastyle")

// Run as part of test task.
lazy val testScalastyle = taskKey[Unit]("testScalastyle")

/**
 * Test configurations
 */
// Tests cannot be run in parallel since mutiple Spark contexts cannot run in the same JVM.
ThisBuild / Test / parallelExecution := false

ThisBuild / Test / fork := true

ThisBuild / Test / javaOptions += "-Xmx1024m"

// Needed to test both non-codegen and codegen parts of expressions
ThisBuild / Test / envVars += "SPARK_TESTING" -> "1"

ThisBuild / coverageExcludedPackages := "com\\.fasterxml.*;com\\.microsoft\\.hyperspace\\.shim"

/**
 * Release configurations
 */
ThisBuild / organization := "com.microsoft.hyperspace"
ThisBuild / organizationName := "Microsoft"
ThisBuild / organizationHomepage := Some(url("http://www.microsoft.com/"))

ThisBuild / releaseCrossBuild := true

ThisBuild / scmInfo := Some(
  ScmInfo(
    url("https://github.com/microsoft/hyperspace"),
    "scm:git@github.com:microsoft/hyperspace.git"))

ThisBuild / developers := List(
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
    url = url("https://github.com/thugsatbay")),
  Developer(
    id = "clee704",
    name = "Chungmin Lee",
    email = "",
    url = url("https://github.com/clee704")))

ThisBuild / description := "Hyperspace: An Indexing Subsystem for Apache Spark"
ThisBuild / licenses := List(
  "Apache 2" -> new URL("http://www.apache.org/licenses/LICENSE-2.0.txt"))
ThisBuild / homepage := Some(url("https://github.com/microsoft/hyperspace"))

// Remove all additional repository other than Maven Central from POM
ThisBuild / pomIncludeRepository := { _ =>
  false
}
ThisBuild / publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (isSnapshot.value) {
    Some("snapshots" at nexus + "content/repositories/snapshots")
  } else {
    Some("releases" at nexus + "service/local/staging/deploy/maven2")
  }
}

ThisBuild / publishMavenStyle := true

import ReleaseTransformations._

ThisBuild / releasePublishArtifactsAction := PgpKeys.publishSigned.value

ThisBuild / releaseProcess := Seq[ReleaseStep](
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

/**
 * Others
 */
bspEnabled := false
