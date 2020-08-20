name := "hyperspaceApp"

version := "0.2"

scalaVersion := "2.11.12"
val sparkVersion = "2.4.2"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-core" % sparkVersion % "provided" withSources(),
  "org.apache.spark" %% "spark-catalyst" % sparkVersion % "provided" withSources()
)

libraryDependencies += "com.microsoft.hyperspace" %% "hyperspace-core" % "0.2.0"

scalacOptions ++= Seq(
  "-target:jvm-1.8"
)
