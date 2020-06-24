---
title: "Quick-Start Guide"
permalink: /docs/ug-quick-start-guide/
excerpt: "How to quickly get started with Hyperspace for use with Apache Spark™."
last_modified_at: 2020-06-23
toc: true
---

This guide helps you quickly get started with Hyperspace with Apache Spark™.

## Run with Apache Spark™
Hyperspace is compatiable with Apache Spark™ 2.4.* (support for Apache Spark™ 3.0 is on the way) and is cross built against Scala 2.11 and 2.12.
There are two ways to set up Hyperspace with Apache Spark™:
1. Run as a project: Create a SBT or Maven project with Hyperspace, copy [code snippet](https://microsoft.github.io/hyperspace/#hyperspace-usage-api-in-apache-spark), and run the project.
2. Run with an interactive shell: Start the Spark Scala shell with Hyperspace and start exploring Hypersace APIs interactively.

### Run as a project
If you want to create a project using Hyperspace, you can get the artifacts from the [Maven Central Repository](https://search.maven.org/search?q=hyperspace) using the Maven coordinates as follows:

#### Maven
For your Maven project, add the following lines to your `pom.xml` file:

* For Scala 2.11:

```
<dependency>
    <groupId>com.microsoft.hyperspace</groupId>
    <artifactId>hyperspace-core_2.11</artifactId>
    <version>0.1.0</version>
</dependency>
```

* For Scala 2.12:

```
<dependency>
    <groupId>com.microsoft.hyperspace</groupId>
    <artifactId>hyperspace-core_2.12</artifactId>
    <version>0.1.0</version>
</dependency>
```

#### SBT
For you SBT project, add the following line to your `build.sbt` file:

```
libraryDependencies += "com.microsoft.hyperspace" %% "hyperspace-core" % "0.1.0"
```

### Run with an interactive shell
To use Hyperspace with a Spark's interactive shell, you need to download/install Apache Spark™ 2.4.x locally by following instructions [here](https://spark.apache.org/downloads.html).

#### Spark Scala Shell
Start the Spark Scala shell as follows:

```
./bin/spark-shell --packages com.microsoft:hyperspace-core_2.11:0.1.0
```

#### PySpark
Support for Pyspark is [on the way](https://github.com/microsoft/hyperspace/pull/36).
