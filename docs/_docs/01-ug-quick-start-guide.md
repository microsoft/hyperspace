---
title: "Quick-Start Guide"
permalink: /docs/ug-quick-start-guide/
excerpt: "How to quickly get started with Hyperspace for use with Apache Spark™."
last_modified_at: 2020-06-24
toc: false
classes: wide
---

This guide helps you quickly get started with Hyperspace with Apache Spark™.

## Set up Hyperspace
Hyperspace is compatiable with Apache Spark™ 2.4.* (support for Apache Spark™ 3.0 is on the way) and is cross built against Scala 2.11 and 2.12.
There are two ways to set up Hyperspace:
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
    <version>0.3.0</version>
</dependency>
```

* For Scala 2.12:

```
<dependency>
    <groupId>com.microsoft.hyperspace</groupId>
    <artifactId>hyperspace-core_2.12</artifactId>
    <version>0.3.0</version>
</dependency>
```

#### SBT
For you SBT project, add the following line to your `build.sbt` file:

```
libraryDependencies += "com.microsoft.hyperspace" %% "hyperspace-core" % "0.3.0"
```

### Run with an interactive shell
To use Hyperspace with a Spark's interactive shell, you need to download/install Apache Spark™ 2.4.x locally by following instructions [here](https://spark.apache.org/downloads.html).

#### Spark Scala Shell
Start the Spark Scala shell as follows:

```
./bin/spark-shell --packages com.microsoft.hyperspace:hyperspace-core_2.11:0.3.0
```

#### PySpark
Install Pyspark by running the following:

```
pip install pyspark==2.4.2
```

Then, run PySpark with the Hyperspace package:
```
pyspark --packages com.microsoft.hyperspace:hyperspace-core_2.11:0.3.0
```

## Hyperspace APIs

You can run the code snippets in the following sections to explore the main features of Hyperspace. The full, standalone sample is available [here](https://microsoft.github.io/hyperspace/#hyperspace-usage-api-in-apache-spark). Please refer to [API docs](https://javadoc.io/doc/com.microsoft.hyperspace/hyperspace-core_2.12/latest/com/microsoft/hyperspace/index.html) for more details.

### Index Management APIs

#### Set up

To begin with, create a `DataFrame` from the data files (required to detect source data changes and to perform index refresh across sessions):

Scala:

```scala
import org.apache.spark.sql._
import spark.implicits._

Seq((1, "name1"), (2, "name2")).toDF("id", "name").write.mode("overwrite").parquet("table")
val df = spark.read.parquet("table")
```

Python:

```python
sample_data = [(1, "name1"), (2, "name2")]
spark.createDataFrame(sample_data, ['id', 'name']).write.mode("overwrite").parquet("table")
df = spark.read.parquet("table")
```

Also, create a `Hyperspace` object, which provides index management APIs:

Scala:

```scala
import com.microsoft.hyperspace._

val hs = new Hyperspace(spark)
```

Python:

```python
from hyperspace import Hyperspace

hs = Hyperspace(spark)
```

#### Create an index

To create a Hyperspace Index, specify a `DataFrame` along with index configurations. `indexedColumns` are the column names used for join or filter operations, and `includedColumns` are the ones used for project operations. In this example, we will have a query that filters on the `id` column and projects the `name` column.

Scala:

```scala
import com.microsoft.hyperspace.index._

hs.createIndex(df, IndexConfig("index", indexedColumns = Seq("id"), includedColumns = Seq("name")))
```

Python:

```python
from hyperspace import IndexConfig

hs.createIndex(df, IndexConfig("index", ["id"], ["name"]))
```

##### Supporting Globbing Patterns on hyperspace (since 0.4.0)
Hyperspace also provides creation and maintenance of indexes on data sources with globbing patterns. To support this, users must set `spark.hyperspace.source.globbingPattern` to the data source path at the time of index creation. For multiple paths, use comma separated values as shown in the below example.
Scala:
```scala
val path = "tmp/1/*"
val path2 = "tmp/2/*"
val df = spark
    .read
    .option("spark.hyperspace.source.globbingPattern", s"$path,$path2")
    .parquet(path, path2)

val hs = new Hyperspace(spark)
hs.createIndex(df, ...)
```

#### Getting information on the available indexes

`Hyperspace.indexes` returns a `DataFrame` that captures the metadata of the available indexes, thus you can perform any `DataFrame` operations to display, filter, etc.:

Scala:

```scala
val indexes: DataFrame = hs.indexes
indexes.show
```

Python:

```python
indexes = hs.indexes()
indexes.show()
```

#### Other management APIs

These are the additional APIs for managing (delete, refresh, etc.) indexes:

Scala:

```scala
// Refreshes the given index if the source data changes.
hs.refreshIndex("index")

// Soft-deletes the given index and does not physically remove it from filesystem.
hs.deleteIndex("index")

// Restores the soft-deleted index.
hs.restoreIndex("index")

// Soft-delete the given index for vacuum.
hs.deleteIndex("index")
// Hard-delete the given index and physically remove it from filesystem.
hs.vacuumIndex("index")
```

Python:

```python
# Refreshes the given index if the source data changes.
hs.refreshIndex("index")

# Soft-deletes the given index and does not physically remove it from filesystem.
hs.deleteIndex("index")

# Restores the soft-deleted index.
hs.restoreIndex("index")

# Soft-delete the given index for vacuum.
hs.deleteIndex("index")
# Hard-delete the given index and physically remove it from filesystem.
hs.vacuumIndex("index")
```

### Index Usage APIs

#### Explain the index usage

The following is a query that filters on the `id` column and projects the `name` column:

Scala:
```scala
val query = df.filter(df("id") === 1).select("name")
```

Python:
```python
query = df.filter("""id = 1""").select("""name""")
```

To check whether any index will be used, you can use the `explain` API, which will print out the information on the indexes used, physical plan/operator differences, etc.:

Scala:
```scala
hs.explain(query, verbose = true)
```

Python:
```python
hs.explain(query, verbose = True)
```

#### Enable Hyperspace

Now that you have created an index that your query can utilize, you can enable Hyperspace and execute your query:

Scala:
```scala
spark.enableHyperspace
query.show
```

Python:
```python
Hyperspace.enable(spark)
query.show()
```
