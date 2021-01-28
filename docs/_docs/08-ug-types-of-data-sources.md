---
title: "Types of data sources"
permalink: /docs/ug-types-of-data-sources/
excerpt: "Supported types of data sources"
last_modified_at: 2021-01-28
toc: false
classes: wide
---

This guide helps you to utilize indexes on other types of data source layers.

## Pluggable source builders
Since Hyperspace v0.4, Hyperspace supports the indexes built on other types of data sources.
To avoid unnecessary library dependencies, we provide pluggable source builders
to support other types of sources.

There are no special APIs for other source types, 
so you can use the existing APIs with a DataFrame built on a different type of source seamlessly.

Currently, Hyperspace supports the below source types:
- Delta Lake (since v0.4)

## Delta Lake
Since Hyperspace v0.4, Hyperspace supports index creation on Delta Lake tables.
You can add Delta Lake support by setting the following config before creating Hyperspace object.
```scala
spark.conf.set("spark.hyperspace.index.sources.fileBasedBuilders",
      "com.microsoft.hyperspace.index.sources.delta.DeltaLakeFileBasedSourceBuilder," +
      "com.microsoft.hyperspace.index.sources.default.DefaultFileBasedSourceBuilder")

val hyperspace = new Hyperspace(spark)
```

You can use the existing API with a Delta Lake Dataframe.

For example, index creation:
```scala
val deltaDf = spark.read.format("delta").option("versionAsOf", version.get).load(dataPath)
hyperspace.createIndex(deltaDf, IndexConfig("deltaIndex", Seq("clicks"), Seq("Query")))
```

### Mutable dataset
For Delta Lake, You can still utilize the existing feature set such as Hybrid Scan & incremental refresh in the same way.
Please note that these features are based on the difference of source files.
If there are a lot of changes in the source dataset from update/insert/delete executions,
these features won't be applied or performed in an inefficient way.

We would recommend you not to make a lot of changes or create a proper partition to reduce the difference.

For more information, please refer
[Mutable dataset](https://microsoft.github.io/hyperspace/docs/ug-mutable-dataset/) page.

### Time travel query
Hyperspace can support Delta Lake time travel queries using Hybrid Scan.
In order to support an older version compared to the delta version of the latest index, *appended* files after
the older version are considered as *deleted* files, *deleted* files after the older version are considered as *appended* files.

In the next Hyperspace release (v0.5), we will optimize time travel query by selecting a proper version of the index.
If there are multiple refreshed versions of a candidate index, we could get the "closest" version for the given delta version and apply Hybrid Scan.
In this way, we could reduce the difference of sources files which should be handled with Hybrid Scan.