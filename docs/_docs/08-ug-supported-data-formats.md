---
title: "Supported data formats"
permalink: /docs/ug-supported-data-formats/
excerpt: "Supported data formats"
last_modified_at: 2021-01-28
toc: false
classes: wide
---

This guide helps you to utilize indexes on other types of data sources such as [Delta Lake](https://github.com/delta-io/delta).

## Pluggable source builders
Since Hyperspace v0.4, Hyperspace supports indexes built on other types of data sources.
To avoid unnecessary library dependencies, we provide pluggable source builders
to support other types of sources.

There are no special APIs for other source types, 
so you can use the existing APIs with a DataFrame built on a different type of source seamlessly.

Currently, Hyperspace supports the below source types:
- Delta Lake (since v0.4)
- Apache Iceberg (work-in-progress PRs)

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
For Delta Lake, you can still utilize the existing feature set such as Hybrid Scan & incremental refresh in the same way.
Please note that these features are based on the difference of source files.
If there are a lot of changes in the source dataset from update/insert/delete executions,
these features won't be applied due to performance implications. We highly recommend 
that you analyze performance on your own workloads before going into production. 

For more information, please refer
[Mutable dataset](https://microsoft.github.io/hyperspace/docs/ug-mutable-dataset/) page.

### Time travel query
Hyperspace can support Delta Lake time travel queries using Hybrid Scan.
We can extract appended and deleted files from an older delta version based on the latest index.
Then we can apply Hybrid Scan by considering:
- **appended** files after the older version as **deleted** files 
- **deleted** files after the older version as **appended** files

We have some planned improvements discussed in this [issue](https://github.com/microsoft/hyperspace/issues/270).
