---
title: "Mutable dataset"
permalink: /docs/ug-mutable-dataset/
excerpt: "Mutable dataset guide"
last_modified_at: 2020-10-14
toc: false
classes: wide
---

This guide helps you to utilize indexes on mutable dataset.

## What does mutable mean?
Hyperspace enables users to build indexes on their data.
Since Hyperspace v0.3, we support file-level "mutable" dataset which means users can append new data files and/or
delete existing data files under the root paths of the given source data of indexes.

Previously, any change in the original dataset content required a full refresh to make the index usable again.
This could be a costly operation due to shuffling and repartitioning all records in the latest source data.

Now, we offer several options to handle above scenario more efficiently.

## Options of using index when your dataset changes

1. [Refresh Index](#refresh-index)
  - [Full Mode](#refresh-index---full-mode)
  - [Incremental Mode](#refresh-index---full-mode)
2. [Hybrid Scan](#hybrid-scan)
  - [Append-only](#append-only-dataset)
  - [Append and Delete](#append-and-delete-dataset)

### Refresh Index
You can refresh an index according to its latest source data files by running the `refreshIndex` command.
Hyperspace provide several modes to refresh an index. These modes differ in terms of the way they update the index and the amount of data scans and shuffle each does.
You should pick a mode for refreshing an index according to its current size and total amount of data deleted from or appended to its source data files.
You can specify the mode as an argument when calling the `refreshIndex` command.
Currently, there are two refresh modes available for an index: `"full"` and `"incremental"`.

#### Refresh Index - Full Mode
After some changes in an index's original dataset files, using `refreshIndex` with the `"full"` mode causes
the index refresh action perform a full rebuild of the index.
This ends up creating a new version of the index and involves a full scan and shuffle of its latest source data.
As a result, the amount of time it takes to refresh an index in this mode is similar to creating a new index, with the same index configuration, on the latest source data.
The advantage of using the full mode is that once index refresh is finished successfully, new index files are organized in the most optimized way according to index's latest source data content and its bucketing configuration.

Assume you have an index with the name `empIndex`. After adding and removing some data files from the dataset `empIndex` is created on, you can refresh it in the full mode as below:

Scala:
```scala
import com.microsoft.hyperspace._

val hs = new Hyperspace(spark)
hs.refreshIndex("empIndex", "full")
``` 

Python:

```python
from hyperspace import Hyperspace

hs = Hyperspace(spark)
hs.refreshIndex("empIndex", "full")
```

#### Refresh Index - Incremental Mode
After some files are added to or deleted from the original source files, an index was built on,
using `refreshIndex` with the `"incremental"` mode causes the index refresh action recreate any existing index file,
which has some deleted records, to remove those records. Refresh also creates new index files by indexing newly added data files.
An index needs to have lineage enabled to be eligible for refresh in the incremental mode.
Check the [configuration](https://microsoft.github.io/hyperspace/docs/ug-configuration/) page to see how lineage is enabled when creating an index.

Once refresh is called for an index in the incremental mode, Hyperspace checks latest source data files and identifies
deleted source data files and newly added ones. It recreates those portions of index which have records from the
deleted files. Lineage is used to detect these affected index files. Subsequently, Hyperspace creates new index files on
newly added data files, according to the index's configuration. At the end of this process, index's metadata gets updated to reflect
the latest snapshot of the index. The source content of this snapshot points to the latest dataset files.     

Assume you have an index with the name `empIndex` with lineage enabled. After adding and removing some data files from the dataset `empIndex` is created on, you can refresh it in the incremental mode as below:

Scala:
```scala
import com.microsoft.hyperspace._

val hs = new Hyperspace(spark)
hs.refreshIndex("empIndex", "incremental")
``` 

Python:

```python
from hyperspace import Hyperspace

hs = Hyperspace(spark)
hs.refreshIndex("empIndex", "incremental")
```

### Optimize Index
TODO

### Hybrid Scan

Hybrid Scan enables to utilize existing index data along with newly appended source files or
deleted existing files, without explicit refresh operation. For an index with appended source data files,
HybridScan changes the query plan to shuffle new data on-the-fly and merge it with index records.
For an index with deleted source data files, Hyperspace also modifies the plan to exclude the rows from deleted files
in the index data. This requires enabling lineage for the index at its creation time.

Currently, HybridScan is disabled by default. You can check the [configuration](https://microsoft.github.io/hyperspace/docs/ug-configuration/)
page to see how it can be enabled.

In the current version (0.3), if a dataset has many deleted source data files, query performance
could degrade when Hybrid Scan is enforcing deletes at the query runtime. 
Hyperspace provides two different configurations to tune this behavior. 
You can use them to enable supporting deletes during Hybrid Scan and determine when it should be applied,
depending on the total number of deleted source data files.

#### Append-only dataset

If your dataset is append-only dataset, you can use Hybrid Scan for appended files only.
In this case, Hyperspace will not pick an index with some deleted source data file(s) for Hybrid Scan.

###### How to enable

You can use the following configurations to enable Hybrid Scan for indexes on an append-only dataset.
You need to call `spark.enableHyperspace` after setting the configuration to load additional modules
for Hybrid Scan.

Scala:
```scala
import com.microsoft.hyperspace._

spark.conf.set("spark.hyperspace.index.hybridscan.enabled", true)
spark.conf.set("spark.hyperspace.index.hybridscan.delete.enabled", false) // false by default
spark.enableHyperspace
```

Python:
```python
from hyperspace import Hyperspace

spark.conf.set("spark.hyperspace.index.hybridscan.enabled", true)
spark.conf.set("spark.hyperspace.index.hybridscan.delete.enabled", false) // false by default
Hyperspace.enable(spark)
```

###### Example

This is a simple example in Scala from [Quick-Start Guide](https://microsoft.github.io/hyperspace/docs/ug-quick-start-guide/).
Of course, you can try this in Python accordingly.

```scala
// Setup source data.
import org.apache.spark.sql._
import spark.implicits._

Seq((1, "name1"), (2, "name2")).toDF("id", "name").write.mode("overwrite").parquet("table")
val df = spark.read.parquet("table")

// Setup Hyperspace.
import com.microsoft.hyperspace._
val hs = new Hyperspace(spark)

// Create an index.
import com.microsoft.hyperspace.index._
hs.createIndex(df, IndexConfig("index", indexedColumns = Seq("id"), includedColumns = Seq("name")))

// Create a query and check if the index is applied or not.
val query = df.filter(df("id") === 1).select("name")
hs.explain(query, verbose = true)

// Run query with the index.
spark.enableHyperspace
query.show
```

Now, the following example shows how Hybrid Scan works with appended files.

```scala
// Append new data to source dataset.
Seq((3, "name3"), (4, "name4")).toDF("id", "name").write.mode("append").parquet("table")

// Check if the index is applied for the dataset with appended files.
val df = spark.read.parquet("table")
val query = df.filter(df("id") === 1).select("name")
hs.explain(query, verbose = true)

// Turn on Hybrid Scan and check if the index is applied.
spark.conf.set("spark.hyperspace.index.hybridscan.enabled", true)
hs.explain(query, verbose = true)

// Query execution with Hybrid Scan.
spark.enableHyperspace
query.show
```

#### Append and Delete dataset

Now, we can consider handling deleted files. Basically, Hybrid Scan excludes indexed data from deleted source files
by scanning all index rows and verifying whether each is coming from a deleted source data file or not.
In order to trace which source file each row is from, you need to enable linage column config before creating an index.
Check the [configuration](https://microsoft.github.io/hyperspace/docs/ug-configuration/) page to see how lineage is enabled when creating an index.

Due to the way Hybrid Scan enforces deletes at the query time, supporting deletes is more expensive than appended
files. It could cause performance regression if an index has a number of deleted source data files, especially for
an index which is less effective for a given query.
Therefore, you need to be aware of possible regression from it.

We will provide several threshold configs after some experiments and optimizations.

###### How to enable

You can use the following configurations to enable Hybrid Scan for indexes on a dataset with both append and delete files.
You need to call `spark.enableHyperspace` after setting the configuration to load additional modules for Hybrid Scan.

We currently provide one threshold config:
`spark.hyperspace.index.hybridscan.delete.maxNumDeleted`. If there are more deleted files than the config value,
we do not perform Hybrid Scan for the index.

Scala:
```scala
import com.microsoft.hyperspace._
spark.conf.set("spark.hyperspace.index.hybridscan.enabled", true)
spark.conf.set("spark.hyperspace.index.hybridscan.delete.enabled", true)
// spark.conf.set("spark.hyperspace.index.hybridscan.delete.maxNumDeleted",  30) // 30 by default
spark.enableHyperspace
```

Python:
```python
from hyperspace import Hyperspace

spark.conf.set("spark.hyperspace.index.hybridscan.enabled", true)
spark.conf.set("spark.hyperspace.index.hybridscan.delete.enabled", true)
// spark.conf.set("spark.hyperspace.index.hybridscan.delete.maxNumDeleted",  30) // 30 by default
Hyperspace.enable(spark)
```
