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
  - [Incremental Mode](#refresh-index---incremental-mode)
  - [Quick Mode](#refresh-index---quick-mode)
2. [Hybrid Scan](#hybrid-scan)
  - [Append-only](#append-only-dataset)
  - [Append and Delete](#append-and-delete-dataset)
  
### Lineage
Hyperspace uses lineage for tracing index entries back to the source data files from which they were generated.
Lineage is required for removing deleted index entries during [index refresh in the incremental mode](#refresh-index---incremental-mode)
or enforcing deletes at the query time when using [Hybrid Scan](#hybrid-scan).
By default, lineage is disabled for indexes, and if required it should be enabled at the time of index creation.
Check the [configuration](https://microsoft.github.io/hyperspace/docs/ug-configuration/) page to see how you can enable lineage for an index.

Once lineage is enabled for an index, Hyperspace adds a new column to the index schema to save the source data file path for each
index entry. This means enabling lineage increases total storage size used for index files, proportional to the
number of distinct source data files the index is built on. If you know you will not delete any source data file after index
creation or you are fine recreating the index using [index refresh in the full mode](#refresh-index---full-mode)
after deleting some source data files, then you do not need to enable lineage for the index. 

### Refresh Index
You can refresh an index according to its latest source data files by running the `refreshIndex` command.
Hyperspace provides several modes to refresh an index. These modes differ in terms of the way they update the index data, the amount of data scans and shuffle each does.
You should pick a mode for refreshing an index according to its current size and total amount of data deleted from or appended to its source data files.
You can specify the mode as an argument when calling the `refreshIndex` command.
Currently, there are 3 refresh modes available for an index: `"full"`, `"incremental"` and `"quick"`.

#### Refresh Modes

|          |                | Full - Rebuild                                        | Incremental - Quick Query                                                                 | Quick - Fast Refresh                                                                        |
|----------|----------------|-------------------------------------------------------|--------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------|
| Append   | Characteristic | Slowest refresh/fastest query                         | Slow refresh/fast query                                                                    | Fast refresh/moderately fast query                                                          |
|          | API            | refreshIndex(mode="full")                             | refreshIndex(mode="incremental")                                                           | refreshIndex(mode="quick")                                                                  |
|          | What it does?  | Rebuilds the index                                    | Builds index on newly added data                                                           | Captures meta-data for appended  files and leverages hybrid scan                            |
|          | When to use?   | Underlying source data is  relatively stable          | Infrequently appending large  amounts of data                                              | Frequently appending small  amounts of data                                                 |
| Delete   | Characteristic | Creates a new index (by  reshuffling the source data) | Slow refresh/fast query                                                                    | Fast refresh/moderately fast query                                                          |
|          | API            |                                                       | refreshIndex(mode="incremental")                                                           | refreshIndex(mode="quick")                                                                  |
|          | What it does?  |                                                       | Deletes rows from index immediately; Avoids  shuffling the source data using index lineage | Captures file/partition predicates  and deletes entries at query time                       |
|          | When to use?   |                                                       | Infrequently deleting large  amount of data                                                | Frequently deleting small  amounts of data                                                  |


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
An index needs to have [lineage](#lineage) enabled, at the creation time, to support deletes during refresh in the incremental mode.
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

#### Refresh Index - Quick Mode

Refresh Quick mode is a metadata only operation. It collects the list of appended and deleted source data files at refresh time, and does not update any index data.
To leverage the difference of source data files, Hyperspace utilizes [Hybrid Scan](#hybrid-scan) when applying the index, even if Hybrid Scan is disabled.
It relies on on-the-fly shuffle and merge for appended files and injected filter condition for deleted files.

To handle deleted files, [Lineage column](#lineage) is required as in [incremental mode](#refresh-index---incremental-mode) or Hybrid Scan.

Scala:
```scala
import com.microsoft.hyperspace._

val hs = new Hyperspace(spark)
hs.refreshIndex("empIndex", "quick")
``` 

Python:

```python
from hyperspace import Hyperspace

hs = Hyperspace(spark)
hs.refreshIndex("empIndex", "quick")
```

### Hybrid Scan

Hybrid Scan utilizes existing index data along with newly appended source files and/or deleted
source files, without explicit refresh operation. For an index with appended source files,
HybridScan changes the query plan to shuffle new data on-the-fly and merge it with index records.
For an index with deleted source files, Hyperspace also modifies the plan to exclude the rows from
deleted files in the index data. This requires enabling lineage for the index at its creation time.

Currently, HybridScan is disabled by default. You can check the [configuration](https://microsoft.github.io/hyperspace/docs/ug-configuration/)
page to see how it can be enabled.

Hyperspace provides two threshold configurations (`spark.hyperspace.index.hybridscan.maxDeletedRatio`, `spark.hyperspace.index.hybridscan.maxAppendedRatio`)
to determine whether we apply the candidate index with Hybrid Scan or not depending on the amount of appended data and deleted data.

#### Append-only dataset

If your dataset is append-only dataset, you can use Hybrid Scan for appended files only.
In this case, Hyperspace will not pick an index with some deleted source file(s) for Hybrid Scan.
Hybrid Scan with only appended source files does not need the [lineage column](#lineage)
and any other pre-requisite.

###### How to enable

You can use the following configurations to enable Hybrid Scan for indexes on an append-only dataset.
We provide a threshold config for the amount of appended data (`spark.hyperspace.index.hybridscan.maxAppendedRatio`, 0.0 to 1.0).
It indicates the maximum ratio of *total size of appended files files* to *total size of all source files
covered by the candidate index*. If there's more appended data than this threshold, Hybrid scan won't be applied.
As Hybrid Scan causes some regression depending on workload types, we allow 30% (0.3) of appended data by default.

Scala:
```scala
spark.conf.set("spark.hyperspace.index.hybridscan.enabled", true)
spark.conf.set("spark.hyperspace.index.hybridscan.maxAppendedRatio", 0.3) // 30% by default
```

Python:
```python
spark.conf.set("spark.hyperspace.index.hybridscan.enabled", true)
spark.conf.set("spark.hyperspace.index.hybridscan.maxAppendedRatio", 0.3) # 30% by default
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
query.show
```

#### Append and Delete dataset

Now, we can consider handling deleted files. Basically, Hybrid Scan excludes indexed data from deleted source files
by scanning all index rows and verifying whether each row is coming from a deleted source file or not.
In order to trace which source file each row is from, you need to enable linage column config before creating an index.
Check [configuration](https://microsoft.github.io/hyperspace/docs/ug-configuration/) page to see how to enable the lineage column at index creation.

Due to the way Hybrid Scan enforces deletes at the query time, supporting deletes is more expensive than appended
files. The more deleted files it has, the more overhead it will incur to filter the rows, and the benefit from the index will decrease.
Therefore, you need to be aware of possible performance regression from it.

###### How to enable

You can use the following configurations to enable Hybrid Scan for indexes on a dataset with both append and delete files.

Similar to appended files, we provide the threshold config for deleted files:
`spark.hyperspace.index.hybridscan.maxDeletedRatio`

It indicates the maximum ratio of *total size of deleted files* to *total size of all source files
covered by the candidate index*.  If there's more deleted data than this threshold, Hybrid scan won't be applied. Currently it's 0.2 (20%) by default.
To apply Hybrid Scan, both appended & deleted threshold conditions should be met.

Scala:
```scala
spark.conf.set("spark.hyperspace.index.hybridscan.enabled", true)
spark.conf.set("spark.hyperspace.index.hybridscan.maxAppendedRatio", 0.3) // 30% by default
spark.conf.set("spark.hyperspace.index.hybridscan.maxDeletedRatio", 0.2) // 20% by default
```

Python:
```python
spark.conf.set("spark.hyperspace.index.hybridscan.enabled", True)
spark.conf.set("spark.hyperspace.index.hybridscan.maxAppendedRatio", 0.3) # 30% by default
spark.conf.set("spark.hyperspace.index.hybridscan.maxDeletedRatio", 0.2) # 20% by default
```
