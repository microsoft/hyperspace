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

### Lineage
Hyperspace uses lineage for tracing index entries back to the source data files from which they were generated.
Lineage is required for removing deleted index entries during [index refresh in the incremental mode](#refresh-index---incremental-mode)
or enforcing deletes at the query time when using [Hybrid Scan](#hybrid-scan).
By default, lineage is disabled for indexes, and if required it should be enabled at the time of index creation.
Check the [configuration](https://microsoft.github.io/hyperspace/docs/ug-configuration/) page to see how you can enable lineage for an index.

Once lineage is enabled for an index, Hyperspace adds a new column to the index schema to save the source data file path for each
index entry. This means enabling lineage increases total storage size used for index files, proportional to the
number of distinct source data files the index is built on. If you know you wont delete any source data file after index
creation or you are fine recreating the index using [index refresh in the full mode](#optimize-index---full-mode) after
deleting some data files, then you dont need to enable lineage for the index. 

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
An index needs to have lineage enabled to support deletes during refresh in the incremental mode. Lineage 
has to be enabled at time of index creation. As lineage is captured by adding a new column to the index, enabling it
increases total storage size used for storing index files. 
Check the [configuration](https://microsoft.github.io/hyperspace/docs/ug-configuration/) page to see how you can enable lineage for an index.

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
One way to index new data files and merge them into an existing index is by calling `refreshIndex`
with the `"incremental"` mode. In this mode, each time refresh is called to index newly appended source data files,
it creates fresh index files for these files and updates index metadata to include them in the index content.
As these index files accumulate, they could affect query performance when the index is used.
Once the index is leveraged for a query, the large number of these files could increase overall query time as more index
files need to be accessed and potentially read to compute the query results.
Hyperspace provides the `optimizeIndex` command to alleviate above issue by changing index files layout for an
index which has many index files, due to incremental refresh call(s). This is achieved by merging index files together,
if possible, and replacing them with fewer larger files that capture exact same index records. This process is similar to
compaction in append-only log structured merge index structures.

You should note that the `optimizeIndex` command is a best effort to modify index files layout and its
final outcome depends on how index records are stored in existing index files.
Two or more index files can be merged with each other, if and only if they all have index records which belong to the
same bucket (according to index configuration).
When running `optimizeIndex` command, Hyperspace tries to find such groups of index files and merge them together.
If all index records for each given bucket are already stored in a single index file, then there wont be any
index files merge during optimize and physical layout of index files after running optimize will be the same as the
original layout. An example of such an index is an index right after creation or full refresh.    

Currently, there are two optimize modes available for an index: `"quick"` and `"full"`. These modes differ with each other
in terms of the subset of index files they identify and try to merge.
 
#### Optimize Index - Quick Mode
Using `optimizeIndex` command with the `"quick"` mode on an index with many index files, due to incremental index refresh,
causes Hyperspace look for index files which are smaller than a configurable size threshold and try to merge them.
This mode tries to achieve a moderate query performance improvement through a fast optimize index process.
The size threshold for an index file to be eligible for merging during quick optimization can be changed.
Check the [configuration](https://microsoft.github.io/hyperspace/docs/ug-configuration/) page to see how this threshold can be adjusted.
Quick mode is the default mode for `optimizeIndex`.

Assume you have an index with the name `empIndex`. After adding some data files to the dataset `empIndex` is created on
and refreshing it in the incremental mode, you can optimize `empIndex` in the quick mode as below:

Scala:
```scala
import com.microsoft.hyperspace._

val hs = new Hyperspace(spark)
hs.optimizeIndex("empIndex", "quick")
``` 

Python:

```python
from hyperspace import Hyperspace

hs = Hyperspace(spark)
hs.optimizeIndex("empIndex", "quick")
```

#### Optimize Index - Full Mode
In the `"full"` mode, `optimizeIndex` command considers all existing index files as candidates for merging. Therefore,
Hyperspace does a full scan on current index content and identifies groups of index files which have index records belonging
to the same bucket according to the index configuration. It then replaces each group with a single index file created through
merging all the files in that group together. This mode tries to achieve the best query performance improvement via a
potentially slow optimize index process.
You should note that `optimizeIndex` differs from `refreshIndex` in the sense that optimize is an index-only operation.
Unlike refresh, when optimize is running on an index, it does not look at the current state of the source data files.
If there were any source data file changes after index creation or last refresh, optimize wont apply those data changes
to the index files.

Assume you have an index with the name `empIndex`. After adding some data files to the dataset `empIndex` is created on
and refreshing it in the incremental mode, you can optimize `empIndex` in the full mode as below:

Scala:
```scala
import com.microsoft.hyperspace._

val hs = new Hyperspace(spark)
hs.optimizeIndex("empIndex", "full")
``` 

Python:

```python
from hyperspace import Hyperspace

hs = Hyperspace(spark)
hs.optimizeIndex("empIndex", "full")
```

### Hybrid Scan
TODO
