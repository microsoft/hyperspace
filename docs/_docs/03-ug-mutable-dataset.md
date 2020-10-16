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

### Refresh Index
You can refresh an index according to its latest source data files by running the `refresh` command.
Hyperspace provide several modes to refresh an index. These modes differ in terms of the way they update the index and the amount of data scans and shuffle each does.
You should pick a mode for refreshing an index according to its current size and total amount of data deleted from or appended to its source data files.
You can specify the mode as an argument when calling the `refresh` command.
Currently, there are two refresh modes available for an index: `"full"` and `"incremental"`.

#### Full
After some changes in an index's original dataset files, using `refresh` with the `"full"` mode causes
the index refresh action perform a full rebuild of the index.
This ends up creating a new version of the index and involves a full scan and shuffle of its latest source data.
As a result, the amount of time it takes to refresh an index in this mode is similar to creating a new index, with the same index configuration, on the latest dataset content.
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

#### Incremental
After some changes in an index's original dataset files, using `refresh` with the `"incremental"` mode causes
the index refresh action fix any existing index file which has some deleted records and index newly added data files.
An index needs needs to have lineage enabled to be eligible for refresh in the incremental mode.
Check the [configuration](02-ug-configuration.md) page to see how lineage is enabled when creating an index.

Once refresh is called for an index in the incremental mode, Hyperspace checks latest dataset files and identifies
deleted source data files and newly added ones. It recreates those portions of index which have records from the
deleted files. Lineage is used to detect and fix these affected index files. Subsequently, Hyperspace creates new index files on
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

##### Optimize Index
TODO

### Hybrid Scan
TODO
