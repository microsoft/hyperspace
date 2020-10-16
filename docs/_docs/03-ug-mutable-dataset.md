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
Since Hyperspace v0.3, we support file-level "mutable" dataset which means appended files and/or
deleted files under the root paths of the given source data.

Previously, any change in the original dataset content, such as adding a new source data file or 
removing an existing one, required a full refresh to make the index usable again. This could be
a costly operation due to shuffling and repartitioning all records in the latest source data.

Now, we offer several options to handle appended and deleted files more efficiently.

## Options of using index when your dataset changes

### Refresh Index
You can refresh an index according to its latest source data files by running the `refresh` command.
Hyperspace provide several modes to refresh index. These modes differ in terms of the way the update the index and the amount of work done for that.
You should pick a mode for refreshing an index according to its current size and total amount of data deleted from or appended to its source data files. 
You can specify the mode as an argument when calling the `refresh` command.
Currently, there are two refresh modes available for an index: `"full"` and `"incremental"`.

#### Full
After some changes in an index's original dataset files, using `refresh` with the `"full"` mode on the index causes
the index refresh action perform a full rebuild of the index.
This ends up creating a new version of the index and involves a full scan of the underlying latest source data.
As a result, the amount of time it takes to refresh an index in this mode is similar to creating a new index with that size.
The advantage is that once refresh in the full mode is finished successfully, new index files are created and organized in the most optimized way according to index's last source data content and the its bucketing configuration.

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
After some changes in an index's original dataset files, using `refresh` with the `"incremental"` mode on the index causes
the index refresh action to index newly added data files and fix any existing index file which has some deleted records.
An index needs needs to have lineage enabled to be eligible for refresh in the incremental mode.
This can be done by setting the configuration `spark.hyperspace.index.lineage.enabled` to ture before creating the index.

Once refresh is called for an index in the incremental mode, Hyperspace checks latest dataset files and identifies
deleted source data files and newly added ones. It recreates those portions of index which have records from the
deleted files. Lineage is used to detect and fix these affected index files. Subsequently, Hyperspace creates new index files on
newly added data files, according to the index's configuration. At the end of this process, index's metadata gets updated to reflect
the latest snapshot of index. The source content of this snapshot points to the latest data files.     

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
