---
title: "Optimize index"
permalink: /docs/ug-optimize-index/
excerpt: "Optimize index guide"
last_modified_at: 2020-10-21
toc: false
classes: wide
---

## Optimize Index
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

One important point is that `optimizeIndex` differs from `refreshIndex` in the sense that `optimizeIndex` is an index-only operation.
Unlike `refreshIndex`, when `optimizeIndex` is running on an index, it does not look at the current state of the source data files.
If there were any source data file changes after last refresh, `optimizeIndex` will not apply those data changes
to the index files.

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
 
### Optimize Index - Quick Mode
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

### Optimize Index - Full Mode
In the `"full"` mode, `optimizeIndex` command considers all existing index files as candidates for merging. Therefore,
Hyperspace does a full scan on current index content and identifies groups of index files which have index records belonging
to the same bucket according to the index configuration. It then replaces each group with a single index file created through
merging all the files in that group together. This mode tries to achieve the best query performance improvement via a
potentially slow optimize index process.

Assume you have an index with the name `empIndex`. After adding some source data files to the dataset `empIndex` is created on
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