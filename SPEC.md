# Hyperspace 

- [Overview](#overview)
- [Hyperspace Index Specification](#hyperspace-index-specification)
  - [Actions](#actions)
    - [Create](#create)
    - [Refresh](#refresh)
    - [Optimize](#optimize)
    - [Delete](#delete)
    - [Vacuum](#vacuum)
    - [Cancel](#cancel)
  - [Index Type](#index-type)
  - [Querying with Hyperspace](#querying-with-hyperspace)
  - [Supported data formats](#supported-data-format)
  - [Supported languages](#supported-language)
- [Appendix](#appendix)


# Overview

This document is a specification for Hyperspace which brings abilities for users to build indexes on their data, 
maintain them through a multi-user concurrency mode, and leverage them automatically - without any change to their
application code - for query/workload acceleration.

Hyperspace is designed with the following design goals in mind 
(details are [here](https://microsoft.github.io/hyperspace/docs/toh-design-goals/#agnostic-to-data-format)):

- **Agnostic to data format** - Hyperspace intends to provide
the ability to index data stored in the lake in any format, including
text data and binary data.

- **Low-cost index metadata management** - Hyperspace should be light-weight, fast to retrieve, and
operate independent of a third-party catalog.

- **Multi-engine interoperability** - Hyperspace should make third-party engine integration easy. 

- **Simple and guided user experience** - Hyperspace should offer the simplest
possible experience, with relevant helper APIs, documentation and tutorials.

- **Extensible indexing** - Hyperspace should offer mechanisms for easy pluggability of newer auxiliary data structures.

- **Security, Privacy, and Compliance** - Hyperspace should meet the necessary security, privacy, and compliance standards.


# Hyperspace Index Specification

Indexes are managed by `IndexLogEntry` which consists of 

* `name`: Name of the index.
* `derivedDataset`: Data that has been derived from one or more datasets and may be optionally used by 
an arbitrary query optimizer to improve the speed of data retrieval.
* `content`: File contents used by the index.
* `source`: Data source.
* `properties`: Hash map for managing properties of the index. 

Indexes can have the following states:
* Stable states
  * ACTIVE
  * DELETED
  * DOESNOTEXIST
* Non-stable states
  * CANCELLING
  * CREATING
  * DELETING
  * OPTIMIZING
  * REFRESHING
  * RESTORING
  * VACUUMING

Index states are changed by invoking actions.


## Actions

Actions modify the state of the index. 
This section lists the space of available actions as well as their schema.


### Create

To create a Hyperspace Index, specify a `DataFrame` along with index configurations. 
`indexedColumns` are the column names used for join or filter operations. 
Some index types such as Covering Index use  
`includedColumns` as the ones utilized for project operations.


### Refresh

If the source dataset on which an index was created changes, then the index will no longer capture the latest state of 
data and hence will not be used by Hyperspace to provide any acceleration. The user can refresh such a stale index using
the refreshIndex API.
This API provides a few supported refresh modes. Currently, supported modes are `full`, `incremental` and `quick`.
You can read the details [here](https://microsoft.github.io/hyperspace/docs/ug-mutable-dataset/#refresh-index).


### Optimize

Optimize index by changing the underlying index data layout (e.g., compaction).
Note: This API does NOT refresh (i.e. update) the index if the underlying data changes. 
It only rearranges the index data into a better layout, by compacting small index files. The
index files larger than a threshold remain untouched to avoid rewriting large contents.

Available modes:
* Quick mode: This mode allows for fast optimization. Files smaller than a predefined threshold
`spark.hyperspace.index.optimize.fileSizeThreshold` will be picked for compaction.
* Full mode: This allows for slow but complete optimization. ALL index files are picked for compaction.


### Delete

A user can drop an existing index by using the deleteIndex API and providing the index name.
Index deletion is a soft-delete operation i.e., only the index's status in the Hyperspace metadata from is changed 
from `ACTIVE` to `DELETED`. This will exclude the deleted index from any future query optimization and Hyperspace 
no longer picks that index for any query. However, index files for a deleted index still remain available 
(since it is a soft-delete), so if you accidentally deleted the index, you could still restore it.


### Vacuum

The user can perform a hard-delete i.e., fully remove files and the metadata entry for a deleted index using the 
vacuumIndex API. Once done, this action is irreversible as it physically deletes all the index files associated 
with the index.


### Restore

A user can use the restoreIndex API to restore a deleted index. 
This will bring back the latest version of index into ACTIVE status and makes it usable again for queries.


### Cancel

Cancel API to bring back index from an inconsistent state to the last known stable state. 
E.g. if index fails during creation, in `CREATING` state. 
The index will not allow any index modifying operations unless a cancel is called.

> Note: Cancel from `VACUUMING` state will move it forward to `DOESNOTEXIST` state. 

> Note: If no previous stable state exists, cancel will move it to `DOESNOTEXIST` state.


# Index Type

Hyperspace provides several index types.

* Covering Index
  * Roughly speaking, index data for `CoveringIndex` is just a vertical
    slice of the source data, including only the indexed and included columns,
    bucketed and sorted by the indexed columns for efficient access.
* Data Skipping Index
  * DataSkippingIndex is an index that can accelerate queries by filtering out
    files in relations using sketches.


# Querying with Hyperspace


## Enable Hyperspace

Hyperspace provides APIs to enable or disable index usage with Spark™.

* By using enableHyperspace API, Hyperspace optimization rules become visible to the Apache Spark™ optimizer, 
and it will exploit existing Hyperspace indexes to optimize user queries.
* By using disableHyperspace command, Hyperspace rules no longer apply during query optimization. 
You should note that disabling Hyperspace has no impact on created indexes as they remain intact.


## List indexes

You can use the indexes API which returns information about existing indexes as a Spark™'s DataFrame.
For instance, you can invoke valid operations on this DataFrame for checking its content or analyzing it further 
(for example, filtering specific indexes or grouping them according to some desired property).


## Index Usage

In order to make Spark™ use Hyperspace indexes during query processing, the user needs to make sure that Hyperspace 
is enabled. After Hyperspace is enabled, without any change to your application code, Spark™ will use the indexes 
automatically if it is applicable.


## Explain

Explains how indexes will be applied to the given dataframe.
Explain API from Hyperspace is very similar to Spark's `df.explain` API but allows users to compare their original plan 
vs the updated index-dependent plan before running their query. 
You have an option to choose from html/plaintext/console mode to display the command output.


# Supported Data Format

* Parquet
* Delta Lake
* Iceberg
* CSV
* JSON


# Supported Language

* Scala
* Python
* C#


# Appendix
