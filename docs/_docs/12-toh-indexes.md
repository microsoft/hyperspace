---
title: "Indexes et al."
permalink: /docs/toh-indexes/
excerpt: "How to configure Hyperspace for your needs."
last_modified_at: 2020-06-20
toc: true
---

## Viewing indexes as *derived datasets*

### What are *derived datasets*?
While indexes were traditionally built and maintained as auxiliary data 
structures internal to a DBMS, in a data lake, since there is no single 
*database system*, we consider indexes to be a form of *derived data* 
i.e., data that has been derived from one or more datasets and may be
optionally used by an arbitrary query optimizer to improve the speed 
of data retrieval. 

### What are their properties?
The only assumption we make about these *derived datasets* is that they:
  1. Support basic lifecycle operations such as create, delete, 
     (either full or incremental) rebuild, and restore
  2. Can be leveraged for query acceleration (in particular, be 
     integrated with query optimizers and execution runtimes). 

Therefore, covering indexes, zone maps, materialized views, statistics, 
and chunk-elimination indexes are all included when we use the
term *index*.

## Examples of derived datasets

For completeness, we discuss a few examples of derived datasets that 
Hyperspace aspires to provide out-of-box.

### Covering Index
Covering indexes are efficient in scenarios where certain selection 
and filter columns co-occur frequently in queries. They have the 
following properties: 
  1. Non-clustered: index is separated from the data.
  2. Covering: index contains both key columns (i.e., 
     *indexed columns*) and data/payload columns (i.e., *included 
     columns*); these data/payload columns are duplicated from 
     the original data (for *index only* query access paths).
  3. Columnar: index is stored in some columnar format (e.g., 
     Parquet in the case of Hyperspace) rather than a row-oriented 
     format such as a B-tree. This allows us to leverage techniques 
     like vectorization along with min-max pruning to accelerate 
     scans over indexes.

With all columns in the query being included in the covering index 
either as key or non-key columns, query performance may be significantly 
improved. Additional physical layout properties (such as bucketization, 
partitioning, and sort order) can speed up workhorse operators such as 
filter and join that typically dominate query execution time. 

In Hyperspace, all columns marked as *indexed columns* by the user are 
bucketized and (optionally) sorted.

### Chunk-Elimination Index 
For queries that are highly selective (e.g., searching for a single 
GUID amongst billions), we intend to support a class of indexes 
called *chunk-elimination indexes* analogous to a traditional inverted 
index, except that the pointer is an arbitrary URI (as opposed to a 
*row_id*) that refers to a *chunk*, a reasonable unit of addressable data
stored in data lake (e.g., a single Parquet file or an offset range 
within a large CSV file). An optimizer can leverage this index to 
quickly prune irrelevant blocks for a query.

### Materialized Views 
For expensive queries with joins or aggregations, users can create 
materialized views as derived datasets. These materialized views can 
then be used transparently by the underlying query optimizer. 
Hyperspace will only support full rebuild of materialized views to
begin with.

### Statistics
In environments with cost-based query optimizers, we intend to also 
support collection of statistics (e.g., histograms) *a priori*
for columns of interest. A capable optimizer can then leverage these 
statistics at runtime to optimize resources.
