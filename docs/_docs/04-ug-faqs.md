---
title: "Frequently Asked Questions (FAQs)"
permalink: /docs/ug-faqs/
excerpt: "Frequently asked questions about Hyperspace"
last_modified_at: 2020-06-20
toc: true
toc_label: "FAQs"
toc_icon: "cogs"
---

## General
### What is an index?
Exploiting indexes is a widely used techniques to improve query performance in databases.
An index is an auxiliary data structure which is organized and stored in a way that it 
optimizes certain kinds of data retrieval operations. Any subset of the fields of 
original data records can serve as the fields for an index. An index normally speeds up 
search operations that are not efficiently supported on original data records due to 
their physical arrangement in the storage.

### What type of an index does Hyperspace create? Do I have any control over it?
Hyperspace currently uses **covering indexes** although we have active plans on supporting
other kinds of indexes in the future. Covering Indexes are known to be efficient in 
scenarios where co-occurrence of certain columns happens frequently in query activities 
for selection and filtering.

A covering index contains a list of key columns, called “indexed columns”, and a list of 
data columns, called “included columns”. The data for both columns is **copied** from 
the original data. Indexed columns are used to rearrange data rows and organize them 
as index rows when storing on a storage medium. Hyperspace stores index files in 
Parquet format. Parquet is a popular columnar format which is known to be efficient 
for storage and performant for analytics workloads. Each index in Hyperspace is 
identified by a user-provided unique name. User can create an index via “createIndex” 
command in which he specifies data records to be indexed along with the index name 
and lists of indexed and included columns. For a simple example on how to do this,
you can refer to our [Quick-Start Guide](https://microsoft.github.io/hyperspace/docs/ug-quick-start-guide/).

#### How do I decide which columns to index?
A number of factors in your data and query workload determine which columns to index. 
There are two cases in a query's predicate where Spark would exploit Hyperspace indexes: 
* Lookup or range selection filter.
* Join with an equality predicate on join columns (i.e. equi-join).

In your queries, if you have filtering predicates such as:
* `Column = Constant`, or 
* `Constant1 <= Column <= Constant2` 

and these filters are highly selective (i.e. a small portion of total data records 
qualify for the predicate) then such a column is good candidate to be indexed.

If your queries combine records from large tables using equi-join with a predicate such as: 
* `Table1.ColumnA = Table2.ColumnB` 

then ColumnA and ColumnB are good candidates to create index on.

#### How can I force Hyperspace to use my index?
Hyperspace provides commands to enable and disable index usage. Using “enableHyperspace” 
command, existing indexes become visible to the query optimizer and Hyperspace would 
exploit them, if applicable to a given query. By using “disableHyperspace” command, 
Hyperspace will no longer consider using indexes during query optimization.

#### How can I check if Hyperspace indexes are used in my query?
You can use the "explain" command in Hyperspace to check if a given query would use 
indexes when running. Assuming your query dataFrame is DF, running `hyperspace.explain(DF)` 
prints out the query plan for two cases: when Hyperspace is enabled and disabled. 
If any Hyperspace index is used for the enable case, explain's output lists all such 
indexes and highlights portion(s) of the plan where those indexes make a difference.

# Debugging
#### I am running a query which uses Hyperspace indexes, but I do not see significant performance improvement. What could be the reason?
How much an index could help a query depends on many factors including the total 
data size, total index size, value distribution for indexed and non-indexed columns, 
and different expressions and predicates appearing in the query. Assuming that the 
query plan is carefully checked and it shows expected indexes are used, there are 
several cases you can check: 

* _Is your original data too small?_ 
  An index which is created on a small table (e.g., MBs, 10s of GBs) normally does 
  not help much, especially when you are using a big cluster (e.g., 10s of cores). 
  This is because scanning a table whose total size is small is typically fast 
  enough that switching to an index for that table shows a similar performance.
* _Are you using filter indexes?_ 
  If an index is used for a filter predicate (lookup or range scan), then you 
  should check the ‘selectivity’ of that filter. Selectivity of a filtering predicate 
  on a table is percentage of the rows that satisfy the filtering conditions. It 
  is essentially defined as 
  `[Number of rows that pass the predicate] / [Total number of rows in the table]`. 
  An index would not improve performance much for a filter which selects a large 
  portion of row. For instance, if Spark ends up fetching all or a large percentage 
  of original records, reading from index basically behaves similar to scanning 
  original data files. 
* _Are there other intensive operations in your query?_
  Parts of the query plan which do not necessarily use an index can have a major 
  impact on the overall performance of query if they dominate the performance 
  gain achieved by using index. There are operations which are known to take a 
  long time on large data (e.g., nested loop join). It is a good idea to 
  carefully explore your query plan and look for such operations along with 
  exchange (shuffle) or sort operators that need to process large amounts of 
  input data. During query execution on large scales of data, such operations 
  could take a long time and hide any performance improvement you achieved 
  through using indexes. As an example, if a nested loop join takes more than 
  80% of total query time then any improvement in other parts of the query 
  (e.g., that an index has provided) would not be noticeable.

# Advanced

#### What can I do to tune my index?
While there is no single set of recommendation that will tune an index for all
use cases, it is useful to understand what is happening in Hyperspace. 

The list of indexed columns, defined by user has an impact on the physical 
layout of index records when stored in the Parquet format. Hyperspace uses 
Spark's partitioning and bucketization to re-arrange data records according 
to the indexed columns during index creation. If two rows in the original 
data have same values for the columns marked as indexed columns, they will 
end up in the same partition. An index is expected to perform better if its 
partitions have similar number of records (i.e. records are uniformly 
distributed across partitions). As a result, you need to make sure that the 
columns picked as indexed columns do not have a low cardinality of distinct 
values in original data. If many records have same values for indexed 
columns, the final arrangement of indexed entries on disk could degrade 
index usage performance. 

For example, a boolean column is normally not a good choice for an indexed 
column. There are only two possible values for a boolean column which could 
lead into skew when saving index records on disk. Bucketing is further 
optimization that Spark uses to organize data so that it avoids expensive 
exchanges (shuffles) during operations such as Join. User can override 
number of buckets used for index creation by changing the 
"spark.hyperspace.index.num.buckets" configuration in the Spark session. For
a full list of configuration you can tweak, please see 
[Configurations](https://microsoft.github.io/hyperspace/docs/ug-configuration/).
