---
title: "Introduction"
permalink: /docs/toh-introduction/
excerpt: "Introduction"
last_modified_at: 2020-06-23
toc: false
---

At large companies, it has now become typical to store datasets ranging in size
from a few GBs to 100s of PBs in data lakes. The scope of analytics 
on these datasets ranges from traditional batch-style queries 
(e.g., OLAP) to explorative, *finding needle in a haystack* type of 
queries (e.g., point-lookups, summarization etc.). Resorting to 
linear scans of these large datasets with huge clusters for every 
simple query is prohibitively expensive and not the top choice for 
many of our customers, who are constantly exploring 
ways to reducing their operational costs – incurring unchecked 
expenses are their worst nightmare. One way to alleviate this
issue would be to bring in ‘indexing’ capabilities (which come 
*de facto* in the traditional database systems world) into Apache Spark™.

Among many ways to improve query performance and lowering resource 
consumption in database systems, indexes are particularly efficient in 
providing tremendous acceleration for certain workloads since they could 
reduce the amount of data scanned for a given query and thus also result 
in lowering resource costs. 

Hyperspace is envisioned to be an indexing subsystem for Apache Spark 
that introduces the ability for users to build, maintain (through a 
multi-user concurrency model) and leverage indexes (automatically, 
without any changes to their existing code) on their data (e.g., CSV, 
JSON, Parquet etc.) for query/workload acceleration. 

The rest of the documentation covers the necessary foundations behind Hyperspace 
including the API design, and how it leverages Apache Spark™ Catalyst optimizer to 
provide a transparent user experience.
