---
title: "Configuration"
permalink: /docs/ug-configuration/
excerpt: "How to configure Hyperspace for your needs."
last_modified_at: 2020-06-23
toc: false
classes: wide
---
| Property name                                        | Default                                                                                          | Meaning                                                                                               | Since Version |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|---------------|
| spark.hyperspace.system.path                         | "(value of `spark.sql.warehouse.dir`)/indexes" | Root directory to store Hyperspace index files.                                                     | 0.1.0         |
| spark.hyperspace.index.numBuckets                    | value of `spark.sql.shuffle.partitions`, which defaults to 200 | Number of buckets to use when creating covering indexes.                                     | 0.1.0         |
| spark.hyperspace.index.cache.expiryDurationInSeconds | 300                                                                                              | Number of seconds since the last index modification action before index metadata cache is marked as stale.  | 0.1.0         |
| spark.hyperspace.explain.displayMode                 | "plaintext"                                                                                        | Display mode for Hyperspace explain() output. The valid set of values is: "console", "plaintext", "html".   | 0.1.0         |
| spark.hyperspace.explain.displayMode.highlight.beginTag | "" (An empty string)                                                                                     | Tag to mark beginning of highlight portion in explain() output according to the display mode.         | 0.1.0         |
| spark.hyperspace.explain.displayMode.highlight.endTag   | "" (An empty string)                                                                                    | Tag to mark ending of highlight portion in explain() output according to the display mode.            | 0.1.0         |
| spark.hyperspace.index.lineage.enabled   | false                                                                                    | Add lineage column to index upon creation to track source data file for each index record. Lineage is required to handle deleted files in Hybrid Scan, or to refresh an index in the incremental mode. Adding lineage will increase the size of the index, proportional to the number of distinct source data files the index is built on. | 0.3.0         |
| spark.hyperspace.index.optimize.fileSizeThreshold   | 256MB                                                                                    | Threshold of size of index files in bytes to optimize. Files with size below this threshold are eligible for merge during index optimization.            | 0.3.0         |
| spark.hyperspace.index.hybridscan.enabled   | false                                                                                  | Enable Hybrid Scan at query execution time. If enabled, Hyperspace considers an index with appended and/or deleted source data files as a candidate during query optimization, with some additional optimization overhead. | 0.3.0         |
| spark.hyperspace.index.hybridscan.delete.enabled   | false                                                                                    | (temporary) Enable Hybrid Scan for deleted files. Lineage is required to handle deleted files in Hybrid Scan.  | 0.3.0         |
| spark.hyperspace.index.hybridscan.delete.maxNumDeleted   | 30                                                                                    | Threshold of the maximum number of deleted files for Hybrid Scan. | 0.3.0         |
| spark.hyperspace.source.globbingPattern   | N/A                                                                                    | DataFrameReader option (not a spark config) to allow indexes on globbing patterns. E.g. `spark.read.option("spark.hyperspace.source.globbingPattern", "/temp/*/*").parquet("/temp/*/*")` | 0.4.0         |
