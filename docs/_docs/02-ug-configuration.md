---
title: "Configuration"
permalink: /docs/ug-configuration/
excerpt: "How to configure Hyperspace for your needs."
last_modified_at: 2020-06-23
toc: true
---
| Property name                                        | Default                                                                                          | Meaning                                                                                               | Since Version |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|---------------|
| spark.hyperspace.system.path                         | (value of `spark.sql.warehouse.dir`)/indexes` (Check value of `spark.sql.warehouse.dir` under Spark configurations).                                                                   | Root directory to store Hyperspace index files.                                                     | 0.1.0         |
| spark.hyperspace.index.num.buckets                   | Equal to number of shuffle partitions (Check `spark.sql.shuffle.partitions` (default value 200) under Spark configurations). | Number of buckets to use when creating covering indexes.                                     | 0.1.0         |
| spark.hyperspace.index.cache.expiryDurationInSeconds | 300                                                                                              | Number of seconds since the last index modification action before index metadata cache is marked as stale.  | 0.1.0         |
| spark.hyperspace.explain.displayMode                 | plaintext                                                                                        | Display mode for Hyperspace explain() output. The valid set of values is: "console", "plaintext", "html".   | 0.1.0         |
| spark.hyperspace.explain.displayMode.highlight.beginTag | empty String                                                                                     | Tag to mark beginning of highlight portion in explain() output according to the display mode.         | 0.1.0         |
| spark.hyperspace.explain.displayMode.highlight.endTag   | empty String                                                                                     | Tag to mark ending of highlight portion in explain() output according to the display mode.            | 0.1.0         |
