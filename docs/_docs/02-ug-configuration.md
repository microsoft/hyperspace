---
title: "Configuration"
permalink: /docs/ug-configuration/
excerpt: "How to configure Hyperspace for your needs."
last_modified_at: 2020-06-23
toc: true
---
| Property name                                        | Default                                                                                          | Meaning                                                                                               | Since Version |
|------------------------------------------------------|--------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------------|---------------|
| spark.hyperspace.system.path                         | "root/spark-warehouse/indexes"                                                                   | Folder to store Hyperspace index files.                                                     | 0.1.0         |
| spark.hyperspace.index.num.buckets                   | Equal to number of shuffle partitions (Check 'spark.sql.shuffle.partitions' under Spark configurations). | Number of buckets to use for a Hyperspace index upon creation.                                     | 0.1.0         |
| spark.hyperspace.index.cache.expiryDurationInSeconds | 300                                                                                              | Maximum time elapsed from the last index modification action before index cache content is marked as stale.  | 0.1.0         |
| spark.hyperspace.explain.displayMode                 | plaintext                                                                                        | Display mode for Hyperspace explain() output. The valid set of values is: console, plaintext, html.   | 0.1.0         |
