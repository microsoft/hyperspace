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

We provide several refresh modes to refresh index data.

#### Full
TODO

#### Incremental
TODO

#### Quick
TODO

### Hybrid Scan
TODO
