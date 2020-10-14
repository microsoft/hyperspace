---
title: "Mutable dataset"
permalink: /docs/ug-quick-start-guide/
excerpt: "Mutable dataset guide"
last_modified_at: 2020-10-14
toc: false
classes: wide
---

This guide helps you to utilize indexes on mutable dataset.

## What does mutable mean?
Hyperspace can build indexes based on the source files in the given logical plan.
Since Hyperspace v0.3, we support file-level "mutable" dataset which means appended files and/or
deleted files under the root paths of the given logical plan.

Previously, if the original dataset is modified by adding new source files or removing source files,
we need to refresh the index which causes repartitioning full source data to utilize the index.

Now, we proposed several options to handle appended and deleted files in more efficient way.

## Choices of using index when your dataset changes

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
