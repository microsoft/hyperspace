---
title: "Indexes on the Lake"
permalink: /docs/toh-indexes-on-the-lake/
excerpt: "Index organization on the lake."
last_modified_at: 2020-06-23
toc: false
---

To accomplish the goals outlined in [Design Goals](/hyperspace/docs/toh-design-goals/), 
Hyperspace stores all index metadata in the lake, without any external dependencies. 

The following shows the organization of the index metadata on the data lake file 
system.

```bash
Filesystem Root
├── /indexes/
|  └── <index name>
|     ├── _hyperspace_log      # Hyperspace operation log
|     |   ├── create (active)  # Entry indicating that the index got created
|     |   ├── refresh (inc)    # Entry indicating that the index is getting refreshed
|     |   └── active           # Entry indicating that the index is active again
|     ├── ...                  
|     ├── <index-directory-1>  # First 'version' of the index ---
|     ├── <index-directory-2>  # -------------------------------- |- Second version
```

As shown, we store all indexes (or *derived datasets*, in the more general sense) 
at the root of the file system. An alternative
design we considered was to co-locate the index with the dataset. However, 
since Hyperspace intends to support a more general notion of indexes such as 
materialized views (which can span datasets), Hyperspace decouples the index 
location from the original data location. 
