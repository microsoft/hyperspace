---
title: "Design Goals"
permalink: /docs/toh-design-goals/
excerpt: "Hyperspace design goals."
last_modified_at: 2020-06-20
toc: true
toc_label: "Design Goals"
toc_icon: "bullseye"
---

## Design Goals

Hyperspace is designed with the following design goals in mind:

### Agnostic to data format
To support the most diverse scenarios, the indexing subsystem should be able 
to index data stored in the lake in any format, including text data (e.g., 
CSV, JSON, Parquet, ORC, Avro, etc.) and binary data (e.g., videos, audios, 
images, etc.). 

We consider this data as *externally managed*, i.e., we do not assume control 
over the lifecycle of the datasets. 

### Low-cost index metadata management
To avoid burdening the query optimizer and the end-user, our index metadata 
should be light-weight, fast to retrieve, and operate independent of a third-party 
catalog. In other words, the indexing subsystem should only depend on the data 
lake for its operation and should not assume the presence of any other service 
to operate correctly.

### Multi-engine interoperability 
Our indexing subsystem should make third-party engine integration easy. To 
achieve this, we should expose (a) index state management and (b) index 
metadata in as transparent a way as possible.

### Simple and guided user experience
The indexing subsystem should support diverse users including data scientists, 
data engineers, and data enthusiasts. Therefore, it should offer the simplest 
possible experience.

### Extensible indexing
Since it is often impractical to provide all possible auxiliary data structures 
that aid in query acceleration, our indexing subsystem should offer mechanisms 
for easy pluggability of newer auxiliary data structures (related to indexing).

### Security, Privacy, and Compliance
Since auxiliary structures such as indexes, views, and statistics copy the 
original dataset either partly or in full, the indexing subsystem should meet 
the necessary security, privacy, and compliance standards (e.g., enforcing 
deletes for GDPR requirements). 
