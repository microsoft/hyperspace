# Proposal: Hybrid Scan for File/Partition Mutable Datasets

Discussion at https://github.com/microsoft/hyperspace/issues/148.

## Abstract

[A short summary of the proposal.]

## Background

Hyperspace supports indexing immutable and mutable unmanaged datasets.
For managed datasets such as Delta Lake, further optimizations are possible.
For instance, instead of scanning the data lake for detecting changes to
the source data, we can "peek" into the transaction log of these systems
to determine and detect changes. 

## Proposal

In this design document, we propose an end-to-end solution for supporting
managed tables such as [Delta Lake](https://delta.io).

## Rationale

[A discussion of alternate approaches and the trade offs, advantages, and disadvantages of the specified approach.]

TBD

## Compatibility

[A discussion of the change with regard to the
[compatibility guidelines](../../COMPATIBILITY.md).]

TBD

## Design

This design directly corresponds to Tracks 2,3,4 from the [Hyperspace's roadmap](../ROADMAP.md).

### Overview

### Change Detection

### Handling Appends and Deletes

## Implementation

[A description of the steps in the implementation, who will do them, and when.]

> Note: If you want to use any images, please upload the .svg AND .png/.jpg file them to `/docs/design/img/` and link to them here.

## Impact on Performance (if applicable)

[A discussion of impact on performance and any corner cases that the author is aware of. If there is a negative impact on performance, please make sure 
to capture an issue in the next section. This section may be omitted if there are none.]

## Open issues (if applicable)

[A discussion of issues relating to this proposal for which the author does not
know the solution. If you have already opened the corresponding issues, please link
to them here. This section may be omitted if there are none.]

  - This is the first issue ([issue-link]())
  - This is the second issue ([issue-link]())
  - ...
