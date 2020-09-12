# Proposal: Hybrid Scan for Mutable Datasets

Discussion at https://github.com/microsoft/hyperspace/issues/150.

## Abstract

[A short summary of the proposal.]

## Background

Hyperspace supports indexing immutable datasets. When the underlying data 
changes, users have to invoke `refresh` which will either fully or 
incrementally rebuild the index. While these approaches are simple and clean, 
for relatively small operations on the underlying datasets, it is cumbersome
for the user to remember to invoke index maintenance operations (failing which,
Hyperspace disables index usage).

## Proposal

In this design document, we propose an enhancement to the existing Hyperspace
Optimization process through a technique called **Hybrid Scan**. Hybrid Scan
allows users to continue benefiting from indexes even when their underlying
data changes, without having to manually invoking index maintenance operations.

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

### Hybrid Scan Strategy

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
