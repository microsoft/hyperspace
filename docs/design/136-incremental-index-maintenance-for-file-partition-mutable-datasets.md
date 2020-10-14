# Proposal: Incremental Index Maintenance for File/Partition Mutable Datasets

Discussion at https://github.com/microsoft/hyperspace/issues/136.

## Abstract

[A short summary of the proposal.]

## Background

Hyperspace supports indexing immutable datasets. When the underlying data 
changes, users have to invoke `refresh` which will fully rebuild the index.
While this approach is simple and clean, it is not scalable, especially
for large datasets. 

## Proposal

In this design document, we propose an enhancement to the existing `refresh`
API in Hyperspace which allows users to perform *incremental maintenance*
of their indexes i.e., ways to avoid a full index rebuild.

## Rationale

[A discussion of alternate approaches and the trade offs, advantages, and disadvantages of the specified approach.]

TBD

## Compatibility

[A discussion of the change with regard to the
[compatibility guidelines](../../COMPATIBILITY.md).]

TBD

## Design

This design directly corresponds to Tracks 2,3,4 from the [Hyperspace's roadmap](../ROADMAP.md).

<table>
<thead>
  <tr>
    <th></th>
    <th></th>
    <th><b>Full Rebuild</b></th>
    <th><b>Read Optimized (Quick Query)</b></th>
    <th><b>Write Optimized (Fast Refresh)</b></th>
  </tr>
</thead>
<tbody>
  <tr>
    <td><b>Append</b></td>
    <td><b>Characteristic</b></td>
    <td>Slowest refresh/fastest query</td>
    <td>Slow refresh/fast query</td>
    <td>Fast refresh/slow query</td>
  </tr>
  <tr>
    <td></td>
    <td><b>API</b></td>
    <td>hs.refreshIndex(mode="full")</td>
    <td>hs.refreshIndex(mode="smart")</td>
    <td><b>**hs.refreshIndex(mode="quick")**</b></td>
  </tr>
  <tr>
    <td></td>
    <td><b>What it does?</b></td>
    <td>Will rebuild the entire index by scanning the underlying source data</td>
    <td>Will build index on newly added data and also optimizes on-the-fly small index files</td>
    <td>Will build index ONLY on newly added data</td>
  </tr>
  <tr>
    <td></td>
    <td><b>When to use?</b></td>
    <td>Underlying source data is relatively stable</td>
    <td>Frequently appending new data</td>
    <td>Infrequently appending new data</td>
  </tr>
  <tr>
    <td colspan="5"></td>
  </tr>
  <tr>
    <td><b>Delete</b></td>
    <td><b>Characteristic</b></td>
    <td rowspan="4">Same as above; <br><br>Creates a new index (and consequently, reshuffles the source data)</td>
    <td>Slow refresh/fast query</td>
    <td>Fast refresh/slow query</td>
  </tr>
  <tr>
    <td></td>
    <td><b>API</b></td>
    <td>hs.refreshIndex(mode="smart")</td>
    <td><b>**hs.refreshIndex(mode="quick")**</b></td>
  </tr>
  <tr>
    <td></td>
    <td><b>What it does?</b></td>
    <td>
       <ul>
         <li>Deletes entries from index immediately</li>
         <li>DOES NOT shuffle the underlying source data</li>
         <li>Operates on lineage</li>
       </ul>
     </td>
    <td>Captures file/partition predicates and deletes entries at query time</td>
  </tr>
  <tr>
    <td></td>
    <td><b>When to use?</b></td>
    <td>Lots of underlying data getting deleted</td>
    <td>Little data getting removed from the underlying data</td>
  </tr>
  <tr>
    <td colspan="5"></td>
  </tr>
  <tr>
    <td><b>Optimize</b></td>
    <td></td>
    <td></td>
    <td><b>Faster Optimize Speed (Quick)</b></td>
    <td><b>Slower Optimize Speed (Full)</b></td>
  </tr>
  <tr>
    <td></td>
    <td><b>API</b></td>
    <td></td>
    <td>hs.optimizeIndex(mode="quick")</td>
    <td><b>**hs.optimizeIndex(mode="full")**</b></td>
  </tr>
  <tr>
    <td></td>
    <td><b>What it does?</b></td>
    <td></td>
    <td>
       <ul>
          <li>Changes the physical layout of the index to improve perf but across multiple DELTA indexes (i.e., d___=x index directories)</li>
          <li>May have multiple files per bucket which means it does a best-effort merge of small files</li>
          <li>DOES NOT refresh the index</li>
       </ul>
    </td>
    <td>
       <ul>
          <li>Changes the physical layout of the index to improve perf</li>
          <li>Create a single file per bucket by merging both small and large files</li>
          <li>DOES NOT refresh the index</li>
       </ul>
    </td>
  </tr>
  <tr>
    <td></td>
    <td><b>When to use?</b></td>
    <td></td>
    <td>When perf starts degrading</td>
    <td>When perf starts degrading</td>
  </tr>
  <tr>
    <td colspan="5">Legend: <b>**DEFAULT**</b></td>
  </tr>
</tbody>
</table>

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
