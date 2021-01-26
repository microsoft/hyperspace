# Proposal: Bloom Filter non-covering index for HyperSpace

Discussion of [#161](https://github.com/microsoft/hyperspace/issues/161) Bloom Filter.

## Abstract

A design doc proposing how we might go on implementing Bloom Filter in [HyperSpace](https://github.com/microsoft/hyperspace). 

## Background

Hyperspace currently only supports covering indexing over the datasets. The covering indexing is good
when user knows or has a pre-defined set of query's he wants to execute on the data. However, in cases where 
user wants to run some queries on certain columns which are not widely used but also want to leverage our
indexing system, maintaining a full fledged covering index can be expensive. Or another scenario where user want 
to leverage our index system, but the user data is just too big and maintaining a covering index is not 
worthwhile (storage expensive). Hence, we propose bloom filter. A non-covering index that is space-efficient 
probabilistic data structure to calculate and store and eventually benefits by reducing scan time/files.

## Proposal

In this design document, we propose an addition to hyperspace indexing system. By adding a potential first 
'non-covering' index.
Covering and non-covering index config API in Hyperspace which allows users to build indexes on their dataset.

## Rationale

[A discussion of alternate approaches and the trade offs, advantages, and disadvantages of the specified approach.]

TBD, (examples of how it can be used given in Background)

## Compatibility

[A discussion of the change with regard to the
[compatibility guidelines](../../COMPATIBILITY.md).]

TBD

## Design

Creating covering non-covering index config.
<table>
<thead>
<tr>
    <th></th>
    <th>Bloom Filter Config Design</th>
    <th>Covering Index Config Design Changes</th>
</tr>
</thead>
<tbody>

<tr>
<td>Initial Config</td>
<td colspan="2">

    sealed trait IndexConfigBase {
        indexName: String
        indexedColumns: Seq[String]
    }

    trait CoveringIndexConfig extends IndexConfigBase {
        includedColumns: Seq[String]
    }

    trait NonCoveringIndexConfig extends IndexConfigBase {
    } 
</td>
</tr>

<tr>
<td>Defining Config</td>
<td>

    case class BloomIndexConfig private (
        indexName: String, 
        indexedColumns: Seq[String], 
        expectedNumItems: Long, 
        fpp: Double, 
        numBits: Long
    ) extends NonCoveringIndexConfig

    def this(
        indexName: String, 
        indexedColumns: Seq[String], 
        expectedNumItems: Long
    )

    def this(
        indexName: String, 
        indexedColumns: Seq[String], 
        expectedNumItems: Long,
        numBits: Long
    )

    def this(
        indexName: String, 
        indexedColumns: Seq[String], 
        expectedNumItems: Long, 
        fpp: Double
    )

Or we can substitute this with 3 builders design.
</td>
<td>

    final case class IndexConfig(
        indexName: String,
        indexedColumns: Seq[String],
        includedColumns: Seq[String] = Seq()
    ) extends CoveringIndexConfig

By allowing Index Config to remain same we allow backward compatibility with older scripts.
</td>
</tr>

<tr>
<td>Additional Methods</td>
<td>
    
    // Returns the erroraneous probability of this BloomFilter returning true for an element not actually 
    // being put in this BloomFilter 
    def expectedFpp(): Double   
</td>
<td>
    
    // TODO - proposed
    def addAllIndexedColumns(columnName: String*): IndexConfig
    def removeAllIndexedColumns(columnName: String*): IndexConfig
    def addAllIncludedColumns(columnName: String*): IndexConfig
    def removeAllIncludedColumns(columnName: String*): IndexConfig
</td>
</tr>
</tbody>
</table>

<br>
Creating covering non-covering index.
<table>
<thead>
<tr>
<td></td>
<td>Covering Index</td>
<td>Non Covering Index</td>
</tr>
</thead>
<tbody>
<tr>
<td>Base</td>
<td colspan="2">

    sealed trait HyperSpaceIndex {
        def kind: String
        def kindAbbr: String
    }
</td>
</tr>
<tr>
<td>Definition</td>
<td>

    case class CoveringIndex(
        kind: String = "Covering", 
        kindAbbr: String = "CI", 
        properties: CoveringIndex.Properties
    ) extends HyperSpaceIndex
</td>
<td>
    
    case class CoveringIndex(
        kind: String = "NonCovering", 
        kindAbbr: String = "BFNC", 
        properties: CoveringIndex.Properties
    ) extends HyperSpaceIndex
</td>
</tr>
</tbody>
</table>

FileFormat to store the index log / config. <br>
TODO, mostly modify the previous schema ?

## Implementation

- [ ] Support new index configs for bloom filter
- [ ] Add a new case class for non-covering Index
- [ ] Modify IndexLogEntry to support design changes including non-covering index
- [ ] Create the index using the config
- [ ] Store the newly created index config info in a usable file format
- [ ] Plugin the index in the Filter and Join Rule for it's usability

## Impact on Performance (if applicable)

[A discussion of impact on performance and any corner cases that the author is aware of. If there is a negative impact on performance, please make sure
to capture an issue in the next section. This section may be omitted if there are none.]

TODO

## Open issues (if applicable)

[A discussion of issues relating to this proposal for which the author does not
know the solution. If you have already opened the corresponding issues, please link
to them here. This section may be omitted if there are none.]

- Understanding how the plan will look for join operation, working on it TODO(thugsatbay)