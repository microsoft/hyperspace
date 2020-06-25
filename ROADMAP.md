# Hyperspace Roadmap

This document defines a high level roadmap for Hyperspace development and upcoming releases. 
Community and contributor involvement is vital for successfully implementing all desired 
items for each release. We hope that the items listed below will inspire further engagement 
from the community to keep Hyperspace progressing and shipping exciting and valuable features.

**Note**: Any dates listed below and the specific issues that will ship in a given milestone 
are subject to change but should give a general idea of what we are planning. We use the 
milestone feature in Github so look there for the most up-to-date and issue plan.

If you see a problem on this, or something is not clear, please consider 
[opening an issue](https://github.com/microsoft/hyperspace/issues).

## Table of Contents

- [Zooming Out](#zooming-out)
- [Short-term](#short-term)
- [Long-term](#long-term)

## Zooming Out

Before we discuss the short and long term plans, it is useful to look at the big picture.
The figure below shows the various investments in Hyperspace. 

![Icon](https://github.com/rapoth/hyperspace/blob/master/docs/assets/images/hyperspace-roadmap.png?raw=true)

In short, there are **six** parallel tracks of work happening within Hyperspace. While we use the
word *track* to imply that work can happen mostly independently without depending on design
decisions from the other tracks, should the community notice such a hard dependency, we should
work towards making the tracks independent, to the extent possible.

The best way to understand the tracks shown in the figure is:
  - **Track 1**: This is the foundation where we ensure that the meta-data we are
    storing is enough to capture all the context for an index. For instance, there are a number
    of questions that pop up when using indexes **instead** of the original base data:
      - Is the index up-to-date?
      - Were there any transformations applied?
  - **Tracks 2,3,4**: These tracks deal with the immutability aspect of the underlying
    data. The current focus of Hyperspace is on supporting indexing for *immutable datasets*
    (meaning that the only way to refresh the index would be to rebuild it in full). In
    the upcoming months, we hope to focus on the other aspects allowing us to index data
    that is getting updated (either append-only, or updated like how it happens in Delta Lake).
  - **Track 5**: One of the most frequently asked questions with indexing subsystems is
    *what to index?*. This track of Hyperspace focuses on finding a reasonable answer to this
    question. The current focus is on rule-based recommender systems, which are simple
    and effective, although not the best. Subsequently, the focus of Hyperspace would explore
    more sophisticated approaches like hypothetical indexes.
  - **Track 6**: Since indexes are a relatively new concept to data lakes, there are lots
    of gotchas in terms of how one would create and maintain them. In addition, there are
    lots of design decisions that go into supporting an index. An important challenge that 
    Hyperspace primarily deals with is ensuring there is a balance between customizability
    and simplicity. Hyperspace chooses to provide simple APIs, with options to customize.
    This aspect of Hyperspace focuses on documenting all the surrounding concepts and 
    writing accessible tutorials for users.

We will refer to each of these tracks in the format **T<num>** e.g., T1, where applicable.

## Short Term

In the short-term (i.e., next 3-6 months), the focus of Hyperspace would be on work that
spans the following categories:

  - **Bug fixes** - We **do not** recommend using Hyperspace in production. However, we do
    encourage trying it out on your workloads and telling us what you like vs. what you would
    like to see. Our primary focus is on fixing any usage bugs that are reported in real-world
    usage.
  - **Stability improvements** - Includes aspects such as challenging the API design to ensure
    it is robust enough for evolving to support other index types, ensuring backward/forward
    compatibility of the meta-data (**T1**) and verifying that Hyperspace works correctly and consistently.
  - **Optimizer enhancements** - Hyperspace implements optimizer rules to perform index matching.
    It may be possible that there is definitely scope of improving these rules to achieve better
    optimizations.
  - **Robust support for Immutable (T2) & Append-only (T2) Datasets** - Includes aspects such
    as providing incremental indexing support with the necessary foundational APIs for optimizations.

## Long Term

The long-term (i.e., next 6-12 months), the focus of Hyperspace would be on work that validates the
underlying idea, obtaining community feedback on figuring out ways to decentralize the development
of Hyperspace (e.g., across tracks) to make consistent progress. That being said, the immediate
next focus would be on work that spans the following categories:

  - **Index Recommendation** - To make Hyperspace immediately usable, it is critical to invest
    in index recommendation (even if it is a simpler rule-based engine). This aspect of Hyperspace
    would also include necessary work for doing cost-benefit analysis and laying down the necessary
    foundation.
  - **Robust support for Updateable (T3) Datasets** - Users are increasingly resorting to using
    engines such as Delta Lake and Hudi to deal with update semantics on data lakes. Hyperspace
    intends to first explore the cost of providing support for these engines and potentially
    providing first-class support.
  - **More index types** - Hyperspace implements support for one specific type of an index called
    the covering index. The long-term focus of Hyperspace would entail supporting other kinds of
    index types (or hyperspaces).
