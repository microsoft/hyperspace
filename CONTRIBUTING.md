# Welcome!

If you are here, it means you are interested in helping us out. A hearty welcome and thank you! There are many ways you can contribute to the Hyperspace project:

* Offer PR's to fix bugs or implement new features
* Review currently [open PRs](https://github.com/microsoft/hyperspace/pulls)
* Give us feedback and bug reports regarding the software or the documentation
* Improve our design docs, examples, tutorials, and documentation

Please start by browsing the [issues](https://github.com/microsoft/hyperspace/issues) and leave a comment to engage us if any of them interests you. And don't forget to take a look at the project [roadmap](ROADMAP.md).

Here are a few things to consider:

* Before starting working on a major feature or bug fix, please open a GitHub issue describing the work you are proposing. We will make sure no one else is already working on it and the work aligns with the project [roadmap](ROADMAP.md).
* A "major" feature or bug fix is defined as any change that is > 100 lines of code (not including tests) or changes user-facing behavior (e.g., breaking API changes). Please read [Proposing Major Changes to Hyperspace](#proposing-major-changes-to-hyperspace) before you begin any major work.
* Once you are ready, you can create a [PR](https://github.com/microsoft/hyperspace/pulls) and the committers will help reviewing your PR.

**Coding Style:** Please review our [coding guidelines](/docs/coding-guidelines/scala-coding-style.md).

## Proposing Major Changes to Hyperspace

The development process in Hyperspace is design-driven. If you intend of making any significant changes, please consider discussing with the Hyperspace community first (and sometimes formally documented), before you open a PR.

The rest of this document describes the process for proposing, documenting and implementing changes to the Hyperspace project.

To learn about the motivation behind Hyperspace, see the talk [Hyperspace: An Indexing Subsystem for Apache Spark](https://www.youtube.com/watch?v=ofn53mT7H6c) from Spark+AI Summit 2020.	

### The Proposal Process

The process outlined below is for reviewing a proposal and reaching a decision about whether to accept/decline a proposal.	

  1. The proposal author [creates a brief issue](https://github.com/microsoft/hyperspace/issues/new?assignees=&labels=untriaged%2C+proposal&template=design-template.md&title=%5BPROPOSAL%5D%3A+) describing the proposal.
     > **Note: There is no need for a design document at this point.
  2. A discussion on the issue will aim to triage the proposal into one of three outcomes:
     - Accept proposal
     - Decline proposal
     - Ask for a design doc
     If the proposal is accepted/declined, the process is done. Otherwise, the discussion is expected to identify concerns that should be addressed in a more detailed design document.
  3. The proposal author [writes a design doc](#writing-a-design-document) to work out details of the proposed design and address the concerns raised in the initial discussion.
  4. Once comments and revisions on the design document are complete, there is a final discussion on the issue to reach one of two outcomes:
     - Accept proposal
     - Decline proposal

After the proposal is accepted or declined (e.g., after Step 2 or Step 4), implementation work proceeds in the same way as any other contribution. 	

> **Tip:** If you are an experienced committer and are certain that a design doc will be required for a particular proposal, you can skip Step 2 and just include the doc PR with the initial issue.

### Writing a Design Document

As noted [above](#the-proposal-process), some (but not all) proposals need to be elaborated in a design document.	

  - The design document should follow the template outlined [here](./docs/design/TEMPLATE.md) and must be named as `docs/design/GITHUB-ISSUE-NUMBER-shortname.md`.
    > Note: To obtain the `GITHUB-ISSUE-NUMBER`, you need to first open a GitHub issue and since you are in this section reading how to write a design document, it is assumed that you have already gone through a round of initial discussion in the issue and were asked to explicitly write a design document. 
  - Once you have the document ready and have addressed any specific concerns raised during the initial discussion, please open a PR. 
  - Address any additional feedback/questions and update your PR as needed. New design doc authors may be paired with a design doc *shepherd* to help work on the doc.
  - Once all the comments are address, you can check-in the design doc. It is expected that the design doc may go through multiple checked-in revisions so please feel free to open subsequent PRs to update/add more information. 

### Proposal Review

A group of Hyperspace team members will review your proposal and CC the relevant developers, raising important questions, pinging lapsed discussions, and generally trying to guide the discussion toward agreement about the outcome. The discussion itself is expected to happen on the issue, so that anyone can take part.	

### Consensus and Disagreement

The goal of the proposal process is to reach general consensus about the outcome in a timely manner.	

If general consensus cannot be reached, the proposal review group decides the next step by reviewing and discussing the issue and reaching a consensus among themselves. 

## Becoming a Hyperspace Committer

The Hyperspace team will add new committers from the active contributors, based on their contributions to Hyperspace. The qualifications for new committers are derived from [Apache Spark Contributor Guide](https://spark.apache.org/contributing.html):

  - **Sustained contributions to Hyperspace**: Committers should have a history of major contributions to Hyperspace. An ideal committer will have contributed broadly throughout the project, and have contributed at least one major component where they have taken an “ownership” role. An ownership role means that existing contributors feel that they should run patches for this component by this person.
  - **Quality of contributions**: Committers more than any other community member should submit simple, well-tested, and well-designed patches. In addition, they should show sufficient expertise to be able to review patches, including making sure they fit within Hyperspace’s engineering practices (testability, documentation, API stability, code style, etc). The committership is collectively responsible for the software quality and maintainability of Hyperspace. 
  - **Community involvement**: Committers should have a constructive and friendly attitude in all community interactions. They should also be active on the dev and user list and help mentor newer contributors and users. In design discussions, committers should maintain a professional and diplomatic approach, even in the face of disagreement.
