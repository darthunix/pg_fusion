# ADR Process

## Overview

An [architectural decision record](./adr-template.md) (ADR) is a document that describes
a choice the team makes about a significant aspect of the software architecture they’re
planning to build.

The ADR process outputs a collection of architectural decision records. This collection
creates the [decision log](.). The decision log provides the project context as well as
detailed implementation and design information.

Project members skim the headlines of each ADR to get an overview of the project context.
They read the ADRs to dive deep into project implementations and design choices.
When the team accepts an ADR, it becomes immutable. If new insights require a different
decision, the team proposes a new ADR. When the team accepts the new ADR, it supersedes
the previous ADR.

## When ADR is needed

In our context usually an ADR is based on functional requirements represented as an epic.
Some signs that you might need an ADR:
1. Implementing a new major feature
2. Huge refactoring in codebase
3. The solution can not be expressed in a couple of sentences in an issue
4. The solution has several alternatives that need to be considered
5. The decision in previous ADR needs to be reconsidered

## How to propose an Architectural Decision

Make a copy of [adr-template](./adr-template.md) in this directory. Rename it to
`{%Y%m%d}-{adr-title}.md` (e.g. `20241231-feature.md`). We use the date in a `%Y%m%d`
prefix as the sequence number of this adr, so it's possible to sort them correctly by
file name. 

Fill the sections that you find relevant for the discussion. Feel free to add additional
sections if needed.

Open a pull request to this repository and start the ADR discussion in the comments. The
suggested prefix for the commit and MR name is `adr:`.

## What happens next?

Then as the owner of an ADR and a merge request it's your task to drive it to completion.
Get comments from the relevant stakeholders, set deadlines and schedule ADR review meetings
as suggested by [AWS Guidlines][].

### ADR - Accepted

ADR is accepted when the merge request is approved by the relevant stakeholders. Then the
`status` in the template is set to accepted and the MR is merged. So every ADR that this
repository contains should be a final decision with **no unanswered questions**.

### ADR - Rejected

If the proposed change is rejected, the pull request is closed, and the ADR is not
included in the decision log.

### ADR - Superseded

When there is a need to make an update on the decision that was previously taken. Instead
of updating an old ADR, one should make an alternative ADR with a reference to the previous
one on this topic. When such an MR is approved, the previous ADR's status is changed to
`superseded by ...`.

## Useful Links

- [AWS Guidlines][]
 for ADR process
- [ADR Github Org](https://adr.github.io/)
- [Source](https://github.com/adr/madr/blob/0d4cf71fd80cef0039875ce6801af8c5ddeb525d/template/adr-template.md)
 of our ADR template

[AWS Guidlines]: https://docs.aws.amazon.com/prescriptive-guidance/latest/architectural-decision-records/adr-process.html
