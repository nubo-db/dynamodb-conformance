---
layout: layouts/prose.webc
# Hand-authored page: bump when the prose changes so the sitemap stays honest.
lastmod: "2026-08-11"
meta:
  title: Regional ground truth
  description: "Why the conformance suite scores against every AWS region rather than one, how the best-matching region becomes a target's headline, and why real DynamoDB diverges nowhere by definition."
---

# Ground truth, one region at a time

The [methodology](/methodology) covers how a score is worked out. This page is about one part of it: what the suite treats as the correct answer, and why that is now a set of regions rather than a single one.

## Real DynamoDB disagrees with itself

There is an assumption buried in "does this emulator match DynamoDB": that there is one DynamoDB to match. Mostly there is. Not always. AWS rolls behaviour out region by region, so for a while the same request gets different answers in different places.

The clearest case the suite has recorded is a `PutItem` with a `{ NULL: false }` attribute value. Through June 2026, eu-west-2 and eu-central-1 accepted it and normalised it to `{ NULL: true }`; us-east-1 and most other regions rejected it with a `ValidationException`. One API call, two different correct answers, depending on where you sent it. By mid-July AWS had brought the regions back into line and the difference was gone.

Until version 2.0.0, the suite pinned one region, eu-west-2, as ground truth. That was quietly unfair. An emulator that behaved exactly like us-east-1 on `{ NULL: false }` was marked wrong, not because it disagreed with real DynamoDB, but because it agreed with the wrong copy of it.

{% if splits.available and splits.featured %}
The `{ NULL: false }` difference has since closed, but others are open. The suite tracks {{ splits.count }} confirmed regional split{% if splits.count != 1 %}s{% endif %} today. Here is one, with what each region actually returned: {{ splits.featured.behaviour }}

<div class="not-prose space-y-3 my-6">{{ splits.featured | splitEvidence | safe }}</div>

Each cohort is a real region's answer, so an emulator matching either is behaving like some real region. Pin a single region as the only right answer and the other cohort is marked wrong for doing what AWS does elsewhere. An observed region missing from both cohorts is not a gap in the split: it had no definite recorded answer for this behaviour when the evidence was captured, and an unresolved observation is flagged rather than counted on either side. {{ splits.featured | splitCoverageNote(summary.latest.regions.observed) }}
{% endif %}

## Scored against every region

From 2.0.0, ground truth is per region. The full suite runs against real DynamoDB in every commercial region, and each emulator is scored against every region's answers. Its headline is its best-matching region: the one it agrees with most closely.

That is why the tables name a region beside each score. Most of the time an emulator matches every region equally, and the label just reads "all regions". Where it matches some regions more closely than others, the label names the ones it is closest to. eu-west-2 stays the anchor, because it is the region the suite has always measured against and the one the history here is built on, so a target that is best in eu-west-2 and five other regions reads as "eu-west-2 + 5 regions". A target that matches a region eu-west-2 disagrees with, and diverges less there, is named for that region instead.

Real DynamoDB sits at the top of every table diverging nowhere. That is not a number typed in by hand. Each region is scored against its own answers, so it agrees with itself everywhere by definition, and the zero is earned self-agreement measured the same way as every other score.

## Matching one region at a time

[The proposal that led to this](https://github.com/paritysuite/dynamodb-conformance/issues/75) suggested a looser rule: a behaviour counts as conformant if it matches *any* real region. The release went stricter. A target is scored against one region at a time, and its headline is the best single region it matches coherently.

The difference matters. Under match-any, an emulator could take eu-west-2's answer on one behaviour and us-east-1's answer on another and still score as fully conformant, even though no real deployment behaves like that. Scoring one region at a time rules it out: to score well, a target has to agree with a real place you could actually point an application at, across the behaviours at once. If you arrived from that proposal expecting the looser rule, this is the deliberate departure from it.

There is a related distinction in how a miss is read. A target that fails a region-specific behaviour by matching *no* region is further from real DynamoDB than one that matches a region other than its headline. Both are counted as a fail against the headline, but they are not the same kind of gap, and the per-target breakdown keeps them apart.

## When a region can't answer

A region that times out, gets throttled, or can't be reached during a sweep is recorded as **indeterminate**, not as a disagreement. An absent answer is not a different answer, so it scores nothing either way and cannot drag a target down.

Region health is tracked and shown. A region that scored is **observed**; one that missed a sweep but is still trusted is **unresolved**; one that has missed twice is **dropped** and sits out of scoring until it returns. A region that fell out is named rather than quietly omitted, because a missing region is otherwise indistinguishable from one that agreed, which is the exact confusion this release set out to remove.

## What a score means now

Taken together: a score is conformance to real DynamoDB, measured against every region it runs in, on a named date, headlined by the region the target matches best. Behaviour varies by place and shifts over time, so both sides of the comparison move. The *wording* of validation errors varies by region as well, but the suite matches on error type and field, not the prose, so that difference is deliberately not scored.
