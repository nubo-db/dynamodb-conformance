---
layout: layouts/prose.webc
# Hand-authored page: bump when the prose changes so the sitemap stays honest.
lastmod: "2026-07-29"
meta:
  title: For agents
  description: "How to read Parity Suite's conformance scores, and where to get them as machine-readable data, for agents and anyone consuming the suite programmatically."
---

# Reading these scores

This page is for anyone consuming the suite programmatically - an agent, a dashboard, a script - and for anyone who wants to read a number here and know exactly what it means. A single percentage is easy to misread as a verdict, so here's how the figures are built and where to get them as data.

## Get the data, don't scrape the page

Every figure on the site is published as JSON, regenerated at build time from the same results the pages render from. Read that instead of parsing HTML:

- [/data/latest.json](/data/latest.json) - the latest run in full: every target's divergence and coverage, overall and per tier, its per-capability and per-operation-area state, and the full per-region breakdown, alongside the run's region health.
- [/data/runs.json](/data/runs.json) - the whole history, newest first: per-target divergence and coverage, overall and per tier, plus run-over-run movement and headline region for every recorded run.
- [/data/index.json](/data/index.json) - a discovery manifest: the tier, capability and region vocabularies, where each endpoint lives, and the licence.
- [/feed.xml](/feed.xml) - an Atom feed, one entry per run.

Every target carries the identical schema, live AWS DynamoDB included. The data is published under [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/): use it freely, just credit paritysuite.org. The schema is versioned with a `schemaVersion` field, and a breaking change bumps it.

Every endpoint also carries a `metrics` block naming each published figure, its formula and its `direction` (`lower_is_better` for divergence, `higher_is_better` for coverage and correctness). Read the direction from there rather than assuming it. Schema 3 reversed which way is good, and a consumer that re-derived its own ranking on the old assumption would have inverted with nothing in the shape of the data to catch it.

## What a score actually is

Every target carries two figures over the whole suite. **Divergence** is failed divided by total: how much of DynamoDB's behaviour the target answers differently, so lower is better. **Coverage** is implemented divided by total: how much of the suite's tests it implements at all, so higher is better. Read both. A target with a thin surface that gets it right shows a low divergence and a low coverage, and folding them into one number would lose that.

The denominator is the same for both and never moves, which fixes their relationship: a test that goes from failing to skipped leaves both numerators at once, so divergence and coverage fall by exactly the same amount. If you are tracking a target over time, a divergence fall matched by an equal coverage fall is a withdrawal, not a fix.

The JSON keeps the raw counts (`passed`, `failed`, `skipped`, `implemented`, `total`) alongside both, so you can derive whatever figure you need rather than depending on the one the board leads with. The pass rate over implemented operations is still published, as `correctness`, for consumers that already read it. It used to be `total`, which is also the name of the raw test count in `counts`, so the same word meant a count in one place and a percentage in another.

From schema 4 each target also carries a `grade`: a letter (`A+` to `F`), a plain-language `qualifier`, a `band` (the colour tier: `pass`, `partial`, `fail` or `none`), and a `capped` flag. A target with nothing scored has `letter: null` and `qualifier: "not scored"` - handle that case before recomputing, because a literal reimplementation of the bands mis-grades a null (`null < 5` is `true` in JavaScript). A letter change between tested runs travels as `movement.gradeChange` (`{from, to, label}`), null when the letter held. Two more fields keep the data at parity with the cards: `runDate` is the run that actually measured the row (it lags the run's own date when a target was carried forward untested), and `region.worst` is the worst observed region's divergence - the figure behind a row's "up to X% in the other N" clause - null when the target has no regional spread. The grade is a reading of the two figures, never a blend of them - divergence sets the letter, with A+ meaning exactly zero failing tests rather than a figure that rounds to 0.0%, and low coverage can only cap it - and the full criteria (bands, the exact-zero A+ rule, the coverage caps) travel in `metrics.grade`, versioned as `gradingVersion` separately from the schema, because a criteria change regrades targets whose figures didn't move. Recompute the letter from the two values if you need to check it; the [methodology](/methodology#grading) has the same criteria in prose. A grade is an observation against this suite's tests on a named date, not a certification, and not an endorsement.

[Skips are scope, not failure.](/about) A skipped test is the target's own feature-probe declining to run because it doesn't implement that operation at all. That's kept out of the score and reported separately. A fail means the operation is there and behaves differently from real DynamoDB, and that counts. They mean opposite things, so don't fold skips into a pass rate.

There are [three tiers](/about) - Core, Complete and Strict - and one figure over the whole suite hides too much. "0.0% Core, 5.0% Complete, 20.0% Strict" tells you far more than "8.3%". Each tier carries divergence and coverage on the same terms as the headline: lower is better for every divergence figure, and higher is better for every coverage one. If a user only needs everyday CRUD, the Core figure is the one that matters; if they assert on error behaviour in CI, Strict is where a gap bites. Read the tier that maps to what they actually do.

DynamoDB sits at the top of every table at 0.0% divergence over full coverage. That's the baseline, not a competitor that happened to win: it's the thing everything else is measured against, so it agrees with itself by definition.

## What the numbers don't tell you

A score is tied to a target version, tested on a date, against DynamoDB's behaviour on that date. DynamoDB is neither identical across regions nor fixed over time, so the suite scores each target against every region it can reach and headlines its best-matching one; a figure here means conformance to real DynamoDB as it behaved across the regions on a named date, nothing wider. Both sides move. The [regional ground truth](/ground-truth) page has the detail.

And it's behaviour only. The suite says nothing about performance, scalability, durability, cost, or operational fit. A target can match DynamoDB's behaviour perfectly and still be the wrong tool for a job, or the right one despite a worse figure here. The [methodology](/methodology) has the full limitations.

## Comparing on a capability

If a decision hangs on a specific feature - PartiQL, transactions, GSIs, LSIs, streams, TTL - don't read off the headline. The [capabilities page](/capabilities) lays out every target against the same capability columns, and the same data is in the `capabilities` array for each target in [/data/latest.json](/data/latest.json). Pull the column for the feature you care about and read every target's state on it. The suite scores each target against real DynamoDB, never against each other, so the comparison is like-for-like.

The site won't tell you which target to pick. It gives you the evidence per target, on equal terms.

## Who maintains this

The suite and this site are built and maintained by [Martin Hicks](https://martinhicks.dev), who also maintains Dynoxide, one of the targets scored here. That relationship is why nothing on the site is hand-authored: every figure is derived from the suite's own published results at build time, and the [scoring logic is shared with the suite](/methodology) rather than restated here. A target's score can't be tuned without changing the suite's published results first, in the open, and the tests, the results and the code that scores them are all in [one public repository](https://github.com/paritysuite/dynamodb-conformance) you can clone and run. Real DynamoDB is the baseline, every figure carries the region and date it was measured, and [suggesting a target](https://github.com/paritysuite/dynamodb-conformance/issues) is an open GitHub issue away.
