---
layout: layouts/prose.webc
# Hand-authored page: bump when the prose changes so the sitemap stays honest.
lastmod: "2026-07-30"
meta:
  title: About
  description: "Why an independent DynamoDB conformance suite exists, how it scores emulators against live AWS, and what the tiers mean."
---

# Why measure conformance?

There's no official conformance suite for DynamoDB. AWS doesn't publish one, so every emulator author ends up guessing at how the real thing behaves and testing against their own assumptions. The closest the community had was Dynalite's test suite, and by the start of 2026 over half of its tests had drifted out of step with current DynamoDB. DynamoDB Local ships with no test suite at all.

That's the gap the [conformance suite](https://github.com/paritysuite/dynamodb-conformance) fills. It runs every test against real DynamoDB on AWS first, records what passes, and treats that as the baseline. An emulator only passes a test if it gives the same answer DynamoDB does. Real DynamoDB is the ground truth, which is why it sits at the top of every table diverging from itself nowhere - not because it scored well, but because it's the thing everything else is measured against. DynamoDB doesn't always behave the same way in every region, so the suite records the answer in each region and scores a target against all of them; the [methodology](/methodology) has the detail.

## "Works with the SDK" isn't the same as "behaves like DynamoDB"

Plenty of emulators will happily take your requests and hand something back. The question that matters is whether they hand back the *same* something. A subtly different error, a validation check that fires in a different order, a filter that returns one extra item - none of it breaks the happy path, all of it bites you in production when your code was quietly relying on the real behaviour.

So the suite tests observable behaviour and nothing else. It drives the standard AWS SDK against an HTTP endpoint and asserts on what comes back: the response shape, the error it returns (its type, the field it objects to, and the constraint it failed), the order things are validated in. No internal hooks, no privileged access. If your application talks to the target through the SDK, that's precisely what gets checked.

## Three tiers, because one number hides too much

A target that gets 8% of the suite wrong tells you almost nothing on its own. Eight percent of *what*? Get 8% of the core operations wrong and you'll feel it constantly; get 8% of the strictest edge cases wrong and it's likely still fine for local development. Those are very different situations behind the same number, so the suite splits its tests into three tiers and reports divergence within each.

**Tier 1 - Core.** The operations roughly 90% of DynamoDB users rely on: CRUD, queries, scans, batch operations, GSIs, UpdateTable. The more an emulator diverges here, the more often everyday code will hit a difference.

**Tier 2 - Complete.** Documented but less common features: transactions, PartiQL, LSIs, TTL, streams, tags. A gap here only matters if you actually use that feature.

**Tier 3 - Strict.** Validation ordering, error behaviour at a range of strictness - exact where DynamoDB's wording is stable, structural (the type, field and constraint) where its rendering is non-deterministic - limits, and legacy API shapes. Missing some of this is usually fine when you're developing locally, where the exact error string rarely matters. It matters far more in CI: if your own test suite runs against an emulator and asserts on error messages or validation behaviour, a Tier 3 gap is exactly the kind of thing that lets a bug through a green build and only shows up against real DynamoDB in production.

So a target diverging 8% over the whole suite might be right about every core operation and wrong about a fifth of Tier 3, or the other way round. The tier columns say which, and only one of those two is a problem for most people.

## Skips are scope, fails are bugs

Skips here are deliberate. Each test file probes for feature support up front and skips itself if the target doesn't implement that operation at all. A skip says "I don't do this"; a fail says "I do this, but I get it wrong". Those are different problems for whoever is relying on the thing: an operation a target declines is something you find in minutes and plan around, and one it quietly gets wrong is something you find in production.

So the board reports them apart. **Divergence** counts the fails against the whole suite. **Coverage** says how much of the suite the target implements. A target with a narrow surface that gets it right shows a low divergence and a low coverage, and both are true at once. Because the denominator is the whole suite either way, a fail that turns into a skip leaves both figures together: divergence and coverage fall by exactly the same amount, so a target that stops attempting something it got wrong cannot improve one without paying for it in the other. The letter grade on each row is a reading of the pair, never a sum of it: divergence sets the letter and low coverage can only cap it, under [criteria the methodology publishes in full](/methodology#grading). The [methodology](/methodology) works the rest through.

## Why a whole site for it

The suite already publishes its latest numbers as a table in its README. That's fine for a single snapshot, but a markdown table can't show whether an emulator is improving or regressing, it can't carry a per-tier breakdown without overflowing the page, and it throws away every run before the current one.

The data to tell the fuller story was there all along. Every run stamps its results into the repo, and the git history holds every past snapshot. So this site reads that history and rebuilds the whole timeline: the [latest results](/) with run-over-run movement, a page for [each target](/targets), and [every recorded run](/runs) you can step back through.

Every figure here is derived from the suite's own results at build time. None of it is typed in by hand, which is the point - the moment you copy a number into a second place, the two start drifting apart, and conformance figures that drift are worse than none at all.

## On independence

The suite and this site are built and maintained by [Martin Hicks](https://martinhicks.dev), who also maintains Dynoxide - one of the targets scored here. That's why the no-figure-by-hand rule matters for more than drift: it's what keeps the scoring honest. Every number is derived from the suite's own results at build time, and the [scoring logic is shared with the suite](/methodology) rather than restated here. A target's score can't be tuned without changing the suite's published results first, in the open, and the code that turns those results into the numbers on this page is in the same public repository as the tests that produced them. Clone it and run the build, and you get what you see here. Real DynamoDB is the baseline, every figure carries the region and date it was measured, and [anyone can suggest a target](https://github.com/paritysuite/dynamodb-conformance/issues). There's more for programmatic consumers, and the raw data, on the [agent guide](/for-agents).

The letter grades need a second answer, because that argument only covers the figures. Where the bands and the caps sit was a choice, and moving one regrades targets whose results never changed - no run required, nothing to notice. So the [criteria are versioned and dated](/methodology#grading): they are grading criteria version {{ gradingCriteria.version }}, and any change to a band, the coverage weight or the A+ gate bumps the version and is dated on that page. Both figures stay printed beside every letter, so you can recompute a grade yourself and check it, and a retune leaves a record rather than a quietly different board.
