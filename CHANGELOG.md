# Conformance suite history

A dated log of how the conformance test suite has grown: tests added, tiers
broadened, and targets brought into the run. Newest first.

## 2026-05-26

Grew to 684 tests with a control-plane and table-configuration sweep: the
CreateTable/UpdateTable config parameters and the secondary control-plane
operations (limits, backups and PITR, exports and imports, Kinesis, resource
policies, contributor insights), each characterised against real AWS and
probe-skipped where a target doesn't implement it.

The published percentage changed with it. It now measures correctness over
implemented operations, Pass / (Pass + Fail), so skips no longer count against
the score. A skip is honest scope; a fail is a bug.

## 2026-05-24

Grew to 625 tests, up 24 on the previous run: eleven more in Tier 1 and
thirteen more in Tier 3, tightening coverage of core operations and the strict
edge cases.

## 2026-05-23

ExtendDB joined the run as a target. The suite itself held steady at 601 tests.

## 2026-04-27

Grew to 601 tests, up 29, and every new test landed in Tier 3: a broader strict
surface spanning validation ordering, exact error messages, service limits, and
the legacy request shapes. Floci and Ministack were added to the run as targets
the same day.

## 2026-04-24

Grew to 572 tests, up 46 on the first run: thirty-six more in Tier 1 and ten
more in Tier 2, deepening coverage of the core and complete-feature behaviour.

## 2026-03-23

The suite was established with 526 tests across the three tiers (267 Tier 1, 93
Tier 2, 166 Tier 3), run against Dynalite, DynamoDB Local, Dynoxide, and
LocalStack, with live AWS DynamoDB as the baseline.
