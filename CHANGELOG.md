# Conformance suite history

A dated log of how the conformance test suite has grown: tests added, tiers
broadened, and targets brought into the run. Newest first.

## 2026-06-09

Real DynamoDB in eu-west-2 reworded a chunk of its validation errors, and the
Tier 3 error-message tests moved with it. They now assert the contract the error
carries - its type, the field it objects to, and the constraint - rather than the
exact prose, because AWS varies the wrapper, the echoed input value and the field
casing from one region to the next. A four-region capture in June found eu-west-2
and eu-central-1 on the new wording and us-east-1 and ap-southeast-2 still on the
old, so the line between contract and cosmetic is drawn from what is invariant
across all four.

This moves some Tier 3 numbers. Targets that were only ever marked down for
wording DynamoDB itself renders inconsistently now pass those checks, so their
Tier 3 scores rise: the suite has stopped counting a cosmetic difference as a
behavioural one. Genuine behavioural divergences are still pinned exactly, for
example PutItem with a { NULL: false } attribute, which DynamoDB now accepts in
eu-west-2 and normalises to { NULL: true }.

## 2026-05-28

Grew to 699 tests, up 15 on the previous run: seven more in Tier 1 and eight
more in Tier 2.

Tier 1 picked up two behaviours. A GetItem or BatchGetItem whose projection
matches nothing on a present item still returns that item as an empty result,
rather than treating it as absent. And the TableId from CreateTable matches the
one DescribeTable reports, holding stable across repeated calls rather than
being minted fresh each time.

Tier 2 picked up PartiQL writes with a non-key WHERE predicate. DELETE and
UPDATE evaluate the whole predicate, so a false one fails with
ConditionalCheckFailed and leaves the item untouched, a write WHERE that omits
the primary key is rejected, and a DELETE on an absent key is a silent no-op.

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
