# Conformance suite history

A dated log of how the conformance test suite has grown: tests added, tiers
broadened, and targets brought into the run. Newest first.

## 2026-06-30

Grew to 824 tests, up 7, in two parts: two sibling-parity gaps where one half of
a rule was pinned and the other was not, and the capacity accounting of
conditional and idempotent transactional writes. All characterised against real
DynamoDB in eu-west-2.

The first covers the LSI side of the INCLUDE-projection-without-NonKeyAttributes
rejection; the GSI side already had it. An LSI declared with ProjectionType
INCLUDE and no NonKeyAttributes is rejected as a ValidationException in tier1 and
pinned to the exact message in tier3, the same wording the GSI case returns.

The second pins the Query message for Select SPECIFIC_ATTRIBUTES with no
ProjectionExpression, which Scan already had. Query and Scan enforce the same rule
but word it differently: Query wraps the phrase in the "1 validation error
detected:" envelope, Scan returns it bare.

The transaction cases settle what a conditional TransactWriteItems actually costs.
A passing condition adds no read capacity: a conditional write bills the same 2 WCU
per sub-1KB item as an unconditional one, and a standalone ConditionCheck costs 2
WCU, billed as write not read. Idempotent replay splits the accounting - the first
call reports 2 write capacity units, a same-token replay within the window reports
2 read capacity units for re-reading the stored result. A failing condition cancels
the transaction, and the response carries no ConsumedCapacity at all. Answers #27.

## 2026-06-26

Grew to 817 tests, up 55, covering DynamoDB's 32-level document nesting limit,
number-set equality precision, Number format on PutItem, and a sweep of
parameter-combination rejections behind a new negative-path filter. The nesting cases
pin the boundary on both paths - a stored item via PutItem and an ExpressionAttributeValue
in a ConditionExpression - accepting 31 levels of map nesting and rejecting 32 with the
same ValidationException, captured against real DynamoDB in eu-west-2. The
expression-value error comes back bare, before the condition is evaluated, not wrapped
as an invalid-value message. The precision case pins that two number sets differing
only in the last of 18 digits are distinct, where an f64 comparison would collapse them
and report a match.

A negative-path tag now marks every wholly-rejection describe across all three tiers, so
a run can select or exclude the rejection class with `--tags-filter` - something
directory filtering cannot do, because the negative describes are not all under tier3/.
The new rejection cases it makes filterable pin parameter combinations that are each
well-formed but illegal together, caught before any read: Scan parity for the
Select/ProjectionExpression and consistent-read-on-a-GSI rules already held on Query;
DeleteItem and BatchGetItem rejecting a legacy non-expression parameter mixed with its
modern expression equivalent; and three CreateTable contradictions - PAY_PER_REQUEST
with a ProvisionedThroughput, a GSI INCLUDE projection with no NonKeyAttributes, and a
stream left disabled while a StreamViewType is set. Each was characterised against real
DynamoDB in eu-west-2 and asserts the contractual phrase, floating the region-varying
envelope.

The Number format cases pin which N strings DynamoDB accepts, how it normalises them on
read-back, and which it rejects. A leading '+' on the mantissa is accepted and dropped
(+5 stored as 5), along with the bare-decimal and trailing-dot forms (.5, 5., 1.e5);
1+2, 1.2.3, +e2, a digitless exponent, and any surrounding or internal whitespace are
rejected with a ValidationException. A '+'-prefixed numeric sort key normalises to the
same key as its bare form. Captured against real DynamoDB in us-east-1.

## 2026-06-24

Grew to 762 tests, up 18, covering a malformed value in the lookup Key of a
TransactWriteItems Update, Delete, or ConditionCheck - the path a Put item key does
not take. Captured across four regions (eu-west-2, us-east-1, ap-southeast-2,
eu-central-1), where every string was identical, so they pin exactly. An empty-string
Key surfaces as a top-level ValidationException with the same message a Put item key
gives; a wrong-typed or non-scalar Key cancels with a ValidationError reason carrying
"The provided key element does not match the schema" - the key-only form, not the
"Type mismatch for key" message the item-key path returns. The same run confirmed the
BatchWriteItem table-key schema-mismatch message is region-invariant.

## 2026-06-23

Grew to 744 tests, up 8, pinning the Select / ProjectionExpression rules on Query
and Scan. A ProjectionExpression is only valid with Select SPECIFIC_ATTRIBUTES, and
ALL_PROJECTED_ATTRIBUTES is only valid with an IndexName; real DynamoDB rejects both
with a ValidationException before reading anything. The cases span ALL_ATTRIBUTES,
COUNT, and ALL_PROJECTED_ATTRIBUTES, including the request that breaks both rules at
once, where AWS reports the ProjectionExpression one. They assert the contractual
phrase, so they hold whether or not the engine carries the wrapper AWS adds on Query
but not Scan.

## 2026-06-22

Grew to 736 tests, up 30, covering what TransactWriteItems and BatchWriteItem do
with an item whose key value is malformed - the wrong type, non-scalar, or an
empty string - across both table and index keys. PutItem already covered this;
the transactional and batch paths covered none of it.

Characterising it against real AWS turned up a split worth pinning. An
empty-string key value is rejected by up-front input validation, so even inside a
transaction it surfaces as a top-level ValidationException. A wrong-typed or
non-scalar key value is caught while the transaction runs, so it comes back as a
TransactionCanceledException carrying a ValidationError reason rather than a
top-level error. BatchWriteItem has no cancellation path, so every variant there
is a plain ValidationException. The tests pin both halves, which catches two
opposite mistakes: an engine that wraps the empty-string case as a cancellation,
and one that surfaces the type-mismatch case as a top-level error.

## 2026-06-21

Grew to 706 tests, up 7, tightening secondary-index behaviour in Tier 1. Query
and Scan on a GSI or LSI now assert sparse membership: an item that omits the
index key stays off the index but remains on the base table. And PutItem now
rejects an item whose GSI or LSI key value is the wrong type, non-scalar, or an
empty string while the base-table keys are valid, holding an index key to its
declared scalar type the same way a table key is held.

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
