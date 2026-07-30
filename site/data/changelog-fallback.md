# Conformance suite history

A dated log of how the conformance test suite has grown: tests added, tiers
broadened, and targets brought into the run. Newest first.

## 2026-07-21 (2.1.0)

Grew to 995 tests, up 41, all characterised against real DynamoDB across the
per-region ground truth 2.0.0 put in place. New coverage is PartiQL's
RETURNING clause; the GSI lifecycle also joins the ground truth, and the
sweep's drift classifier gains a converged case.

- PartiQL RETURNING (#103, #105): the clause pinned across every PartiQL
  surface. DELETE accepts only `RETURNING ALL OLD *` and rejects the other
  three variants with a 400, the message pinned; UPDATE accepts all four
  ALL/MODIFIED x OLD/NEW forms, where MODIFIED returns just the changed
  attribute and drops the key. The empty-projection edges are pinned too: a
  MODIFIED projection that resolves to nothing - a nested leaf, a list index,
  a batch statement - returns `Items: []` rather than a keyed row.
  BatchExecuteStatement honours RETURNING; ExecuteTransaction rejects it. The
  non-upsert paths round it out: INSERT on an existing item, and UPDATE or
  DELETE behind a false predicate, fail ConditionalCheckFailed and leave the
  item untouched.
- PartiQL RETURNING over list indices (#102): the MODIFIED projection shapes
  for a list-index `SET` or `REMOVE`, pinned against real AWS. The projection
  reads the literal index against the resulting list (`MODIFIED NEW *`) or the
  prior list (`MODIFIED OLD *`): an in-range index returns its element, an
  out-of-range one returns nothing, and multiple changed indices pack into a
  dense list in ascending index order. So a non-zero index collapses to one
  element, an append projects only when the index lands inside the new list,
  and removing the last index yields nothing under NEW while a middle REMOVE
  returns the shifted element. Setting an index on an absent attribute is
  rejected, not auto-created. In BatchExecuteStatement a member that fails to
  parse surfaces per-statement with the short `Code: ValidationError`, the same
  Code as an execution failure, and does not fail the batch.
- GSI lifecycle in the ground truth (#100): the 14 UpdateTable GSI lifecycle
  tests are now observed by the weekly sweep and recorded as per-region ground
  truth. They are the one slice the gating run drops for runtime, so they were
  the one slice without a real-AWS answer; the sweep records them without
  slowing the gate.
- Drift classification (#104): a drift where every region moves off the pinned
  answer at once is now classified as converged rather than moved, with the
  converged path covered end to end.

## 2026-07-17 (2.0.0)

Per-region scoring lands complete. 2.0.0-pre put the scoring logic in place,
comparing each target against every region's recorded answer, but the
evidence half was never wired: no test recorded what a target actually
answered and the classifier never read one, so a fail could not be credited
to a region the target matched and the score could only ever subtract. 2.0.0
closes that loop, and the seed split runs its whole lifecycle in the same
release.

What changed:

- The split tests now record what the target actually answered
  (`src/observation-sink.ts`), in the same shape the registry stores each
  region's answer, and the classifier carries it onto the verdict. An engine
  that matches a rejecting region on a split is now scored as passing in that
  region. Evidence only ever redeems a committed fail: a pass keeps the
  committed assertion's deliberate wording tolerance and is never held to the
  byte-exact recorded string. Committed results predate the capture, so
  published scores move on each target's next run, not in this change.
- Headline ties now prefer a region the registry characterises. A region
  absent from every split row ties the top score by having nothing recorded
  about it, and the Region column must not answer "conformant to what?" with
  a region the suite knows nothing about. Only the label is affected; a
  strictly higher score still wins whatever its source.
- The seeded `{ NULL: false }` split closed its own loop. eu-west-2 and
  eu-central-1 were the last regions accepting it; by the 2026-07-17 sweep
  both reject it again, so every region agrees, the split is retired, and its
  test asserts the shared rejection. The detect, admit and reconcile path the
  pre-release introduced, exercised end to end within days.
- A tooling test now asserts every tracked file is text, after a raw NUL
  byte used as a delimiter in one source file made grep classify the file as
  binary and silently skip it.

## 2026-07-13 (2.0.0-pre)

The scores barely move in this release, but what they mean has changed.

Until now the suite pinned one region, eu-west-2, as ground truth. That was
quietly unfair: real DynamoDB disagrees with itself in a handful of places,
and a one-region baseline takes a side without saying so. The clearest case
is the `{ NULL: false }` attribute value - accepted and normalised to
`{ NULL: true }` in eu-west-2 and eu-central-1, rejected with a
ValidationException in us-east-1 and ap-southeast-2. An engine matching
us-east-1 on that behaviour was marked non-conformant for doing exactly what
real DynamoDB does in Virginia. From 2.0.0, ground truth is per region.

What changed:

- A weekly sweep runs the full suite against real DynamoDB in every
  commercial region and publishes per-region ground truth
  (`.github/workflows/sweep.yml`, `ground-truth/`). It gates nothing: PR
  scoring stays offline and deterministic.
- Confirmed regional splits live in a checked-in registry
  (`registry/splits.json`), each row carrying what every named region
  actually returned, when, and who admitted it. Detection is automatic;
  admission is not. The sweep files an issue with the evidence, and only a
  human commits a row. The registry ships seeded with the `{ NULL: false }`
  split.
- Each target is scored against every observed region's expectations, and
  its published number is its best-matching region - named in the results
  table's new Region column, with the full per-region view in
  `results/summary.json` (a versioned, additive artefact; the per-target
  `results/<slug>.json` files are unchanged in shape).
- A third result state, indeterminate, for a failed observation: a timeout,
  an exhausted throttle, a transport fault. It is excluded from both sides
  of the score and cannot become a split, a registry row, or a fail. An
  absent answer is not a different answer.
- Region health is tracked in `registry/regions.json`. A region that cannot
  complete a sweep is published as unresolved rather than silently omitted;
  two consecutive misses drop it from the scored set and page a maintainer
  in the same act.

One deliberate departure from the RFC that proposed this (#75): the RFC
suggested a behaviour conforms if it matches *any* real region. 2.0.0
scores each target against one region at a time and headlines the best
match, so a target only passes a behaviour when at least one real region
does what it did, and its headline reflects one coherent region rather than
a mix. Match-any scoring would have accepted an engine that combines
eu-west-2's answer on one behaviour with us-east-1's on another - a
deployment that exists nowhere. That is stricter than the RFC asked for,
and it is deliberate.

No score moves at release: the one admitted split pins eu-west-2, which is
the only region in the health record until the first sweep runs. Per-target
deltas will be published once the sweep admits more regions; the expected
movement is roughly a tenth of a percent for the six engines that match
us-east-1 on the `{ NULL: false }` split.

The suite also grew to 954 tests, up 81, all characterised against real
DynamoDB - the control-plane pins in eu-west-2, everything else across four
regions (eu-west-2, eu-central-1, us-east-1, ap-southeast-2):

- UpdateTable AttributeDefinitions reconciliation (#77, #78, #79):
  delta-fed GSI adds merge into the stored union rather than replacing it, a
  conflicting redeclaration of an existing key keeps the stored type, and
  deleting a GSI prunes only its orphaned key attributes. An unused
  definition on an add is silently dropped where CreateTable rejects the
  same shape.
- ProjectionExpression validation (#81): duplicate paths, alias collisions
  and parent/child overlaps rejected identically on GetItem, Query, Scan and
  BatchGetItem, pinned with exact messages; legal shared-prefix projections
  guarded as accepted; GetItem's misfiled validation tests rehomed into
  Tier 3.
- Expression-size limit (#80): every expression parameter caps at 4096
  bytes, measured on the raw string before ExpressionAttributeNames
  substitution - 4096 accepted, 4097 rejected, on all five expression
  surfaces. No tracked target enforces this limit today, so the Tier 3
  movement it causes is new coverage, not a regression.
- Empty set members (#82): empty strings in an SS and zero-length members
  in a BS are accepted and round-trip intact through every write path,
  and contains() can find them; an empty NS member and duplicate empty
  members are rejected, with messages pinned. One existing assertion got
  stricter: the empty-binary round-trip now asserts byte length zero.

## 2026-07-01

Grew to 873 tests, up 49, all characterised against real DynamoDB in eu-west-2.
New coverage in three areas:

- ConsumedCapacity: the transactional read/write split on a same-token replay and
  on ExecuteTransaction, and a correction that single-item operations report the
  aggregate CapacityUnits and omit the split, which is transactional-only.
- Empty-binary key values: rejected as a top-level ValidationException on every
  path, including secondary-index keys and inside transactions, mirroring the
  empty-string rejection.
- Expression, limit and response-shape parity: KeyConditionExpression operand and
  nested-path rules, ExpressionAttributeNames/Values hygiene, projection
  validation and list-index fidelity, reversed-bounds BETWEEN, read-path
  key-length and segment caps, whitespace numbers, batch unprocessed fields and
  cross-table projection mixing, the bare no-op upsert, multi-subpath
  UPDATED_NEW, filter operand ordering, hash-only-GSI pagination, and CreateTable
  spec validation.

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
