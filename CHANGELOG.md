# Conformance suite history

A dated log of how the conformance test suite has grown: tests added, tiers
broadened, and targets brought into the run. Newest first.

## 2026-07-30 (3.1.0)

Every target now wears a letter grade, read from the two published figures and
never blended from them. Divergence sets the letter - A+ for exactly zero
failing tests against the target's best-matching region, A under 5%, B under
15%, C under 25%, D under 35%, F beyond -
and low coverage can only cap it: under 90% caps at B, under 70% at C, under
50% at D, and coverage alone never grades F. No measurement changed and
no row moved; the grade restates the colour bands the board has always
published, with the criteria versioned (v1) and dated in the methodology so a
future retuning cannot move grades silently.

- Dynoxide reads `A+` where it read `0.0% diverges, 98.6% covered` - both
  figures stay printed beside the letter. Its WebAssembly build reads `B` on
  the same zero divergence, capped by its 78.7% coverage: the cap applies to
  the board author's own engine like any other.
- The per-target badges show the grade under a `parity` label. They had still
  been publishing the correctness percentage the board retired, under a
  `conformance` label, so a badge could disagree with the table it cited.
- The README table gains a Grade column, and the data endpoints (schema 4)
  carry each target's grade plus the full criteria in `metrics.grade`.

The board publishes two figures now instead of one, and the index exclusions
started meaning what they said. No target was re-run for this and no
pass, fail or skip changed: what moved is how the same counts are expressed, so
a figure that looks different here is the same measurement described more
honestly. Relative order is the one exception. Divergence puts skips in the
denominator where correctness left them out, which is enough to swap two
adjacent rows: Ministack has no skips and DynamoDB Local has 21, so DynamoDB
Local now sits above Ministack having sat below it, on identical pass and fail
counts. No other pair moves.

**A score is now divergence and coverage, never one number.** Divergence is
the share of the whole suite a target answers differently from real DynamoDB.
Coverage is the share it implements at all. They are reported apart because a
skip and a fail are different problems: an operation a target declines is
something you find in minutes and plan around, and one it gets quietly wrong is
something you find in production. Summing them would price them the same.

- The standings are ordered by divergence. That ranks how much a target gets
  wrong; it is not a verdict on which emulator to pick, which depends on the
  operations you need. A target with no divergences over a narrow surface sits
  high, and its coverage figure says how narrow.
- Dynoxide reads `0.0% diverges, 98.6% covered` where it used to read `100%`.
  Ministack reads `16.1% diverges, 100.0% covered` where it used to read
  `83.9%`. Neither engine changed.
- The Region column is gone. It named the cohort a target matched at its best
  rate, which read as breadth: a target equally wrong in all 33 regions showed
  `all regions` while one perfect in 6 showed `6 regions`, even though the
  second diverges less in its worst region than the first does in its best.
  Regional disagreement moves a figure by at most 0.3 points, across three
  tests of about a thousand, so the detail sits in `results/summary.json` and
  on each target's page instead.
- Movement follows divergence, where lower is better. The states are
  `improved` and `regressed` rather than `up` and `down`, because a rise is now
  the bad direction and the old names would have coloured a regression green.
  **Anything reading `movement.state` from the JSON needs updating.**
- `latest.json` and `runs.json` gain `divergence`, and `project`,
  `configuration` and `isVariant` so a consumer can tell a build of an engine
  from a rival without parsing display names. The raw counts stay beside every
  figure, so anyone preferring their own arithmetic has it.
- A build of an engine nests under it rather than taking a row beside it. You
  choose between projects; which build you want follows from where your code
  runs. Dynoxide (wasm) keeps its own figures, its own page and its own link.
- Each target lists how it is actually distributed - Docker, npx, cargo, a JAR
  and so on - with the project's own page for each. These are the only claims
  on the board the suite does not measure, so each one carries the link that
  backs it.

**Everything else that was a percentage follows the headline down.** The
headline changed first and the rest of the page did not, so a target's page ran
in two directions at once: a falling divergence figure above a rising
correctness chart, with nothing saying they were different measurements.

- Tier figures are divergence within each tier - that tier's fails over its
  whole size - with coverage beside them. Dynalite's Tier 2 reads `14.1%
  diverges, 20.1% covered` where it read `30.0%`; the engine is unchanged, and
  the second pair says the thing the single figure hid, which is that Dynalite
  attempts a fifth of Tier 2. Correctness is still there under its own name.
- The colour bands inverted with the figures. A tier bar was green at 95% and
  above; left alone, a target diverging on 2% of a tier would have rendered
  red. Bar length is that tier's coverage and its colour is that tier's
  divergence, the same encoding the standings row already used.
- A target's history is two plots now, divergence and coverage, where it was
  one chart headed "Conformance over time". Both are needed because divergence
  falls when a target stops attempting an operation it used to get wrong, so a
  divergence line alone can show an improvement that is really a withdrawal.
  Dynalite is the live case: 88 failing tests became skips on 24 July, dropping
  its divergence 8.8 points and its coverage 8.8 points in the same run, and the
  single plot rendered that as the engine getting markedly better. This is the same reason the board
  publishes two figures instead of summing them, applied to the time axis.
- Each plot keeps its own orientation, so a regression always moves away from
  the pinned edge: divergence pins zero to the bottom axis and rises for a
  regression, coverage pins 100 to the top and falls for one. Neither is
  squashed onto a shared axis with the other, and each names its own sense
  down the side of the plot, because a reader can't be expected to assume which
  way is good for either. The caption reads off the worst run for that metric -
  the highest divergence, the lowest coverage - which stopped being the same
  run once the headline changed.
- The old heading was wrong as well as incomplete. A line sitting at 0% under
  "Conformance over time" invites reading as no conformance at all, when 0%
  divergence is the best result there is.
- The per-region drilldown is divergence too, ordered best first. Best first
  means lowest first now, so a descending sort would have put a target's worst
  regions at the top under a heading that reads as its best.
- Each tier states its coverage as a figure, not only as the length of its bar.
  Divergence is paired with a number everywhere else it is published, and a
  tier whose coverage lived solely in a bar was the one place a thin surface
  stopped being stated: Dynalite's Tier 2 diverges on 14.1%, which reads
  unremarkable until you know it attempts a fifth of the tier.
- The per-operation table on a target page is divergence and coverage too, with
  the counts as fails over each operation's whole size. It was a pass rate over
  what the operation attempted, which left the most detailed table on the page
  running opposite to every figure above it.
- **The data schema is at 3, and three changes in it are breaking.** A tier no
  longer carries `pct` and `value`; it carries `divergence`, `coverage` and
  `correctness`, each with a `pct` and a `value` of its own. The whole-suite
  correctness percentage is `correctness`, not `total`: `total` also names the
  raw test count inside `counts`, so the same word meant a count in one place
  and a percentage in another. Anything reading a tier's `pct`, or `total` as a
  percentage, has to be updated - read as-is, both now invert or vanish. The
  third is `movement.state`, whose values are named above; it is listed here too
  so a consumer counting the breaks against the version gets all of them in one
  place.

**A headline says how many regions it came from.** Each target is scored
against every observed region and headlines its best-matching cohort, and the
size of that cohort only appeared on the target's own page. A figure earned
across six regions and one earned across all thirty-three rendered identically.

- The board and the targets index show the count beside the figure - `6 of 33
  regions` - for the same reason coverage sits beside divergence: the number
  that qualifies a headline belongs with it, not a click away. The full cohort
  listing stays on the target page, where a reader has asked for that detail.

**A methodology claim was wrong, and is corrected.** The page said that
measuring divergence over the whole suite stops an emulator implementing a
sliver from posting a perfect score on a thin surface. It doesn't: zero fails
is 0.0% at any coverage, and the Dynoxide WebAssembly build is a live
counterexample at 0.0% over 78.7%. A second formulation - that whole-suite
measurement stops a target lowering its divergence by attempting less - was
false too, so the page now states the identity that does hold rather than
claiming an immunity the arithmetic never gave. A test going from failing to
skipped leaves the divergence numerator and the coverage numerator together over
the same unchanged denominator, so both figures fall by exactly the same amount.
Withdrawal is disclosed rather than silent: it costs precisely as much coverage
as it gains divergence. Dynalite is the live case. On 24 July, 88 of its failing
tests became skips, nothing was fixed, and its divergence and coverage each fell
8.8 points - both of them exactly 88/998. The correctness figure this replaced
rose 8.4 points on that same act, because those 88 tests left its denominator as
well as its numerator and no second figure remained to record it.

**Every figure on a row now comes from one region.** A target's headline is its
best-matching region, but only the headline was taken from that region: the tier
split and the raw counts stayed on the baseline region's basis. Under correctness
that never had to reconcile, because each tier had its own denominator.
Divergence is additive, so it does: the tiers stopped decomposing the headline,
`failed / total` from a row's own published counts stopped equalling its
published divergence, and this table disagreed with the board about three
targets' Tier 3 figures. The whole region entry is applied now, and the region is
named on the row and in the JSON.

- The overlay is also matched to the run it describes. It was keyed on the date
  real AWS was last swept, while each target entry inside carries its own run
  date, so a summary committed after a later run put that later run's figures on
  an earlier run's page. Four rows on the 22 July run published a divergence from
  the 24 July run: Dynalite read 12.3% beside its own count of 213 fails in 998.
  Those four rows now read what that run measured - Dynalite 21.3%, ExtendDB
  15.3%, Dynoxide 5.2%, DynamoDB Local 16.8% - and 22 other rows' fail counts
  move by one or two, to the headline region's count. No target was re-run for
  any of it.
- Coverage is unaffected, and can't be affected: scoring a target against a
  region only ever turns a pass into a fail or back, so what a target implements
  is the same in every region. Divergence is the figure a region decides, which
  is why it is the one the headline region governs.

**The index exclusions create no indexed table (#116).** `!gsi and !lsi` used
to select the right tests and then build the tables anyway, because
provisioning ran over a fixed list before any filter applied. An engine with no
secondary-index support died in setup whatever it had asked for.

- Shared tables are created on demand: a test file declares what it needs, and
  setup creates only what the running file declared. A tag filter skips tests
  but still imports the file, so scoping declarations to the file is what makes
  the exclusion real.
- The shared composite table split. The plain name carries no index; a second
  variant carries the same LSIs and GSIs under their existing names. Tests
  depending on an index moved into files of their own, so a file's declaration
  says whether it needs one.
- The `gsi` and `lsi` tags now mark index dependency wherever it occurs,
  including the tests that never name an index and are only rejected because
  the attribute they write is an index key. Both columns grow. No score moves;
  tags never fed it.
- Three guards: a file must declare the shared tables it uses, a test writing
  an index-key attribute must carry an index tag, and a file using the
  index-bearing table must carry both.

## 2026-07-21 (2.1.0)

Grew to 982 tests, up 28, all characterised against real DynamoDB across the
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
