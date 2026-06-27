# Contributing to the DynamoDB Conformance Suite

Thanks for considering a contribution. This suite's value depends on
one invariant: every test passes against real AWS DynamoDB. Keep that
in mind and most of what follows is routine.

If you are using an AI coding tool (Codex, Cursor, Aider, Claude
Code, or similar), please also read [AGENTS.md](AGENTS.md); it covers
the same ground in a form those tools pick up automatically.

## Before you start

- Open a GitHub issue describing the change if it is more than a
  small test addition or fix. A short paragraph is enough.
- Adding new tiers, changing how a tier is defined, or touching the
  results pipeline needs discussion first.

## Test philosophy

Tests encode real AWS DynamoDB behaviour. Implementations are checked
against real DynamoDB, not against each other; two emulators agreeing
on a wrong answer does not move the baseline. Run new or modified
tests against real AWS DynamoDB where you can. If you cannot run
against real AWS, flag that in the PR and a maintainer will verify
before merging. If real DynamoDB rejects a test, the test is wrong;
do not adjust the assertion to make an emulator pass. The suite
exists precisely so emulator authors cannot mark their own homework.
See `AGENTS.md` for the fuller version.

## What a new test needs to demonstrate

Before opening a PR that adds or modifies a test:

1. **Required:** the test passes against real AWS DynamoDB.
2. **Required:** the test runs cleanly against at least one emulator
   target. Dynoxide is the easiest local target (no Docker, no JVM);
   DynamoDB Local, Dynalite, and LocalStack are fine alternatives if
   you have them to hand.
3. **Optional but helpful:** note in the PR description how the test
   behaves across more than one emulator.
4. **Consider the rejection path, not just acceptance.** If the
   behaviour has inputs real DynamoDB rejects (a malformed expression,
   an out-of-range parameter, a key that doesn't match the schema),
   cover the rejection too, not only the accepted form. A target that
   is too lenient passes acceptance-only tests while still diverging
   from DynamoDB. Put the error-code check in the operation's own tier
   (or `tests/tier3/validation-ordering/`) and the exact message, where
   it's stable, in `tests/tier3/error-messages/`.

Regenerating the published results table across every tracked target
is a maintainer task and does not block your PR.

## Local setup

- Node + npm. No global tooling needed.
- `npm install` to install dependencies.
- `npm test` runs the full suite against whatever endpoint
  `DYNAMODB_ENDPOINT` points to. See the `README.md` for the usual
  patterns.

## Tests

- Tests live under `tests/tier1/`, `tests/tier2/`, `tests/tier3/`,
  grouped by the definition of each tier in the `README.md`.
- Prefer the existing wait/retry helpers over `setTimeout` sleeps.

### Tag your test

Every top-level `describe` takes a `tags` array as its second argument: the
operation it covers, exactly one plane (`data-plane` or `control-plane`), and
any cross-cutting tag that applies (`cloud-only`, `gsi`, `lsi`, `legacy`,
`slow`).

```ts
describe('Query — GSI', { tags: ['query', 'data-plane', 'gsi'] }, () => {
  // ...
})
```

The vocabulary lives in `src/tags.ts` - add a tag there before using it, or
`strictTags` rejects the run. The coverage guard (`npm run test:tooling`) fails
if a top-level describe is left untagged, so feature filters like
`--tags-filter='!partiql'` never silently miss a test. See "Filtering by
feature" in the `README.md` for the full vocabulary.

### Tier 3 sub-directory choice

If a new Tier 3 test cares about the exact error message string, put
it in `tests/tier3/error-messages/`. If it only needs to confirm
which validation fired or which error code came back, put it in
`tests/tier3/validation-ordering/`. Limit and shape errors go in
`tests/tier3/limits/`. Legacy API request shapes go in
`tests/tier3/legacy-api/`.

`error-messages/` uses inline `try/catch` with
`expect(err).toBeInstanceOf(...)`, an exact `expect(err.name).toBe(...)`,
and a message assertion whose strictness fits the message:

- **Exact** (`expect(err.message).toBe(...)`) when the whole string is
  stable across regions and over time. Most messages are, so this stays
  the default.
- **Structural** (`expect(err.message).toContain(...)` on the contractual
  core - the field and the constraint phrase) when AWS's rendering is
  non-deterministic. The `N validation error detected:` envelope, the
  echoed input value, and field-name casing all vary by region (see the
  2026-06 four-region capture) and are not part of the contract the API
  model defines, so don't pin them. Pin what is invariant across regions
  and float the rest. This is the same idea as `createTable.test.ts`'s
  backend-variant handling.

Don't use the `expectDynamoError` helper in `error-messages/` - it always
routes the message through `toContain`, so it can't express the exact
rung; use it (or a direct `toContain`) in `validation-ordering/`.

For error messages with a stable prefix and a variable reason or
identifier suffix (`TransactionCanceledException` is the obvious
case), build the expected message from a known reasons array and
structurally cross-check `CancellationReasons[].Code` against the
same array. See `tests/tier3/error-messages/conditionalCheck.test.ts`
for the pattern.

## Commit style

Short subject, lower-case, imperative where possible. A Conventional
Commits-style prefix (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`,
`ci:`) is preferred when one fits but is not a gate. Bodies are
welcome for anything non-obvious.

## Licensing

The conformance suite is licensed under the Apache License 2.0. By submitting a contribution, you agree that your contribution is licensed under the same terms.

## Where to ask

GitHub Issues:
<https://github.com/paritysuite/dynamodb-conformance/issues>. Discussions
are not currently enabled.
