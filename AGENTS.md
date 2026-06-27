# AGENTS.md

Guidance for AI coding tools (Codex, Cursor, Aider, Claude Code, and
others) contributing to this suite. Humans are welcome to read it too;
`CONTRIBUTING.md` covers the same ground in prose.

## What this suite is

An independent conformance test suite for DynamoDB-compatible
endpoints. Tests are first run against real AWS DynamoDB to establish
ground truth, then against any target (DynamoDB Local, Dynoxide,
Dynalite, LocalStack, ExtendDB, or anything else implementing the
DynamoDB HTTP API). A target passes only if it returns the same answer real
DynamoDB does.

## Ground rules for contributions

1. **Real DynamoDB is ground truth.** Run new or modified tests
   against real AWS DynamoDB where you can, and note the result in
   the PR description. If you cannot, flag that in the PR and a
   maintainer will verify against real DynamoDB before merging.
   Either way, if real DynamoDB rejects the test, the test is wrong;
   do not adjust the assertion to match an emulator.
2. **No emulator-specific tests.** Tests must pass on real DynamoDB.
   The suite's value depends on this invariant.
3. **Discuss before coding for anything non-trivial.** Open a GitHub
   issue before a PR that adds a new tier, changes how a tier is
   defined, or touches the results pipeline. Small test additions and
   fixes are fine without a prior issue.

## TypeScript conventions

- Language: TypeScript, ESM (`"type": "module"` in `package.json`).
- Runtime: Node (see `package.json` engines if set) and vitest.
- Tests live under `tests/tier1/`, `tests/tier2/`, `tests/tier3/`.
- No linter or formatter is currently configured, so match the style
  of nearby code.
- Commands contributors will use:
  - `npm install`
  - `npm test` (runs vitest)
  - `npm run test:quick` (faster, skips GSI lifecycle tests)
  - `npm run test:tier1` / `tier2` / `tier3` for a single tier

## Test philosophy

This is what the suite exists for; please read this section before
writing a test.

Tests encode **real AWS DynamoDB behaviour**. They are not a
specification, a wish list, or an agreement between emulators. Every
test is a claim of the form "real DynamoDB does X when given Y", and
the suite's job is to check each implementation against that claim.

Implementations are checked **against real DynamoDB, not against each
other**. Two emulators agreeing on a wrong answer does not move the
baseline; real DynamoDB is the only arbiter. If an emulator author
disagrees with a test's expected value, the resolution is to re-run
it against real DynamoDB and update the baseline, not to negotiate
between emulators.

## What a new test needs to demonstrate

Before opening a PR that adds or modifies a test:

1. **Required: the test passes against real AWS DynamoDB.** This is
   the non-negotiable gate. If real DynamoDB rejects the test, the
   test is wrong; do not adjust the assertion to make an emulator
   pass. Run with an unset `DYNAMODB_ENDPOINT` (or whatever your
   environment configures for real AWS).
2. **Required: the test runs cleanly against at least one emulator
   target.** This proves the test is well-formed and actually
   exercises an emulator rather than just real DynamoDB. Dynoxide is
   the easiest local target (no Docker, no JVM):
   `DYNAMODB_ENDPOINT=http://localhost:8000 npm test`. DynamoDB Local,
   Dynalite, or LocalStack are acceptable alternatives if you have
   them handy.
3. **Optional but welcome: note in the PR description how the test
   behaves across more than one emulator**, for example "passes on
   Dynoxide, fails on DynamoDB Local, matches real DynamoDB". This
   accelerates maintainer review.
4. **Consider the rejection path, not just acceptance.** Where a
   behaviour has inputs real DynamoDB rejects (a malformed expression,
   an out-of-range parameter, a key that doesn't match the schema),
   cover the rejection too, not only the accepted form. A target that is
   too lenient passes acceptance-only tests while still diverging from
   DynamoDB, so acceptance-only coverage of a feature with a validation
   boundary is incomplete. Put the error-code assertion in the
   operation's own tier (or `tests/tier3/validation-ordering/`) and the
   exact message, where it's stable, in `tests/tier3/error-messages/`.

Regenerating the published results table across all tracked targets
(DynamoDB, Dynoxide, DynamoDB Local, Dynalite, LocalStack, ExtendDB) is a
maintainer task, not a contributor requirement. Do not hold a PR for
it.

If a test is flaky against real DynamoDB (for example GSI
propagation), use the existing wait/retry helpers rather than adding
sleeps.

## Commit style

Short subject, lower-case, imperative where possible. A Conventional
Commits-style prefix (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`,
`ci:`) is preferred when one fits but is not a gate. Bodies are
welcome for anything non-obvious.

## Where to discuss

- GitHub Issues:
  <https://github.com/paritysuite/dynamodb-conformance/issues>

Discussions are not currently enabled.
