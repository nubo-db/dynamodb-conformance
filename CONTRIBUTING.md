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

## Commit style

Short subject, lower-case, imperative where possible. A Conventional
Commits-style prefix (`feat:`, `fix:`, `docs:`, `refactor:`, `chore:`,
`ci:`) is preferred when one fits but is not a gate. Bodies are
welcome for anything non-obvious.

## AI-assisted contributions

AI tools are welcome. If an AI tool drafted or materially shaped the
change, say so in the PR description. A single line is enough. This
helps calibrate review; it is not a gate.

## Where to ask

GitHub Issues:
<https://github.com/nubo-db/dynamodb-conformance/issues>. Discussions
are not currently enabled.
