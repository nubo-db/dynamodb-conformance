# Security policy

## Supported versions

This is a test suite, not a deployed service or a published package. Fixes land
on `main` and there are no maintained release branches, so run against the
current `main`.

## Reporting a vulnerability

Report anything security-sensitive privately, not in a public issue. Use
GitHub's private vulnerability reporting:
<https://github.com/paritysuite/dynamodb-conformance/security/advisories/new>.

I aim to acknowledge a report within a few days and will keep you posted on a
fix or mitigation from there.

## What's actually worth reporting

The suite has a small attack surface. It runs tests, it doesn't serve traffic.
The one thing genuinely worth a private report is a **leaked credential**: a
real `AWS_ACCESS_KEY_ID` or `AWS_SECRET_ACCESS_KEY`, or an account ID or ARN
that shouldn't be public, committed anywhere in the repo - a `.env`, a results
JSON, or a capture file.

A few guards already exist for this:

- `.env.example` only ever holds placeholders. Real credentials go in `.env`,
  which is gitignored.
- `scripts/sanitize-arns.mjs` strips account IDs from results in CI, and
  `scripts/check-account-ids.mjs` checks for any that slipped through.
- The contributor docs tell you to grep a results file for your key before
  committing it.

If you spot a credential that has got past all of that, use the advisory link
above rather than opening an issue, so it can be rotated before it's widely
seen.
