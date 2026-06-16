#!/usr/bin/env node

/**
 * Format a GitHub issue body from a Vitest JSON report's failed assertions, and
 * - when a drift diff is supplied - label the failure as confirmed AWS drift or
 * a likely flake.
 *
 * Usage:
 *   node scripts/report-failure.mjs <vitest-json> <run-url> \
 *     [--drift <drift.json>] [--verdict-out <file>]
 *
 * The scheduled-run workflow calls this on a deterministic ground-truth failure
 * (after retries) and threads the output onto a single deduped issue, so a red
 * Monday is actionable rather than a silent X. With --drift (the output of
 * drift-diff.mjs comparing a fresh eu-west-2 capture against the committed
 * baseline) it fills the triage slot with a verdict and writes the recommended
 * issue label to --verdict-out. The pure functions are unit-tested via
 * test:tooling.
 */

import { readFileSync, writeFileSync } from 'node:fs'

/** Pull failed assertions out of a Vitest JSON report. */
export function collectFailures(report) {
  const out = []
  for (const tr of report?.testResults ?? []) {
    for (const ar of tr?.assertionResults ?? []) {
      if (ar?.status !== 'failed') continue
      const name =
        ar.fullName ||
        [...(ar.ancestorTitles ?? []), ar.title].filter(Boolean).join(' > ')
      const detail = (ar.failureMessages ?? [])[0]?.split('\n')[0]?.trim() ?? ''
      out.push({ file: tr.name, name, detail })
    }
  }
  return out
}

/**
 * Turn a drift-diff result (drift-diff.mjs across-time output) into a verdict.
 * Returns null when no usable drift data is available, so the body falls back to
 * the generic triage note.
 */
export function verdictFromDrift(driftResult) {
  if (!driftResult || typeof driftResult.clean !== 'boolean') return null
  // A diff that compared nothing (a missing region block) yields no verdict -
  // fall back to the generic triage note rather than guessing flake or drift.
  if (driftResult.comparable === false) return null
  if (driftResult.clean) {
    return {
      label: 'likely-flake',
      summary:
        "eu-west-2's wording matches the committed baseline, so this is most likely a " +
        'transient flake the retry happened not to catch. Investigate timing rather than ' +
        're-characterising.',
      probes: [],
    }
  }
  const probes = (driftResult.drift?.probes ?? []).map((p) => p.id)
  // A round-trip-only change carries no probe id, so name it explicitly or the
  // issue would claim drift with nothing to act on.
  if (driftResult.drift?.nullRoundTrip) probes.push('{ NULL: false } round-trip')
  return {
    label: 'aws-drift-confirmed',
    summary:
      "eu-west-2's wording has moved from the committed baseline, so this is real AWS drift. " +
      'Re-characterise the affected assertions against current AWS per the suite doctrine.',
    probes,
  }
}

/** Build the Markdown issue body. `report` may be null when parsing failed. */
export function buildIssueBody(report, runUrl, verdict = null) {
  const lines = []
  lines.push('The scheduled `Conformance Tests` ground-truth run went red after retries.')
  lines.push('')
  if (runUrl) lines.push(`Run: ${runUrl}`)
  lines.push('')

  if (!report) {
    lines.push('The Vitest report could not be read or parsed, so the failure was')
    lines.push('likely in setup/teardown or the runner itself. See the run log.')
  } else {
    const failures = collectFailures(report)
    if (failures.length === 0) {
      lines.push('No failed assertions are present in the report, so the failure was')
      lines.push('likely in a `beforeAll`/`afterAll` hook or infrastructure rather than')
      lines.push('a test body. See the run log.')
    } else {
      lines.push(`**${failures.length} failed test${failures.length === 1 ? '' : 's'}:**`)
      lines.push('')
      for (const f of failures) {
        lines.push(`- \`${f.name}\``)
        if (f.detail) lines.push(`  - ${f.detail}`)
      }
    }
  }

  lines.push('')
  lines.push('<!-- triage-slot -->')
  if (verdict) {
    const tag = verdict.label === 'aws-drift-confirmed' ? 'AWS drift confirmed' : 'Likely a flake'
    lines.push(`**Verdict: ${tag}.** ${verdict.summary}`)
    if (verdict.probes.length) {
      lines.push('')
      lines.push('Drifted probes: ' + verdict.probes.map((id) => `\`${id}\``).join(', '))
    }
  } else {
    lines.push(
      '_Triage: a deterministic red here is either real AWS drift (re-characterise ' +
        'against current AWS) or a flake the retry did not catch. No drift verdict was ' +
        'available for this run._',
    )
  }
  return lines.join('\n')
}

function parseArgs(argv) {
  const args = { drift: null, verdictOut: null, _: [] }
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    if (a === '--drift') args.drift = argv[++i]
    else if (a === '--verdict-out') args.verdictOut = argv[++i]
    else args._.push(a)
  }
  return args
}

function readJson(path) {
  try {
    return JSON.parse(readFileSync(path, 'utf8'))
  } catch {
    return null
  }
}

function main() {
  const args = parseArgs(process.argv.slice(2))
  const [reportPath, runUrl = ''] = args._
  if (!reportPath) {
    console.error('usage: report-failure.mjs <vitest-json> <run-url> [--drift <file>] [--verdict-out <file>]')
    process.exit(1)
  }
  const report = readJson(reportPath)
  const verdict = args.drift ? verdictFromDrift(readJson(args.drift)) : null
  process.stdout.write(buildIssueBody(report, runUrl, verdict) + '\n')
  if (args.verdictOut && verdict) writeFileSync(args.verdictOut, verdict.label + '\n')
}

if (import.meta.url === `file://${process.argv[1]}`) main()
