#!/usr/bin/env node

/**
 * Format a GitHub issue body from a Vitest JSON report's failed assertions.
 *
 * Usage:
 *   node scripts/report-failure.mjs results/dynamodb.json <run-url> > body.md
 *
 * The scheduled-run workflow calls this on a deterministic ground-truth failure
 * (after retries) and threads the output onto a single deduped issue, so a red
 * Monday is actionable rather than a silent X. The formatting lives in pure
 * functions (collectFailures / buildIssueBody) so they can be unit-tested once a
 * scripts/ tooling-test harness exists. A later cross-region drift signal will
 * fill in the drift-versus-flake verdict at the triage slot.
 */

import { readFileSync } from 'node:fs'

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

/** Build the Markdown issue body. `report` may be null when parsing failed. */
export function buildIssueBody(report, runUrl) {
  const lines = []
  lines.push('The scheduled `Conformance Tests` ground-truth run went red after retries.')
  lines.push('')
  if (runUrl) lines.push(`Run: ${runUrl}`)
  lines.push('')

  if (!report) {
    lines.push('The Vitest report could not be read or parsed, so the failure was')
    lines.push('likely in setup/teardown or the runner itself. See the run log.')
    return lines.join('\n')
  }

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

  lines.push('')
  lines.push('<!-- triage-slot -->')
  lines.push(
    '_Triage: a deterministic red here is either real AWS drift (re-characterise ' +
      'against current AWS) or a flake the retry did not catch. The drift metric ' +
      'will label this automatically once it lands._',
  )
  return lines.join('\n')
}

function main() {
  const [reportPath, runUrl = ''] = process.argv.slice(2)
  if (!reportPath) {
    console.error('usage: report-failure.mjs <vitest-json> <run-url>')
    process.exit(1)
  }
  let report = null
  try {
    report = JSON.parse(readFileSync(reportPath, 'utf8'))
  } catch {
    // Leave report null; buildIssueBody emits the could-not-parse body.
  }
  process.stdout.write(buildIssueBody(report, runUrl) + '\n')
}

if (import.meta.url === `file://${process.argv[1]}`) main()
