// Shared tier scoring for the results table and the per-target badges, so the
// badge percentage can never drift from the published table.
//
// The percentage is correctness over implemented operations: passed /
// (passed + failed). Skips are operations the target does not implement (the
// feature-probe declined to run them) and are excluded from the denominator.

// The conformance ground truth. Real DynamoDB defines correctness, so its row
// is pinned to 100% rather than scored from a results file. Shared by the
// results table and the badges so the two can't disagree on which slug it is.
export const GROUND_TRUTH_SLUG = 'dynamodb'

// Result-file slugs that are never a published target. `local` is the default
// output of an ad-hoc local run (DYNAMODB_ENDPOINT set with no
// CONFORMANCE_TARGET - see vitest.config.ts), a scratch file that must not be
// scored, badged, or listed in the results table. Kept here so the table and
// the badges agree on what to skip, the same way they share GROUND_TRUTH_SLUG.
export const RESERVED_SLUGS = new Set(['local'])

// Whether a result-file slug is a published conformance target. False for the
// reserved scratch slugs above.
export function isPublishedTarget(slug) {
  return !RESERVED_SLUGS.has(slug)
}

export function tierOf(filePath) {
  if (filePath.includes('/tier1/')) return 'tier1'
  if (filePath.includes('/tier2/')) return 'tier2'
  if (filePath.includes('/tier3/')) return 'tier3'
  return 'other'
}

// Score a Vitest JSON result into per-tier and overall pass/fail/skip counts.
// Returns null only for a file that is not a target's Vitest output at all (no
// testResults array, e.g. results/tag-manifest.json), so callers can skip it.
// A real result file with no scored tests returns zeroed counts rather than
// null, so a genuinely empty run still renders as "-" instead of vanishing.
export function scoreResults(raw) {
  if (!Array.isArray(raw?.testResults)) return null

  const tests = raw.testResults.flatMap(
    (tr) => tr.assertionResults?.map((ar) => ({ file: tr.name, status: ar.status })) ?? [],
  )

  const summary = {
    tier1: { p: 0, f: 0, s: 0 },
    tier2: { p: 0, f: 0, s: 0 },
    tier3: { p: 0, f: 0, s: 0 },
  }
  for (const t of tests) {
    const key = tierOf(t.file)
    if (!(key in summary)) continue
    if (t.status === 'passed') summary[key].p++
    else if (t.status === 'failed') summary[key].f++
    else summary[key].s++
  }

  const passed = summary.tier1.p + summary.tier2.p + summary.tier3.p
  const failed = summary.tier1.f + summary.tier2.f + summary.tier3.f
  const skipped = summary.tier1.s + summary.tier2.s + summary.tier3.s
  return { summary, passed, failed, skipped, count: passed + failed + skipped }
}

// Correctness over implemented operations: passed / (passed + failed), as a
// percentage. Null when nothing ran, so callers render "-".
export function passRate(passed, failed) {
  return passed + failed === 0 ? null : (passed / (passed + failed)) * 100
}
