#!/usr/bin/env node

/**
 * Emit a shields.io endpoint badge per target into results/<slug>.badge.json.
 *
 * A target's own README can then show a live conformance percentage sourced
 * from this repo via a shields endpoint badge, without copying the number into
 * its docs (where it goes stale). Regenerated alongside the results table on
 * every conformance run, so the badge always tracks the latest figures.
 *
 * The percentage matches the published results table exactly: both use the
 * shared scorer in lib/score.mjs (passed / (passed + failed), skips excluded).
 *
 * Run: `npm run results:badges` (regenerates the committed badges). The badge
 * freshness test fails if a committed file drifts from a fresh build.
 */

import { readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { basename, join } from 'node:path'
import { GROUND_TRUTH_SLUG, isPublishedTarget, passRate, scoreResults } from './lib/score.mjs'

const RESULTS_DIR = 'results'

// shields.io named colours, brightest for a clean sweep down to red.
export function colour(pct) {
  if (pct >= 99) return 'brightgreen'
  if (pct >= 95) return 'green'
  if (pct >= 90) return 'yellowgreen'
  if (pct >= 75) return 'yellow'
  if (pct >= 50) return 'orange'
  return 'red'
}

// A target's conformance pass rate, or null when there is nothing to show: the
// slug is a reserved scratch slug (e.g. local), the file is not a target result
// (e.g. tag-manifest.json), or the target ran no scored tests. Real DynamoDB is
// the ground truth, pinned to 100%.
export function rateFor(slug, raw) {
  if (!isPublishedTarget(slug)) return null
  if (slug === GROUND_TRUTH_SLUG) return 100
  const scored = scoreResults(raw)
  return scored ? passRate(scored.passed, scored.failed) : null
}

// Build the shields.io endpoint badge object for a target, or null when there
// is nothing to show. Pure (no I/O) so it backs both the CLI writer and the
// freshness test. The colour keys off the displayed (rounded) percentage, so a
// badge reading "99.0%" can't show the sub-99 colour.
export function buildBadge(slug, raw) {
  const rate = rateFor(slug, raw)
  if (rate === null) return null
  const display = rate.toFixed(1)
  return {
    schemaVersion: 1,
    label: 'conformance',
    message: `${display}%`,
    color: colour(Number(display)),
  }
}

// Write results/<slug>.badge.json for every target result file; returns the
// number of badges written.
export function writeBadges(resultsDir = RESULTS_DIR) {
  const files = readdirSync(resultsDir).filter(
    (f) => f.endsWith('.json') && !f.endsWith('.badge.json'),
  )
  let written = 0
  for (const file of files) {
    const slug = basename(file, '.json')
    const raw = JSON.parse(readFileSync(join(resultsDir, file), 'utf8'))
    const badge = buildBadge(slug, raw)
    if (!badge) continue
    writeFileSync(
      join(resultsDir, `${slug}.badge.json`),
      `${JSON.stringify(badge, null, 2)}\n`,
    )
    written++
  }
  return written
}

// CLI: regenerate the committed badges.
if (import.meta.url === `file://${process.argv[1]}`) {
  const written = writeBadges()
  console.error(`wrote ${written} badge file(s) to ${RESULTS_DIR}/`)
}
