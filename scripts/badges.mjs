#!/usr/bin/env node

/**
 * Emit a shields.io endpoint badge per target into results/<slug>.badge.json.
 *
 * A target's own README can then show its live grade sourced from this repo
 * via a shields endpoint badge, without copying it into its docs (where it
 * goes stale). Regenerated alongside the results table on every conformance
 * run, so the badge always tracks the latest figures.
 *
 * The badge shows the letter grade, and it matches the published results
 * table exactly: both read the target's headline - its best-matching observed
 * region - from the shared scorer (scoreTarget in lib/score.mjs), take the
 * two axes from the shared axesOf, and grade them with the shared gradeOf.
 * Real DynamoDB is the ground truth: each real region agrees with its own
 * recorded behaviour by construction, so its badge is A+ without scoring a
 * file. The badge carried the retired correctness percentage until the board
 * moved to divergence and coverage; a letter cannot be mistaken for either
 * axis, where a bare percentage read as whichever one the reader expected.
 *
 * Run: `npm run results:badges` (regenerates the committed badges). The badge
 * freshness test fails if a committed file drifts from a fresh build.
 */

import { existsSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { basename, join } from 'node:path'
import {
  GROUND_TRUTH_SLUG,
  axesOf,
  isPublishedTarget,
  loadScoringContext,
  scoreTarget,
} from './lib/score.mjs'
import { gradeOf } from './lib/grade.mjs'

const RESULTS_DIR = 'results'

// shields.io named colours per letter, brightest at the top of the scale. The
// steps track the grade bands, which restate the board's published colour
// boundaries, so a badge and the site row it mirrors read the same way.
const COLOURS = {
  'A+': 'brightgreen',
  A: 'green',
  B: 'yellow',
  C: 'orange',
  D: 'red',
  F: 'red',
}

export function colour(letter) {
  return COLOURS[letter] ?? 'lightgrey'
}

// A target's grade, or null when there is nothing to show: the slug is a
// reserved scratch slug (e.g. local), the file is not a target result (e.g.
// tag-manifest.json), or the target ran no scored tests. `context` carries
// the registry and observed region set (loadScoringContext) plus the run's
// indeterminate sidecar, when it wrote one.
export function gradeFor(slug, raw, context) {
  if (!isPublishedTarget(slug)) return null
  if (slug === GROUND_TRUTH_SLUG) return gradeOf(0, 100)
  const scored = scoreTarget(raw, context.sidecar ?? null, context)
  if (!scored) return null
  const { divergence, coverage } = axesOf(scored.regions[scored.headline.region])
  const grade = gradeOf(divergence, coverage)
  return grade.letter === null ? null : grade
}

// Build the shields.io endpoint badge object for a target, or null when there
// is nothing to show. Pure (no I/O) so it backs both the CLI writer and the
// freshness test.
export function buildBadge(slug, raw, context) {
  const grade = gradeFor(slug, raw, context)
  if (grade === null) return null
  return {
    schemaVersion: 1,
    label: 'parity',
    message: grade.letter,
    color: colour(grade.letter),
  }
}

// Write results/<slug>.badge.json for every target result file; returns the
// number of badges written. Sidecar and badge files are companions of a
// target's results file, not targets, so they are never scored themselves.
export function writeBadges(resultsDir = RESULTS_DIR, context = loadScoringContext()) {
  const files = readdirSync(resultsDir).filter(
    (f) =>
      f.endsWith('.json') &&
      !f.endsWith('.badge.json') &&
      !f.endsWith('.indeterminate.json'),
  )
  let written = 0
  for (const file of files) {
    const slug = basename(file, '.json')
    const raw = JSON.parse(readFileSync(join(resultsDir, file), 'utf8'))
    const sidecarFile = join(resultsDir, `${slug}.indeterminate.json`)
    const sidecar = existsSync(sidecarFile)
      ? JSON.parse(readFileSync(sidecarFile, 'utf8'))
      : null
    const badge = buildBadge(slug, raw, { ...context, sidecar })
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
