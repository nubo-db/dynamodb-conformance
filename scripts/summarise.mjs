#!/usr/bin/env node

/**
 * Post-process Vitest JSON output files into the Markdown comparison table.
 *
 * Usage:
 *   node scripts/summarise.mjs                 # all results/*.json -> stdout
 *   node scripts/summarise.mjs results/*.json  # explicit files     -> stdout
 *   node scripts/summarise.mjs --write         # splice into README.md markers
 *
 * Each JSON file is a Vitest --reporter=json output; the target slug is the
 * filename (e.g. "dynoxide" from "dynoxide.json"). Run date comes from the
 * Vitest run; target version from an optional sibling "<slug>.version" file.
 *
 * The real-DynamoDB row is synthesised, not read from a file: real DynamoDB is
 * the ground truth, so it is 100% by definition across the full suite. This
 * keeps the row present and correct even on runs that don't exercise AWS.
 *
 * The percentage is correctness over IMPLEMENTED operations: passed / (passed +
 * failed). Skips are operations the target does not implement (the feature-probe
 * declined to run them) and are reported in their own column, not counted
 * against the percentage. A skip is honest scope documentation; a fail is a
 * correctness bug. The two are not the same and the table treats them apart.
 */

import { existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { basename, join } from 'node:path'
import { GROUND_TRUTH_SLUG, isPublishedTarget, passRate, scoreResults } from './lib/score.mjs'

const argv = process.argv.slice(2)
const write = argv.includes('--write')
const files = argv.filter((a) => !a.startsWith('--'))

if (files.length === 0) {
  try {
    files.push(
      ...readdirSync('results')
        .filter((f) => f.endsWith('.json'))
        .map((f) => join('results', f)),
    )
  } catch {
    console.error('Usage: node scripts/summarise.mjs [--write] [results/*.json]')
    process.exit(1)
  }
}

if (files.length === 0) {
  console.error('No result files found.')
  process.exit(1)
}

// Display names for the published table. Unlisted slugs fall back to a
// hyphen-stripped form.
const DISPLAY = {
  dynamodb: 'DynamoDB',
  'dynamodb-local': 'DynamoDB Local',
  dynoxide: 'Dynoxide',
  dynalite: 'Dynalite',
  localstack: 'LocalStack',
  ministack: 'Ministack',
  floci: 'Floci',
  extenddb: 'ExtendDB',
}
const display = (slug) => DISPLAY[slug] ?? slug.replace(/-/g, ' ')

// Project home for each target, linked from its name in the table. The two AWS
// targets have no source repo, so they point at their AWS pages.
const REPO = {
  dynamodb: 'https://aws.amazon.com/dynamodb/',
  'dynamodb-local':
    'https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/DynamoDBLocal.html',
  dynoxide: 'https://github.com/nubo-db/dynoxide',
  dynalite: 'https://github.com/architect/dynalite',
  localstack: 'https://github.com/localstack/localstack',
  ministack: 'https://github.com/ministackorg/ministack',
  floci: 'https://github.com/floci-io/floci',
  extenddb: 'https://github.com/ExtendDB/extenddb',
}
const label = (slug) => (REPO[slug] ? `[${display(slug)}](${REPO[slug]})` : display(slug))

const rows = []
let groundTruthDate = '-'

for (const file of files) {
  const slug = basename(file, '.json')
  // Reserved scratch slugs (e.g. a local dev run's results/local.json) are not
  // published targets; skip before reading so they never reach the table.
  if (!isPublishedTarget(slug)) continue
  const raw = JSON.parse(readFileSync(file, 'utf8'))

  const runDate = raw.startTime
    ? new Date(raw.startTime).toISOString().slice(0, 10)
    : '-'

  if (slug === GROUND_TRUTH_SLUG) {
    // Scores are synthesised below, but keep the date of the last successful
    // real-AWS run so the ground-truth row isn't dateless.
    groundTruthDate = runDate
    continue
  }

  const versionFile = file.replace(/\.json$/, '.version')
  const version =
    (existsSync(versionFile) && readFileSync(versionFile, 'utf8').trim()) || '-'

  const scored = scoreResults(raw)
  // Files in results/ that aren't a target's Vitest output (e.g.
  // tag-manifest.json) score nothing; skip them rather than emit an empty row.
  if (!scored) continue

  const { summary, passed: allP, failed: allF, skipped, count } = scored
  // Correctness over implemented operations: skips (operations the target does
  // not implement, where the feature-probe declined to run) are excluded from
  // the denominator. A skip is scope, not a failure.
  const pct = (p, f) => {
    const rate = passRate(p, f)
    return rate === null ? '-' : `${rate.toFixed(1)}%`
  }

  rows.push({
    target: label(slug),
    tier1: pct(summary.tier1.p, summary.tier1.f),
    tier2: pct(summary.tier2.p, summary.tier2.f),
    tier3: pct(summary.tier3.p, summary.tier3.f),
    total: pct(allP, allF),
    passed: allP,
    failed: allF,
    skipped,
    count,
    version,
    runDate,
  })
}

// Suite size: the largest test count seen, i.e. a full-suite run.
const suiteSize = Math.max(0, ...rows.map((r) => r.count))

// Sort emulators by total descending (`-` last), then by name.
const num = (t) => (t === '-' ? -1 : parseFloat(t))
rows.sort((a, b) => num(b.total) - num(a.total) || a.target.localeCompare(b.target))

const groundTruth = {
  target: label(GROUND_TRUTH_SLUG),
  tier1: '100%',
  tier2: '100%',
  tier3: '100%',
  total: '100%',
  passed: suiteSize,
  failed: 0,
  skipped: 0,
  version: 'live (AWS)',
  runDate: groundTruthDate,
}

const ordered = [groundTruth, ...rows]
const fmt = (r) =>
  `| ${r.target} | ${r.tier1} | ${r.tier2} | ${r.tier3} | ${r.total} | ${r.passed} | ${r.failed} | ${r.skipped} | ${r.version} | ${r.runDate} |`

const tableBody = [
  '| Target | Tier 1 | Tier 2 | Tier 3 | Total | Pass | Fail | Skip | Version | Date |',
  '|--------|--------|--------|--------|-------|------|------|------|---------|------|',
  ...ordered.map(fmt),
].join('\n')

// Ground-truth region, stamped into results/dynamodb.region by record-version.sh.
// Shown with the table so a reader sees which region the numbers come from.
const region = existsSync('results/dynamodb.region')
  ? readFileSync('results/dynamodb.region', 'utf8').trim()
  : ''
const caption =
  region && region !== '-'
    ? `_Scored against real DynamoDB in \`${region}\`; behaviour varies by region and over time, so these are point-in-time figures._`
    : ''
const table = caption ? `${caption}\n\n${tableBody}` : tableBody

if (write) {
  const path = 'README.md'
  const start = '<!-- results:start -->'
  const end = '<!-- results:end -->'
  const md = readFileSync(path, 'utf8')
  const s = md.indexOf(start)
  const e = md.indexOf(end)
  if (s === -1 || e === -1) {
    console.error(`Could not find ${start} / ${end} markers in ${path}`)
    process.exit(1)
  }
  const updated = `${md.slice(0, s + start.length)}\n${table}\n${md.slice(e)}`
  writeFileSync(path, updated)
  console.error(`Updated the results table in ${path}.`)
} else {
  console.log(table)
}
