#!/usr/bin/env node

/**
 * Post-process Vitest JSON output files into a Markdown comparison table.
 *
 * Usage:
 *   node scripts/summarize.mjs results/dynamodb.json results/dynoxide.json ...
 *   node scripts/summarize.mjs results/*.json
 *
 * Each JSON file should be a Vitest --reporter=json output.
 * The target name is derived from the filename (e.g. "dynoxide" from "dynoxide.json").
 */

import { readFileSync, readdirSync } from 'node:fs'
import { basename, join } from 'node:path'

const files = process.argv.slice(2)
if (files.length === 0) {
  // Default: read all JSON files from results/
  try {
    const dir = 'results'
    const entries = readdirSync(dir).filter(f => f.endsWith('.json'))
    files.push(...entries.map(f => join(dir, f)))
  } catch {
    console.error('Usage: node scripts/summarize.mjs [results/*.json]')
    process.exit(1)
  }
}

if (files.length === 0) {
  console.error('No result files found.')
  process.exit(1)
}

const rows = []

for (const file of files) {
  const target = basename(file, '.json').replace(/-/g, ' ')
  const raw = JSON.parse(readFileSync(file, 'utf8'))

  const tests = raw.testResults?.flatMap(tr =>
    tr.assertionResults?.map(ar => ({
      file: tr.name,
      status: ar.status, // 'passed' | 'failed' | 'pending'
      fullName: ar.fullName,
    })) ?? []
  ) ?? []

  const tier = (filePath) => {
    if (filePath.includes('/tier1/')) return 'tier1'
    if (filePath.includes('/tier2/')) return 'tier2'
    if (filePath.includes('/tier3/')) return 'tier3'
    return 'other'
  }

  const summary = { tier1: { p: 0, f: 0, s: 0 }, tier2: { p: 0, f: 0, s: 0 }, tier3: { p: 0, f: 0, s: 0 } }

  for (const t of tests) {
    const tierKey = tier(t.file)
    if (!(tierKey in summary)) continue
    if (t.status === 'passed') summary[tierKey].p++
    else if (t.status === 'failed') summary[tierKey].f++
    else summary[tierKey].s++
  }

  const pct = (p, total) => total === 0 ? '-' : `${((p / total) * 100).toFixed(1)}%`
  const total = (s) => s.p + s.f + s.s
  const allP = summary.tier1.p + summary.tier2.p + summary.tier3.p
  const allTotal = total(summary.tier1) + total(summary.tier2) + total(summary.tier3)

  rows.push({
    target,
    tier1: pct(summary.tier1.p, total(summary.tier1)),
    tier2: pct(summary.tier2.p, total(summary.tier2)),
    tier3: pct(summary.tier3.p, total(summary.tier3)),
    total: pct(allP, allTotal),
    passed: allP,
    failed: summary.tier1.f + summary.tier2.f + summary.tier3.f,
    skipped: summary.tier1.s + summary.tier2.s + summary.tier3.s,
    count: allTotal,
  })
}

// Print markdown table
console.log('| Target | Tier 1 | Tier 2 | Tier 3 | Total | Pass | Fail | Skip |')
console.log('|--------|--------|--------|--------|-------|------|------|------|')
for (const r of rows) {
  console.log(`| ${r.target} | ${r.tier1} | ${r.tier2} | ${r.tier3} | ${r.total} | ${r.passed} | ${r.failed} | ${r.skipped} |`)
}
