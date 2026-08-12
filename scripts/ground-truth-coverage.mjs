#!/usr/bin/env node

/**
 * Reconcile what real DynamoDB was actually observed on against what the
 * published table claims for it.
 *
 * The ground-truth row is synthesised, not scored: scripts/summarise.mjs pins
 * it to 100% across the full suite size, because real DynamoDB defines
 * correctness and cannot disagree with itself. That is the design, and this
 * script does not touch it. What the design assumes is that every test in the
 * suite has in fact been run against real AWS somewhere - otherwise the row
 * spans tests nobody ever observed, and the assumption quietly becomes a claim.
 *
 * Real AWS coverage is split across three lanes, for runtime reasons rather
 * than conceptual ones:
 *
 *   - the gating job          (`test:gating`)      -> most of the suite
 *   - the integrations lane   (`test:integrations`) -> S3 export/import, Kinesis
 *   - the GSI lifecycle lane  (`test:gsi`)          -> UpdateTable GSI backfills
 *
 * Union those and you should have the whole suite. This script checks that,
 * against registry/suite-manifest.json. Anything the suite defines with no
 * real-AWS observation behind it is reported, and the exit code is non-zero.
 *
 * Usage:
 *   node scripts/ground-truth-coverage.mjs \
 *     results/dynamodb.json ground-truth/gsi.json integration-results/dynamodb.json
 *
 * The population comes from the manifest because that is what the published
 * row's denominator is. It used to come from whichever emulator run was
 * widest, mirroring a `Math.max` in summarise.mjs that no longer exists - so
 * this gate and the publish guard could disagree about what the suite is, and
 * the one that reports per-test detail was the one that could go quiet.
 *
 * Verdicts are irrelevant here: a test that ran and failed was still observed.
 * The question is only whether real AWS was ever asked.
 */

import { readFileSync } from 'node:fs'
import { testIdentities } from './lib/identity.mjs'
import { suiteIdentities } from './suite-manifest.mjs'

/**
 * Tests the suite defines but no ground-truth run observed.
 * Sorted so the report is stable and diffable.
 */
export function uncovered(suite, groundTruthDocs) {
  const observed = new Set()
  for (const doc of groundTruthDocs) {
    for (const id of testIdentities(doc)) observed.add(id)
  }
  return [...suite].filter((id) => !observed.has(id)).sort()
}

export function parseArgs(argv) {
  return { files: argv.filter((a) => a !== '') }
}

function main(argv, suite = suiteIdentities()) {
  const { files } = parseArgs(argv)
  if (files.length === 0) {
    console.error('usage: ground-truth-coverage.mjs <ground-truth.json>...')
    return 2
  }

  const read = (f) => JSON.parse(readFileSync(f, 'utf8'))
  const gaps = uncovered(suite, files.map(read))
  const total = suite.size

  if (gaps.length === 0) {
    console.log(
      `Real AWS was observed on all ${total} tests, across ${files.length} run(s). ` +
        `The synthesised ground-truth row spans nothing unobserved.`,
    )
    return 0
  }

  console.error(
    `${gaps.length} of ${total} tests have no real-AWS observation behind them.\n` +
      `The published ground-truth row spans the full suite, so these are ` +
      `currently claimed rather than evidenced:\n`,
  )
  for (const id of gaps) console.error(`  ${id}`)
  console.error(
    `\nEither a lane did not run (check the run's artefacts) or a test file is ` +
      `excluded from all three. See ground-truth/README.md.`,
  )
  return 1
}

if (import.meta.url === `file://${process.argv[1]}`) {
  process.exit(main(process.argv.slice(2)))
}
