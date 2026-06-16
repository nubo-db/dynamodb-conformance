#!/usr/bin/env node

/**
 * CLI over scripts/lib/drift.mjs - report raw-message drift between captures.
 *
 *   # cross-region: every region vs the document's own baseline region
 *   node scripts/drift-diff.mjs capture.json
 *
 *   # across-time: a fresh capture's region vs a committed baseline's region
 *   node scripts/drift-diff.mjs fresh.json --baseline captures/2026-06-09-validation-rewording.json
 *
 * Flags: --region <r> (baseline region, default eu-west-2); --out <file>
 * (default stdout). The result carries a top-level `clean` boolean so a caller
 * can branch on drift without parsing the probe list. Reads captures, never
 * results/*.json, and writes its own artefact, so summarise.mjs and the website
 * scorer stay byte-stable.
 */

import { readFileSync, writeFileSync } from 'node:fs'
import { diffCaptures, diffRegions, isClean } from './lib/drift.mjs'

export function parseArgs(argv) {
  const args = { region: 'eu-west-2', baseline: null, out: null, _: [] }
  for (let i = 0; i < argv.length; i++) {
    const a = argv[i]
    if (a === '--region') args.region = argv[++i]
    else if (a === '--baseline') args.baseline = argv[++i]
    else if (a === '--out') args.out = argv[++i]
    else args._.push(a)
  }
  return args
}

function main() {
  const args = parseArgs(process.argv.slice(2))
  const [currentPath] = args._
  if (!currentPath) {
    console.error(
      'usage: drift-diff.mjs <capture.json> [--baseline <file>] [--region <r>] [--out <file>]',
    )
    process.exit(2)
  }
  const current = JSON.parse(readFileSync(currentPath, 'utf8'))

  let result
  if (args.baseline) {
    const baseline = JSON.parse(readFileSync(args.baseline, 'utf8'))
    const drift = diffCaptures(baseline.regions?.[args.region], current.regions?.[args.region])
    result = { mode: 'across-time', region: args.region, baseline: args.baseline, clean: isClean(drift), drift }
  } else {
    const cross = diffRegions(current, args.region)
    result = { mode: 'cross-region', ...cross, clean: Object.values(cross.regions).every(isClean) }
  }

  const json = JSON.stringify(result, null, 2)
  if (args.out) {
    writeFileSync(args.out, json + '\n')
    console.error(`drift written to ${args.out} (clean=${result.clean})`)
  } else {
    process.stdout.write(json + '\n')
  }
}

if (import.meta.url === `file://${process.argv[1]}`) main()
