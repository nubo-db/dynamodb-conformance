#!/usr/bin/env node

/**
 * Report which targets look like they share an implementation, from the
 * committed results.
 *
 *   node scripts/lineage.mjs
 *
 * A report, not a published artefact. It writes nothing and feeds nothing on
 * the board: a flagged pair is a prompt to go and check, and what the board
 * eventually says about a relationship is a separate decision from measuring
 * it. See scripts/lib/lineage.mjs for why this is measured rather than typed.
 */

import { readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import { GROUND_TRUTH_SLUG, targetResultSlug } from './lib/score.mjs'
import { failureSet, lineageReport } from './lib/lineage.mjs'
import { display, projectOf } from './lib/targets.mjs'

const targets = readdirSync('results')
  .map(targetResultSlug)
  .filter((slug) => slug && slug !== GROUND_TRUTH_SLUG)
  .map((slug) => ({
    slug,
    failures: failureSet(JSON.parse(readFileSync(join('results', `${slug}.json`), 'utf8'))),
  }))

const { pairs, baseline, deviation, threshold } = lineageReport(targets, {
  sameProject: (a, b) => projectOf(a) === projectOf(b),
})

if (pairs.length === 0) {
  console.log('Nothing to compare: fewer than two targets failed anything.')
  process.exit(0)
}

const pct = (v) => `${v.toFixed(1)}%`
console.log('Shared-failure similarity (Jaccard: shared / combined)\n')
for (const p of pairs) {
  const name = `${display(p.a)} vs ${display(p.b)}`
  console.log(
    `  ${p.flagged ? '►' : ' '} ${name.padEnd(38)}${pct(p.similarity).padStart(6)}` +
      `   (${p.shared} shared of ${p.combined})`,
  )
}
console.log(
  `\nbaseline ${pct(baseline)}, deviation ${pct(deviation)}, ` +
    `flagged above ${pct(threshold)}`,
)

const flagged = pairs.filter((p) => p.flagged)
if (flagged.length === 0) {
  console.log('\nNo pair stands out from the baseline.')
} else {
  console.log(
    `\n${flagged.length} pair${flagged.length === 1 ? '' : 's'} well clear of the baseline. ` +
      `Engines that share an implementation inherit each other's divergences, ` +
      `so this is worth checking against what each project documents.`,
  )
}
