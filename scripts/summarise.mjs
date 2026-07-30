#!/usr/bin/env node

/**
 * Post-process Vitest JSON output files into the published results artefacts:
 * the Markdown comparison table (README) and the versioned per-region summary
 * the site consumes (results/summary.json).
 *
 * Usage:
 *   node scripts/summarise.mjs                 # all results/*.json -> table on stdout
 *   node scripts/summarise.mjs results/*.json  # explicit files     -> table on stdout
 *   node scripts/summarise.mjs --write         # splice the README markers and
 *                                              # refresh results/summary.json
 *
 * Each JSON file is a Vitest --reporter=json output; the target slug is the
 * filename (e.g. "dynoxide" from "dynoxide.json"). Run date comes from the
 * Vitest run; target version from an optional sibling "<slug>.version" file;
 * an optional "<slug>.indeterminate.json" sidecar (src/indeterminate-sink.ts)
 * qualifies the run's failed observations.
 *
 * Scoring is per region: each target is scored against every observed
 * region's recorded expectations (scripts/lib/score.mjs, reading the split
 * registry), and its headline - the table's Total - is the best of them. The
 * real-DynamoDB row renders 100%, earned rather than assumed: each real region
 * scores 100% against its own recorded behaviour by construction, so the max
 * over any observed set is 100% too.
 *
 * The percentage is correctness over IMPLEMENTED, OBSERVED operations:
 * passed / (passed + failed). Two kinds of test are excluded from it, for two
 * reasons that must not be blurred: skips (honest scope - the feature probe
 * declined to run an operation the target does not implement, reported in
 * their own column) and indeterminates (failed observations - a timeout,
 * exhausted throttle or transport fault means nobody knows the answer).
 *
 * results/summary.json is ADDITIVE: the per-target results/<slug>.json files
 * are never modified or reshaped here, so the site's existing reader (and its
 * tag-manifest join on file path + top-level describe) keeps working while the
 * new per-region view is adopted on its own schedule.
 */

import { existsSync, readFileSync, readdirSync, writeFileSync } from 'node:fs'
import { basename, join } from 'node:path'
import {
  GROUND_TRUTH_SLUG,
  axesOf,
  cohortOf,
  isPublishedTarget,
  loadScoringContext,
  passRate,
  regionLabel,
  scoreTarget,
} from './lib/score.mjs'
import { isObserved, observedRegions } from './lib/observed.mjs'
import { gradeOf } from './lib/grade.mjs'
import {
  configurationOf,
  display,
  isVariant,
  label,
  projectOf,
} from './lib/targets.mjs'

/** Version of the results/summary.json contract the site consumes. */
export const SUMMARY_SCHEMA_VERSION = 1

/** Where the versioned summary artefact lives. */
export const SUMMARY_PATH = 'results/summary.json'

// The target registry - who is on the board, how they relate, and how you run
// them - lives in ./lib/targets.mjs. Re-exported here because the site and the
// tooling tests import these names from this module.
export {
  CHANNELS,
  CHANNELS_SHOWN,
  DISPLAY,
  REPO,
  TARGETS,
  configurationOf,
  display,
  distributionOf,
  isVariant,
  label,
  projectOf,
  repoUrl,
} from './lib/targets.mjs'

// ── Reading the target namespace ─────────────────────────────────────────────

/**
 * Read target result files into { slug, raw, sidecar, version, runDate }.
 * Reserved scratch slugs (local, summary) are never published targets, and
 * badge/sidecar files are companions rather than targets, so all are skipped
 * here; a sidecar is instead paired up with the results file it qualifies.
 */
export function readTargets(files) {
  const targets = []
  for (const file of files) {
    if (!file.endsWith('.json')) continue
    if (file.endsWith('.badge.json') || file.endsWith('.indeterminate.json')) continue
    const slug = basename(file, '.json')
    if (!isPublishedTarget(slug)) continue

    const raw = JSON.parse(readFileSync(file, 'utf8'))
    const sidecarFile = file.replace(/\.json$/, '.indeterminate.json')
    const sidecar = existsSync(sidecarFile)
      ? JSON.parse(readFileSync(sidecarFile, 'utf8'))
      : null
    const versionFile = file.replace(/\.json$/, '.version')
    const version =
      (existsSync(versionFile) && readFileSync(versionFile, 'utf8').trim()) || '-'
    const runDate = raw.startTime
      ? new Date(raw.startTime).toISOString().slice(0, 10)
      : '-'

    targets.push({ slug, raw, sidecar, version, runDate })
  }
  return targets
}

// ── Region standing ──────────────────────────────────────────────────────────

/**
 * Sort the tracked regions into their published standing:
 *
 * - observed: regions a score may draw on (scripts/lib/observed.mjs);
 * - unresolved: the subset of observed regions that missed the latest
 *   sweep. Still scored against - their registry rows are retained - but
 *   published as carrying forward their last resolved data, never omitted;
 * - dropped: regions excluded from scoring, either for missing two
 *   consecutive sweeps or for never having resolved at all.
 *
 * A region that is silently absent would be indistinguishable from a region
 * that agreed, so every tracked region appears in exactly one of these lists.
 */
export function regionStanding(health) {
  // observedRegions throws loudly when every region has dropped, which is
  // the behaviour the table wants too: a table scored against nothing is not
  // a table.
  const observed = observedRegions(health)
  return {
    observed,
    unresolved: observed.filter((r) => health.regions[r].consecutiveUnresolved > 0),
    dropped: Object.entries(health.regions)
      .filter(([, entry]) => !isObserved(entry))
      .map(([region]) => region)
      .sort(),
  }
}

// ── The summary artefact ─────────────────────────────────────────────────────

// Published rates are rounded to one decimal everywhere (table, badge,
// summary), so the three surfaces show one number. Raw counts are carried
// alongside for anyone recomputing at full precision.
const round1 = (rate) => (rate === null ? null : Number(rate.toFixed(1)))

// The cohort a headline was measured against, as "N of M". Null-safe on both
// sides: a summary with no observed set (a minimal fixture, or a payload from
// before per-region scoring) has no cohort to state, and neither does a target
// that scored nothing. "0 of 0" would read as a measurement.
const cohortCount = (matched, summary, rate) => {
  const observed = observedCount(summary)
  return rate === null || observed === 0 ? '-' : `${matched} of ${observed}`
}

/**
 * Build the versioned summary object from read targets and the scoring
 * context. Deterministic for a given input: targets and regions are sorted,
 * and nothing here stamps a "generated at" time - the run dates come from the
 * result files, so a re-run over the same inputs is byte-identical.
 */
export function buildSummary(targets, { registry, health }) {
  const standing = regionStanding(health)

  const summary = {
    schemaVersion: SUMMARY_SCHEMA_VERSION,
    regions: {
      ...standing,
      detail: Object.fromEntries(
        Object.keys(health.regions)
          .sort()
          .map((r) => [r, health.regions[r]]),
      ),
    },
    // Real DynamoDB's row is 100% because each real region scores 100% against
    // its own recorded behaviour by construction (self-agreement), so the max
    // over any observed set is 100%. Earned, not assumed.
    groundTruth: { slug: GROUND_TRUTH_SLUG, rate: 100, runDate: '-' },
    targets: {},
  }

  for (const t of [...targets].sort((a, b) => a.slug.localeCompare(b.slug))) {
    if (t.slug === GROUND_TRUTH_SLUG) {
      // Scores are self-agreement (above); keep the date of the last
      // successful real-AWS run so the ground-truth row isn't dateless.
      summary.groundTruth.runDate = t.runDate
      continue
    }
    const scored = scoreTarget(t.raw, t.sidecar, {
      registry,
      observed: standing.observed,
    })
    // Files in results/ that aren't a target's Vitest output (e.g.
    // tag-manifest.json) score nothing; skip them rather than emit a row.
    if (!scored) continue

    const regions = {}
    for (const region of standing.observed) {
      const r = scored.regions[region]
      regions[region] = {
        rate: round1(passRate(r.passed, r.failed)),
        passed: r.passed,
        failed: r.failed,
        skipped: r.skipped,
        indeterminate: r.indeterminate,
        count: r.count,
        tiers: r.summary,
      }
    }

    summary.targets[t.slug] = {
      headline: { region: scored.headline.region, rate: round1(scored.headline.rate) },
      regions,
      version: t.version,
      runDate: t.runDate,
    }
  }

  return summary
}

/** Write the summary artefact (results/summary.json). */
export function writeSummaryFile(summary, path = SUMMARY_PATH) {
  writeFileSync(path, JSON.stringify(summary, null, 2) + '\n')
}

// ── The Markdown table ───────────────────────────────────────────────────────

const pct = (rate) => (rate === null ? '-' : `${rate.toFixed(1)}%`)

// A tier's divergence: its fails over its whole size, the same shape as the
// headline. The tier columns used to be correctness over what the tier
// attempted, so on a row sorted by divergence a rising tier figure was the
// target getting better and the two columns read in opposite directions.
const tierDivergence = (t) => {
  const total = t.p + t.f + t.s + t.i
  const implemented = t.p + t.f
  return total === 0 || implemented === 0 ? null : (t.f / total) * 100
}

/**
 * The table's rows, structured: the ground-truth row first, then targets by
 * headline rate descending (dateless "-" rates last), name breaking ties.
 * Tier and count columns show the headline region's scoring - the region the
 * target's divergence was earned in, whose size the Regions column states.
 */
/** How many regions the run scored against, 0 when none were recorded. Read by
 *  cohortCount to publish the "N of M" cohort the README's Regions column shows. */
const observedCount = (summary) => summary.regions?.observed?.length ?? 0

// Regional variation is deliberately not rendered here. Real DynamoDB differs
// between regions on three of ~1000 behaviours, which moves a target's figure
// by at most 0.3 points - far less than the cost of explaining regional
// divergence to a reader who does not know it exists. Worse, the earlier
// per-region label actively misled: a target matching 6 regions exactly read as
// narrower than one matching 33 equally badly, even when the first diverges
// less in its worst region than the second does in its best. The caveat is
// stated in the caption and the per-region breakdown is a click away.

export function tableRows(summary) {
  const rows = Object.entries(summary.targets).map(([slug, t]) => {
    const best = t.regions[t.headline.region]
    // Name the cohort the headline was earned in, not the lone tie-break
    // winner. Ties are read off the published per-region rates, the same
    // numbers a viewer sees, so the label matches paritysuite.org's.
    const cohort = cohortOf(
      Object.entries(t.regions).map(([region, r]) => ({ region, rate: r.rate })),
    )
    // The two published axes, both over the whole suite so neither can hide
    // behind the other. Divergence is what a target gets wrong; coverage is how
    // much it attempts. A fail and a skip are deliberately not interchangeable:
    // an operation a target declines is discoverable in minutes, one it gets
    // quietly wrong is discovered in production, so they are never summed into
    // a single figure. The maths lives in axesOf, shared with the badges.
    const { divergence: divergenceRate, coverage: coverageRate } = axesOf(best)

    // The regional range lives on the site's target page, beside that target's
    // headline, where there is room to say what it means. The README carries the
    // cohort count instead: it answers "how many regions is this measured
    // against" in one column, without a second percentage to misread.
    return {
      slug,
      target: label(slug),
      grade: gradeOf(divergenceRate, coverageRate).letter ?? '-',
      tier1: pct(tierDivergence(best.tiers.tier1)),
      tier2: pct(tierDivergence(best.tiers.tier2)),
      tier3: pct(tierDivergence(best.tiers.tier3)),
      divergence: pct(divergenceRate),
      coverage: pct(coverageRate),
      divergenceValue: divergenceRate,
      coverageValue: coverageRate,
      total: pct(t.headline.rate),
      cohort: cohortCount(cohort.regions.length, summary, t.headline.rate),
      cohortLabel: t.headline.rate === null ? '-' : regionLabel(cohort),
      passed: best.passed,
      failed: best.failed,
      skipped: best.skipped,
      count: best.count,
      version: t.version,
      runDate: t.runDate,
    }
  })

  // Tie-break on the display name, not the `[name](url)` label. Comparing the
  // label sorts on the first character after the name - a `]` for a bare name,
  // a space for a parenthetical one - so `[Dynoxide (wasm)]` would sort above
  // `[Dynoxide]` on an equal total, putting the preview above the engine it is
  // a variant of. Comparing names makes a base engine a prefix of its variant,
  // so `Dynoxide` sorts above `Dynoxide (wasm)`.
  const sortName = (row) => {
    const m = row.target.match(/^\[([^\]]+)\]/)
    return m ? m[1] : row.target
  }
  // Ordered by what a target gets wrong, ascending, then by how much it
  // attempts. The sort key is a risk measure rather than a verdict on which
  // engine is better: a target with no divergences over a narrow surface is
  // described accurately by its own two figures, so the order needs no
  // coverage floor to stay honest. Nulls (nothing scored) sort last.
  const asc = (v) => (v == null ? Number.POSITIVE_INFINITY : v)
  const desc = (v) => (v == null ? Number.NEGATIVE_INFINITY : v)
  const byRisk = (a, b) =>
    asc(a.divergenceValue) - asc(b.divergenceValue) ||
    desc(b.coverageValue) - desc(a.coverageValue) ||
    sortName(a).localeCompare(sortName(b))

  // Only projects compete for a place in the order; a variant travels with its
  // parent. Sorting variants into the same list would seat builds of one engine
  // in consecutive top slots, which reads as a project occupying the board
  // rather than as one engine with two shapes.
  const byProject = new Map()
  for (const row of rows) {
    const project = projectOf(row.slug)
    if (!byProject.has(project)) byProject.set(project, [])
    byProject.get(project).push(row)
  }
  const parents = []
  for (const group of byProject.values()) {
    const parent = group.find((r) => !isVariant(r.slug)) ?? group[0]
    parent.variants = group.filter((r) => r !== parent).sort(byRisk)
    parents.push(parent)
  }
  parents.sort(byRisk)
  rows.length = 0
  rows.push(...parents)

  // Suite size: the largest test count seen, i.e. a full-suite run. Over
  // parents and their nested variants both - by this point `rows` holds only
  // parents, and a build can carry the newest (largest) run when its project
  // was not re-tested.
  const suiteSize = Math.max(
    0,
    ...rows.flatMap((r) => [r.count, ...(r.variants ?? []).map((v) => v.count)]),
  )
  const groundTruth = {
    slug: summary.groundTruth.slug,
    target: label(summary.groundTruth.slug),
    grade: gradeOf(0, 100).letter,
    tier1: '0.0%',
    tier2: '0.0%',
    tier3: '0.0%',
    total: '100.0%',
    divergence: '0.0%',
    coverage: '100.0%',
    divergenceValue: 0,
    coverageValue: 100,
    // Real DynamoDB is every region's own behaviour, so its row is not pinned
    // to one region the way a target's headline is.
    cohort: cohortCount(observedCount(summary), summary, 100),
    cohortLabel: 'all regions',
    passed: suiteSize,
    failed: 0,
    skipped: 0,
    count: suiteSize,
    version: 'live (AWS)',
    runDate: summary.groundTruth.runDate,
  }

  return [groundTruth, ...rows]
}

/**
 * The caption above the table: which regions the numbers were scored against,
 * with unresolved and dropped regions named explicitly. A reader must never be
 * able to mistake an unresolved region for an agreeing one, so absence is
 * spelled out rather than implied.
 */
export function tableCaption(regions) {
  const list = (rs) => rs.map((r) => `\`${r}\``).join(', ')
  const sentences = [
    `Scored against real DynamoDB's recorded behaviour in each observed region ` +
      `(${list(regions.observed)}), at each target's best-matching region. ` +
      `**Divergence** is the share of the whole suite a target answers differently ` +
      `from real DynamoDB - the operations it implements and gets wrong. ` +
      `**Coverage** is the share it implements at all. They are reported apart ` +
      `because they carry opposite risks: a declined operation is discoverable in ` +
      `minutes, a wrong one in production. **Grade** is a reading of the pair, ` +
      `never a blend of it: divergence sets the letter and low coverage can only ` +
      `cap it, under the versioned criteria in the ` +
      `[methodology](https://paritysuite.org/methodology). Sorted by divergence, so the order ranks ` +
      `risk rather than declaring a winner - a target with no divergences over a ` +
      `narrow surface is exactly what its two figures say it is. Regions is how many ` +
      `of the observed regions the headline was measured against. The tier columns ` +
      `are divergence too, within each tier, so lower is better in every column ` +
      `but Coverage. Real DynamoDB does ` +
      `not behave identically in every region, so each target is measured in every ` +
      `region above and scored against its best match; the per-region detail is in ` +
      `\`results/summary.json\`. Behaviour varies by region and over time, so these ` +
      `are point-in-time figures.`,
  ]
  if (regions.unresolved.length > 0) {
    sentences.push(
      `${list(regions.unresolved)} did not resolve the latest sweep and ` +
        `${regions.unresolved.length === 1 ? 'carries' : 'carry'} forward the last ` +
        `resolved data.`,
    )
  }
  if (regions.dropped.length > 0) {
    sentences.push(
      `${list(regions.dropped)} ${regions.dropped.length === 1 ? 'has' : 'have'} been ` +
        `dropped from the observed set and ` +
        `${regions.dropped.length === 1 ? 'is' : 'are'} not scored against.`,
    )
  }
  return `_${sentences.join(' ')}_`
}

/** Render the full table block: caption plus Markdown table. */
// A variant is rendered as an indented row directly beneath its project,
// labelled with what makes it distinct rather than repeating the project name.
// Markdown has no nested tables, so the indent is the relationship: the reader
// sees one entry per project, and a build of one reads as a build rather than
// as a rival. This replaces a footnote keyed off a bracket in the display name.
const VARIANT_PREFIX = '↳ '

export function renderTable(summary) {
  const rows = tableRows(summary)
  const fmt = (r, name) =>
    `| ${name} | ${r.grade} | ${r.divergence} | ${r.coverage} | ${r.cohort ?? '-'} | ${r.tier1} | ${r.tier2} | ${r.tier3} | ${r.failed} | ${r.skipped} | ${r.version} | ${r.runDate} |`
  const body = [
    // Regions is the cohort the headline was measured against, as a count. The
    // Region column this replaces named the cohort, which read as breadth: a
    // target equally wrong in all 33 showed "all regions" while one perfect in 6
    // showed "6 regions". A count out of the observed total cannot.
    '| Target | Grade | Divergence | Coverage | Regions | Tier 1 | Tier 2 | Tier 3 | Fail | Skip | Version | Date |',
    '|--------|-------|-----------|----------|---------|--------|--------|--------|------|------|---------|------|',
    ...rows.flatMap((r) => [
      // The parent's figures are its reference configuration's, so name that
      // configuration inline when the project has more than one shape. Without
      // it a reader cannot tell which storage engine or build was measured, and
      // the row silently becomes ambiguous the moment a second one ships.
      fmt(r, configurationOf(r.slug) ? `${r.target} · ${configurationOf(r.slug)}` : r.target),
      ...(r.variants ?? []).map((v) =>
        fmt(v, `${VARIANT_PREFIX}${configurationOf(v.slug) ?? display(v.slug)}`),
      ),
    ]),
  ].join('\n')
  return `${tableCaption(summary.regions)}\n\n${body}`
}

// ── CLI ──────────────────────────────────────────────────────────────────────

function main() {
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

  const summary = buildSummary(readTargets(files), loadScoringContext())
  const table = renderTable(summary)

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
    writeSummaryFile(summary)
    console.error(`Updated the results table in ${path} and wrote ${SUMMARY_PATH}.`)
  } else {
    console.log(table)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) main()
