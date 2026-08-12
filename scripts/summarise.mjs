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
 * over any observed set is 100% too. Real AWS is observed in three runs rather
 * than one, so its lane documents ("<ground truth>.<lane>.json") are folded in
 * before scoring and their dates published beside the row.
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
import { join } from 'node:path'
import {
  GROUND_TRUTH_LANES,
  GROUND_TRUTH_SLUG,
  axesOf,
  cohortOf,
  isTargetResultFile,
  loadScoringContext,
  passRate,
  regionLabel,
  scoreTarget,
  targetResultSlug,
  verdictsForRegion,
} from './lib/score.mjs'
import { classifyResults } from './lib/classify.mjs'
import { relativeTestPath, testIdentities } from './lib/identity.mjs'
import { suiteIdentities, suiteSizeOf } from './suite-manifest.mjs'
import { isObserved, observedRegions } from './lib/observed.mjs'
import { BASELINE_LABEL, gradeOf } from './lib/grade.mjs'
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

/** A run's capture date as YYYY-MM-DD, or "-" for a document with no start time. */
const runDateOf = (raw) =>
  raw?.startTime ? new Date(raw.startTime).toISOString().slice(0, 10) : '-'

/** The name the ground truth's gating run is published under in `lanes`. */
export const GATING_LANE = 'gating'

/**
 * Fold sibling lane documents into a ground-truth document, deduplicating on
 * test identity.
 *
 * Identity is `<repo-relative file>::<fullName>`, the key
 * scripts/ground-truth-coverage.mjs reconciles the lanes on. Shared rather
 * than restated: if the merge and the coverage check disagreed about what
 * "the same test" is, one would count an observation the other cannot see.
 * The gating run wins any overlap, so a lane can add tests but never restate
 * an answer the gate already recorded.
 */
export function mergeLanes(base, docs) {
  const seen = testIdentities(base)
  const merged = {
    ...base,
    testResults: base.testResults.map((tr) => ({
      ...tr,
      assertionResults: [...(tr.assertionResults ?? [])],
    })),
  }
  // Keyed on the repo-relative path: the lanes run in their own CI jobs, so
  // the same test file arrives under a different absolute prefix and would
  // otherwise be appended as a second entry for one file.
  const byFile = new Map(merged.testResults.map((tr) => [relativeTestPath(tr.name), tr]))

  for (const doc of docs) {
    for (const tr of doc.testResults ?? []) {
      const path = relativeTestPath(tr.name)
      for (const ar of tr.assertionResults ?? []) {
        const id = `${path}::${ar.fullName}`
        if (seen.has(id)) continue
        seen.add(id)
        let into = byFile.get(path)
        if (!into) {
          into = { ...tr, assertionResults: [] }
          byFile.set(path, into)
          merged.testResults.push(into)
        }
        into.assertionResults.push(ar)
      }
    }
  }
  return merged
}

/**
 * The ground-truth document with its sibling lanes folded in, plus what each
 * lane contributed: { raw, lanes, missingLanes }.
 *
 * The lanes are captured at different times, and three runs rendered as one
 * measurement is worse than a disclosed literal, so each one's date and size
 * are published rather than averaged away. A lane that did not run is simply
 * not merged, which leaves the document exactly as the gating run wrote it.
 */
function withLanes(file, base) {
  const lanes = [{ name: GATING_LANE, runDate: runDateOf(base), tests: testIdentities(base).size }]
  const docs = []
  const unusable = []
  for (const lane of GROUND_TRUTH_LANES) {
    const path = file.replace(/\.json$/, `.${lane}.json`)
    if (!existsSync(path)) continue

    let doc
    try {
      doc = JSON.parse(readFileSync(path, 'utf8'))
    } catch (err) {
      // A truncated artefact used to abort the whole regeneration with a bare
      // stack trace. Every other degraded input in this pipeline warns and
      // carries on, and the lane is optional by construction.
      console.warn(`::warning title=Unreadable lane::${path} (${err.message}); treating the lane as absent`)
      unusable.push(lane)
      continue
    }

    // A lane's own sidecar travels with its document. Without it a run-level
    // indeterminate in the lane arrives as plain failed assertions: the tests
    // still count as observed, `unobserved` empties, the row derives, and the
    // board publishes real DynamoDB diverging from itself.
    const sidecarPath = path.replace(/\.json$/, '.indeterminate.json')
    if (existsSync(sidecarPath)) {
      console.warn(
        `::warning title=Lane not observed::${path} shipped an indeterminate sidecar; the baseline row stays pinned`,
      )
      unusable.push(lane)
      continue
    }

    docs.push(doc)
    lanes.push({ name: lane, runDate: runDateOf(doc), tests: testIdentities(doc).size })
  }
  const present = new Set(lanes.map((l) => l.name))
  return {
    raw: docs.length === 0 ? base : mergeLanes(base, docs),
    lanes,
    // An unreadable or unobserved lane is missing, not merged. The row then
    // stays pinned and names the tests nobody ran, rather than deriving from
    // an observation that did not happen.
    missingLanes: GROUND_TRUTH_LANES.filter((lane) => !present.has(lane) || unusable.includes(lane)),
  }
}

/**
 * Read target result files into { slug, raw, sidecar, version, runDate }.
 * Reserved scratch slugs (local, summary) are never published targets, and
 * badge/sidecar/lane files are companions rather than targets, so all are
 * skipped here; a sidecar is instead paired up with the results file it
 * qualifies, and a lane document folded into the ground-truth run it evidences
 * (which also carries `lanes` and `missingLanes`).
 */
export function readTargets(files) {
  const targets = []
  for (const file of files) {
    const slug = targetResultSlug(file)
    if (!slug) continue

    const raw = JSON.parse(readFileSync(file, 'utf8'))
    const sidecarFile = file.replace(/\.json$/, '.indeterminate.json')
    const sidecar = existsSync(sidecarFile)
      ? JSON.parse(readFileSync(sidecarFile, 'utf8'))
      : null
    const versionFile = file.replace(/\.json$/, '.version')
    const version =
      (existsSync(versionFile) && readFileSync(versionFile, 'utf8').trim()) || '-'
    const runDate = runDateOf(raw)

    targets.push({
      slug,
      raw,
      sidecar,
      version,
      runDate,
      ...(slug === GROUND_TRUTH_SLUG ? withLanes(file, raw) : {}),
    })
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
export function buildSummary(targets, { registry, health, suite = suiteIdentities() }) {
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
    // over any observed set is 100%. Earned, not assumed. `derived` says
    // whether the counts under it were measured or pinned, and the lanes below
    // say what they were measured from - see recordGroundTruth.
    groundTruth: {
      slug: GROUND_TRUTH_SLUG,
      rate: 100,
      runDate: '-',
      derived: false,
      testsObserved: 0,
      suiteSize: suite.size,
      lanes: [],
      missingLanes: [...GROUND_TRUTH_LANES],
      counts: null,
    },
    targets: {},
  }

  let baseline = null
  for (const t of [...targets].sort((a, b) => a.slug.localeCompare(b.slug))) {
    if (t.slug === GROUND_TRUTH_SLUG) {
      // Scores are self-agreement (above); keep the date of the last
      // successful real-AWS run so the ground-truth row isn't dateless.
      summary.groundTruth.runDate = t.runDate
      baseline = t
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
      ...regionFailuresOf(t, scored, registry, standing.observed),
    }
  }

  if (baseline) {
    recordGroundTruth(summary, baseline, { registry, observed: standing.observed, suite })
  }

  return summary
}

/**
 * Name the tests a zero-divergence target fails outside its headline region.
 *
 * The site build checks the A+ premise and cannot recompute verdicts: it
 * renders fetched data, and the summary carried per-region counts alone, which
 * makes three fails on confirmed splits indistinguishable from three fails on
 * anything else. The suite holds the verdicts, so it publishes the names and
 * the build joins them against the split registry itself.
 *
 * Only zero-divergence targets carry the field, and within them only regions
 * that fail, so the artefact grows by the names under discussion rather than
 * by the board. Every other target gets no key at all.
 */
function regionFailuresOf(target, scored, registry, observed) {
  const headline = scored.regions[scored.headline.region]
  if (!headline || headline.count === 0 || axesOf(headline).divergence !== 0) return {}

  const verdicts = classifyResults(target.raw, target.sidecar ?? null)
  const regionFailures = {}
  for (const region of observed) {
    const names = verdictsForRegion(verdicts, registry, region)
      .filter((v) => v.verdict === 'fail')
      // Full identity, not the bare name. splitFor matches on file AND
      // fullName, and a title is unique only within its file, so joining on
      // the name alone would let a same-named test in another file satisfy
      // the guard - which is the substitution the guard exists to catch.
      .map((v) => `${relativeTestPath(v.file)}::${v.fullName}`)
      .sort()
    if (names.length > 0) regionFailures[region] = names
  }
  return { regionFailures }
}

/**
 * Publish the baseline's provenance, and measure its row only where the
 * real-AWS lanes together span the whole suite.
 *
 * The row's figures are divided by the suite size, so a row derived from a
 * partial merge would state counts over a surface nobody observed: the gating
 * lane alone stops short by the tests the other lanes exist to run. Short of
 * the suite the row stays pinned and says so, naming the lanes that are
 * missing and how far the observation fell short. A disclosed pin is honest;
 * a measurement quietly narrower than the row it fills is not.
 */
function recordGroundTruth(summary, baseline, context) {
  const gt = summary.groundTruth
  const observedTests = testIdentities(baseline.raw)
  gt.testsObserved = observedTests.size
  // A baseline read off disk carries its lanes; one handed straight to
  // buildSummary is the gating run on its own.
  gt.lanes = baseline.lanes ?? [
    { name: GATING_LANE, runDate: baseline.runDate, tests: gt.testsObserved },
  ]
  gt.missingLanes = baseline.missingLanes ?? []

  // What the suite contains, by test identity, from the suite's own manifest.
  // This used to be the widest emulator run on the board, which put one of the
  // measured things in charge of the denominator every other figure divides by.
  const { suite } = context

  // Spanning, not counting. A cardinality check passes on the right number of
  // the wrong tests, and the three lanes have independently changing test sets,
  // which is exactly the shape that produces one. So the row derives only when
  // every test the suite contains was actually observed against real AWS.
  gt.unobserved = [...suite].filter((id) => !observedTests.has(id)).sort()

  // Nothing else on the board means no suite to check against, which is not the
  // same as agreement.
  if (gt.suiteSize === 0 || gt.unobserved.length > 0) return

  const scored = scoreTarget(baseline.raw, baseline.sidecar, context)
  if (!scored) return
  const best = scored.regions[scored.headline.region]
  gt.derived = true
  gt.rate = round1(scored.headline.rate)
  gt.counts = {
    passed: best.passed,
    failed: best.failed,
    skipped: best.skipped,
    indeterminate: best.indeterminate,
    count: best.count,
    tiers: best.summary,
  }
}

/**
 * Every published row divides by the same whole-suite count, or the artefact
 * is not written.
 *
 * A short results file is a cheaper lever than scope withdrawal, and in the
 * opposite direction: withdrawal trades divergence against coverage one for
 * one, but a truncated denominator lowers divergence and raises coverage at
 * once. Nothing about the file says it is short - it reads as a target that
 * simply ran fewer tests. This was asserted in the tooling suite, which no
 * publishing path runs, so the board could ship a row the tests would have
 * rejected.
 *
 * A target that scored nothing publishes "-" rather than a shrunken figure and
 * is not in scope. Everything else is measured against the suite manifest,
 * which also catches a carried row whose count went stale while the suite grew.
 * A row above the manifest is the same check read the other way: no target can
 * run more tests than the suite defines, so the manifest needs regenerating
 * before anything published divides by it.
 */
export function assertOneDenominator(summary, size = suiteSizeOf()) {
  // A headline naming a region the row has no results for is a bug in the
  // scorer, not a target that scored nothing, so it is reported rather than
  // exempted. `?? 0` used to collapse the two into the same silent pass.
  const counts = Object.entries(summary.targets).map(([slug, t]) => {
    const region = t.regions[t.headline.region]
    return [slug, region ? region.count : null]
  })
  const wrong = counts.filter(([, c]) => c !== 0 && c !== size)
  if (wrong.length === 0) return
  const detail = wrong
    .map(([slug, c]) =>
      c === null ? `${slug} has no results for its headline region` : `${slug} scored ${c}`,
    )
    .join(', ')
  throw new Error(
    `refusing to publish: every row divides by the ${size}-test suite, but ${detail}. ` +
      (wrong.some(([, c]) => c !== null && c > size)
        ? 'registry/suite-manifest.json is stale. Run: node scripts/suite-manifest.mjs'
        : 'A short results file lowers divergence and raises coverage at the same time.'),
  )
}

/**
 * Every published row measures the tests the suite actually has, or the
 * artefact is not written.
 *
 * The count check above passes on the right number of the wrong tests, and
 * that is not a hypothetical: a results file carried from before a test was
 * moved or renamed keeps its old total while naming tests that no longer
 * exist. Its divergence and coverage then divide by the right denominator over
 * the wrong population, and nothing in the file says so - it reads as a target
 * that ran everything. This was invisible while the suite size came from the
 * widest run, because the widest run had nothing to disagree with.
 *
 * Checking for strays alone still left the population forgeable, because
 * identities are a set and a repeated one collapses into it: drop a failing
 * result, duplicate a passing one, and the file keeps the right total, names no
 * test the suite lacks, and reports a lower divergence. So repetition is
 * counted too. A file with no strays, no repeats and the whole-suite total from
 * `assertOneDenominator` can only be the suite itself - a subset of the right
 * size with nothing counted twice leaves nothing out.
 *
 * Repetition is checked on every target, including the deliberately partial
 * ground-truth lanes, since no run has cause to report one test twice.
 */
export function assertMeasuredSuite(targets, suite = suiteIdentities()) {
  const stale = []
  const repeated = []
  for (const t of targets) {
    if (!Array.isArray(t.raw?.testResults)) continue
    const ids = testIdentities(t.raw)
    const stray = [...ids].filter((id) => !suite.has(id)).sort()
    if (stray.length > 0) stale.push([t.slug, stray])
    const reported = t.raw.testResults.reduce(
      (n, tr) => n + (tr.assertionResults?.length ?? 0),
      0,
    )
    if (reported !== ids.size) repeated.push([t.slug, reported - ids.size])
  }
  if (stale.length > 0) {
    const detail = stale
      .map(([slug, stray]) => `${slug} ran ${stray.length} (${stray[0]})`)
      .join('; ')
    throw new Error(
      `refusing to publish: some rows name tests the suite no longer defines - ${detail}. ` +
        'Those results predate a change to the tests, so they are measured over a population ' +
        'the suite no longer has. Re-run the target, or drop its results file until it is re-run.',
    )
  }
  if (repeated.length === 0) return
  const detail = repeated.map(([slug, n]) => `${slug} reports ${n} twice`).join('; ')
  throw new Error(
    `refusing to publish: some rows count a test more than once - ${detail}. ` +
      'A repeated result fills the suite total without measuring the test it stands in for, ' +
      'so the row divides by the right denominator over a population it did not run. ' +
      'Re-run the target.',
  )
}

/**
 * Publish the board: the README table and the summary artefact, in that order,
 * and neither unless both guards pass.
 *
 * The guards used to run inside writeSummaryFile, which is called after the
 * README has already been spliced to disk. A refusal then left the rejected
 * table published and the artefact it belongs to missing - the half-written
 * state the guards exist to prevent. Nothing published may be touched until
 * the whole board has been checked, so they run here, first.
 */
export function publish(summary, targets, { readmePath = 'README.md', summaryPath = SUMMARY_PATH } = {}) {
  assertOneDenominator(summary)
  assertMeasuredSuite(targets)

  const start = '<!-- results:start -->'
  const end = '<!-- results:end -->'
  const md = readFileSync(readmePath, 'utf8')
  const s = md.indexOf(start)
  const e = md.indexOf(end)
  if (s === -1 || e === -1) {
    throw new Error(`Could not find ${start} / ${end} markers in ${readmePath}`)
  }
  writeFileSync(readmePath, `${md.slice(0, s + start.length)}\n${renderTable(summary)}\n${md.slice(e)}`)
  writeSummaryFile(summary, targets, summaryPath)
}

/** Write the summary artefact (results/summary.json). */
export function writeSummaryFile(summary, targets, path = SUMMARY_PATH) {
  assertOneDenominator(summary)
  assertMeasuredSuite(targets)
  writeFileSync(path, JSON.stringify(summary, null, 2) + '\n')
}

// ── The Markdown table ───────────────────────────────────────────────────────

const pct = (rate) => (rate === null ? '-' : `${rate.toFixed(1)}%`)

// A tier's divergence: its fails over its whole size, the same shape as the
// headline. The tier columns used to be correctness over what the tier
// attempted, so on a row sorted by divergence a rising tier figure was the
// target getting better and the two columns read in opposite directions.
// Through axesOf rather than restating it, so a tier carrying indeterminates
// withholds its figure exactly as the headline above it does. Restated, a row
// whose headline read "-" still printed three per-tier percentages.
const tierDivergence = (t) =>
  axesOf({ passed: t.p, failed: t.f, count: t.p + t.f + t.s + t.i, indeterminate: t.i }).divergence

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

  const suiteSize = summary.groundTruth.suiteSize
  // The baseline's counts are measured when the real-AWS lanes together span
  // the suite (buildSummary sets `counts` then) and pinned to the suite size
  // when they fall short, which is the one case where the pin is the honest
  // figure: every test was observed, just not in one run.
  const measured = summary.groundTruth.counts ?? null
  const axes = measured ? axesOf(measured) : { divergence: 0, coverage: 100 }
  const tier = (name) => (measured ? pct(tierDivergence(measured.tiers[name])) : '0.0%')
  const rate = measured ? summary.groundTruth.rate : 100

  const groundTruth = {
    slug: summary.groundTruth.slug,
    target: label(summary.groundTruth.slug),
    // The yardstick does not wear a grade (see BASELINE_LABEL). The site made
    // this call when it moved the baseline into a control strip above the board;
    // the table had been left grading it A+, which is the row a reader meets
    // first.
    grade: BASELINE_LABEL,
    tier1: tier('tier1'),
    tier2: tier('tier2'),
    tier3: tier('tier3'),
    total: pct(rate),
    divergence: pct(axes.divergence),
    coverage: pct(axes.coverage),
    divergenceValue: axes.divergence,
    coverageValue: axes.coverage,
    // Real DynamoDB is every region's own behaviour, so its row is not pinned
    // to one region the way a target's headline is.
    cohort: cohortCount(observedCount(summary), summary, rate),
    cohortLabel: 'all regions',
    passed: measured ? measured.passed : suiteSize,
    failed: measured ? measured.failed : 0,
    skipped: measured ? measured.skipped : 0,
    count: measured ? measured.count : suiteSize,
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
export function tableCaption(regions, groundTruth = null) {
  const list = (rs) => rs.map((r) => `\`${r}\``).join(', ')
  const sentences = [
    `Scored against real DynamoDB's recorded behaviour in each observed region ` +
      `(${list(regions.observed)}), at each target's best-matching region. ` +
      `**Divergence** is the share of the whole suite a target answers differently ` +
      `from real DynamoDB - the operations it implements and gets wrong. ` +
      `**Coverage** is the share it implements at all. They are reported apart ` +
      `because they carry opposite risks: a declined operation is discoverable in ` +
      `minutes, a wrong one in production. **Grade** is a reading of the pair: ` +
      `divergence sets the letter and coverage can only lower it, never raise it, ` +
      `under the versioned criteria in the ` +
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
  // A baseline row that is pinned rather than derived reads exactly like a
  // fully measured one: the same 0.0% over the same coverage. The site's
  // control strip discloses the difference and this table did not, so the one
  // surface generated by the script that knows about it was the one that said
  // nothing.
  const unobserved = Math.max(0, (groundTruth?.suiteSize ?? 0) - (groundTruth?.testsObserved ?? 0))
  if (groundTruth && unobserved > 0) {
    const missing = groundTruth.missingLanes ?? []
    sentences.push(
      `Real DynamoDB is measured in three passes and ${missing.length > 0 ? list(missing) : 'one or more'} ` +
        `${missing.length === 1 ? 'has' : 'have'} not reported, so its row is pinned to its last clean ` +
        `measurement rather than derived from this run: ${groundTruth.testsObserved} of ` +
        `${groundTruth.suiteSize} tests were re-observed and the other ${unobserved} ` +
        `${unobserved === 1 ? 'is' : 'are'} carried.`,
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
  return `${tableCaption(summary.regions, summary.groundTruth)}\n\n${body}`
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
          .filter(isTargetResultFile)
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

  const targets = readTargets(files)
  const summary = buildSummary(targets, loadScoringContext())
  const table = renderTable(summary)

  if (write) {
    publish(summary, targets)
    console.error(`Updated the results table in README.md and wrote ${SUMMARY_PATH}.`)
  } else {
    console.log(table)
  }
}

if (import.meta.url === `file://${process.argv[1]}`) main()
