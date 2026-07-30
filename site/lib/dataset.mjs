// Neutral, machine-readable views of the conformance model.
//
// These shape the same build-time model the HTML renders from into JSON a third
// party (or an agent) can consume instead of scraping the pages. Every target,
// including the live-AWS baseline, gets the identical schema: the data exposes
// the numbers and lets the consumer rank, it never editorialises or privileges
// one target's row. Like everything else on the site these are derived from the
// suite's own results at build time, so they can't drift from what's on screen.

import {
  BASELINE_GRADE,
  CAPABILITIES,
  COVERAGE_CAPS,
  GRADE_BANDS,
  GRADING_VERSION,
  asPct,
  configurationOf,
  gradeOf,
  isSelfMaintained,
  isVariant,
  projectOf,
} from "./scoring.mjs";

// The two published figures for any {passed, failed, total}-shaped tally, on the
// same terms and with the same null-guard as everywhere else: divergence is
// undefined when nothing was implemented, coverage is undefined when nothing ran.
// A tier or area names its denominator `total`; a region row names it `count`.
const implementedOf = (a) => (a.passed ?? 0) + (a.failed ?? 0);
const totalOf = (a) => a.total ?? a.count ?? 0;
const divergenceOf = (a) =>
  !totalOf(a) || implementedOf(a) === 0 ? null : ((a.failed ?? 0) / totalOf(a)) * 100;
const coverageOf = (a) => (!totalOf(a) ? null : (implementedOf(a) / totalOf(a)) * 100);

// 2 adds the per-region dimension (each target's headline region and, on the
// latest endpoint, its full per-region breakdown and the run's region health),
// and corrects the baseline's region from the old single pin to "all".
//
// 3 converts tier figures from correctness to divergence and coverage, the same
// two axes as the headline, and renames the whole-suite correctness percentage
// from `total` to `correctness`. Three changes in it are breaking: a tier's
// `pct` and `value` are gone; `total` now means only the raw test count it also
// meant before; and `movement.state` reports `improved`/`regressed` where it
// reported `up`/`down`, since a rise is now the bad direction. A consumer
// reading any of the three as before would have silently inverted.
//
// 4 adds the letter grade: a `grade` object on every target row and the
// grading criteria in the envelope's metrics, versioned separately from this
// schema (a criteria change regrades targets that changed nothing, so it
// carries its own version). Additive - a schema 3 consumer keeps working.
export const DATA_SCHEMA_VERSION = 4;

// Tier metadata, surfaced so a consumer doesn't have to hard-code the names.
export const TIERS = [
  { key: "tier1", name: "Core", description: "The operations roughly 90% of DynamoDB users rely on: CRUD, queries, scans, batch operations, GSIs, UpdateTable." },
  { key: "tier2", name: "Complete", description: "Documented but less common features: transactions, PartiQL, LSIs, TTL, streams, tags." },
  { key: "tier3", name: "Strict", description: "Validation ordering, error behaviour, limits, and legacy API shapes." },
];

const groupByCapability = Object.fromEntries(CAPABILITIES.map((c) => [c.key, c.group]));

// Tier scores as a {tier1, tier2, tier3} map, one shape whether the source row
// carries a tier or not (a missing tier is null, never absent). Each tier
// carries the same two axes as the headline, so a tier figure and the figure
// above it move in the same direction.
function tierScores(tiers) {
  const out = {};
  for (const { key } of TIERS) {
    const t = tiers?.[key];
    out[key] = t
      ? {
          divergence: { pct: t.divergence, value: t.divergenceValue },
          coverage: { pct: t.coverage, value: t.coverageValue },
          correctness: { pct: t.correctness, value: t.correctnessValue },
          passed: t.passed,
          failed: t.failed,
          skipped: t.skipped,
          total: t.total,
        }
      : null;
  }
  return out;
}

// The headline region a row's total was earned in, compacted for every row
// (latest and historical). `kind` says how the headline relates to the pinned
// baseline region: "all" (ties everywhere), "pinned-plus" (eu-west-2 is in the
// best cohort) or "beats-pinned" (the best cohort excludes eu-west-2, so the
// target matches a region eu-west-2 disagrees with). null before the per-region
// data begins. The full per-region breakdown is on the latest endpoint.
function regionSummary(row) {
  const label = row.regionLabel;
  if (!label) return null;
  return {
    kind: label.kind,
    // The region this row's figures were measured against. Every figure on the
    // row comes from this one region, so a consumer can name the measurement
    // rather than inferring it from the cohort.
    headline: row.headlineRegion ?? null,
    cohort: label.regions ?? [],
    // How many observed regions the headline was measured against, out of how
    // many were observed at all. The kind alone doesn't say: "all" over six
    // regions and "all" over thirty-three are the same word.
    cohortSize: (label.regions ?? []).length,
    observed: label.observed ?? null,
    pinned: label.pinned ?? "eu-west-2",
    beatsPinned: label.kind === "beats-pinned",
    // The worst observed region's divergence - the figure behind the row's
    // "up to X% in the other N" clause, published so an agent can read what
    // a human sees on the card. Null when the target has no regional spread
    // (or the row predates the per-region overlay).
    worst:
      row.divergenceWorst == null
        ? null
        : { pct: asPct(row.divergenceWorst), value: row.divergenceWorst },
  };
}

// One standings row -> the neutral, identical-schema target object every
// endpoint shares. Divergence and coverage travel together by design, so a
// target that is right about a narrow surface cannot read as broad conformance.
function targetRow(row) {
  const baseline = row.slug === "dynamodb";
  return {
    slug: row.slug,
    display: row.display,
    version: row.version,
    baseline,
    // Conflict-of-interest disclosure: maintained by the board's author. A
    // static fact, derived from the slug, so it can't go stale.
    maintainedByAuthor: isSelfMaintained(row.slug),
    carried: !!row.carried,
    reTested: !!row.reTested,
    // The run that actually measured this row - the "last measured" date a
    // carried row shows on the board. Equals the run's own date unless the
    // row was carried forward.
    runDate: row.runDate ?? null,
    // The two published axes, plus the legacy percentage under a name that
    // says what it is. It used to be `total`, which also names the raw test
    // count in `counts` below, so the same word meant a count in one place and
    // correctness over implemented operations in another.
    divergence: { pct: row.divergence, value: row.divergenceValue },
    coverage: { pct: row.coverage, value: row.coverageValue },
    correctness: { pct: row.total, value: row.totalValue },
    // The letter grade, derived here from the two values above so it cannot
    // disagree with them. The criteria are in the envelope's metrics.grade.
    // The baseline carries no letter: it is what a grade measures distance
    // from, and the pages, the results table and the badges all decline to
    // grade it, so the endpoints would be the one surface that did.
    grade: baseline ? BASELINE_GRADE : gradeOf(row.divergenceValue, row.coverageValue),
    // Which project this target belongs to, and which configuration it is.
    // Without it a consumer has to infer from the display name that
    // "Dynoxide (wasm)" is a build of "Dynoxide" rather than a rival.
    project: projectOf(row.slug),
    configuration: configurationOf(row.slug),
    isVariant: isVariant(row.slug),
    counts: { passed: row.passed, failed: row.failed, skipped: row.skipped, implemented: row.implemented, total: row.count },
    tiers: tierScores(row.tiers),
    region: regionSummary(row),
    movement: row.movement
      ? {
          state: row.movement.state,
          delta: row.movement.delta,
          deltaLabel: row.movement.deltaLabel,
          label: row.movement.label,
          // Present only when the letter changed between this run and the
          // previous one the target was tested in; null otherwise.
          gradeChange: row.movement.grade ?? null,
        }
      : null,
  };
}

// A target's per-region breakdown for the latest endpoint: every observed
// region's rate, tier split and counts, with eu-west-2 flagged as the historical
// baseline and each region flagged for whether it's in the best-scoring cohort.
function regionTier(t) {
  return t
    ? {
        divergence: { pct: t.divergence, value: t.divergenceValue },
        coverage: { pct: t.coverage, value: t.coverageValue },
        correctness: { pct: t.correctness, value: t.correctnessValue },
        passed: t.passed,
        failed: t.failed,
        skipped: t.skipped,
        indeterminate: t.indeterminate,
        total: t.total,
      }
    : null;
}
function regionDetail(pt) {
  return (pt.regions || []).map((r) => ({
    region: r.region,
    rate: r.rate,
    divergence: { pct: r.divergence, value: r.divergenceValue },
    // Coverage is invariant across a target's regions (only pass/fail flip), but
    // it is published per region anyway so a region row carries the same pair as
    // every other level of this schema rather than half of it.
    coverage: { pct: asPct(coverageOf(r)), value: coverageOf(r) },
    pinned: !!r.pinned,
    inCohort: !!r.inCohort,
    passed: r.passed,
    failed: r.failed,
    skipped: r.skipped,
    indeterminate: r.indeterminate,
    total: r.count,
    tiers: { tier1: regionTier(r.tiers?.tier1), tier2: regionTier(r.tiers?.tier2), tier3: regionTier(r.tiers?.tier3) },
  }));
}

// What each published metric is and which way is good. Carried in the envelope so
// a consumer holding one file can tell that lower divergence is better without
// fetching a prose page: the direction reversed in schema 3, and a consumer that
// re-derived its own ranking on the old assumption would have inverted silently
// with nothing in the payload to catch it.
const METRICS = {
  divergence: {
    formula: "failed / total",
    direction: "lower_is_better",
    description:
      "The share of the whole suite the target answers differently from real DynamoDB. Skips and indeterminates are in the denominator, so it cannot be lowered by shrinking what is attempted without the same fall showing in coverage.",
  },
  coverage: {
    formula: "(passed + failed) / total",
    direction: "higher_is_better",
    description:
      "The share of the suite's tests the target implements at all. Weighted by test count, not by a count of features.",
  },
  correctness: {
    formula: "passed / (passed + failed)",
    direction: "higher_is_better",
    legacy: true,
    description:
      "The pass rate over implemented operations, which the board led with before schema 3. Retained for consumers that already read it; its denominator shrinks as a target attempts less.",
  },
  grade: {
    direction: "A+ is best, F is worst",
    gradingVersion: GRADING_VERSION,
    description:
      "A reading of the divergence and coverage pair, never a blend of it: divergence sets the letter and low coverage can only cap it. Recomputable from the two values with the criteria below; a criteria change bumps gradingVersion. The bands and caps read both values rounded to one decimal place (the precision the board publishes); only the A+ test reads the raw divergence, because A+ means exactly zero failing tests, not a figure that rounds to 0.0. When either value is null the grade is null ('not scored') - check for null before applying the bands, or a null divergence will coerce and mis-grade.",
    // The criteria themselves, so a consumer can regrade without a prose page:
    // the letter for a divergence below each band's bound, A+ for exactly
    // zero divergence (zero failing tests, not a rounded 0.0%), and the
    // coverage caps, which apply to every letter and stop at D.
    bands: GRADE_BANDS,
    aPlus: { divergence: 0, exact: true },
    coverageCaps: COVERAGE_CAPS,
  },
};

// Self-describing header shared by every endpoint: schema version, provenance,
// licence and the baseline's identity, so any single file stands on its own.
function envelope(conformance, site) {
  return {
    schemaVersion: DATA_SCHEMA_VERSION,
    metrics: METRICS,
    source: site.url,
    repository: site.sourceRepo,
    license: site.dataLicense,
    licenseName: site.dataLicenseName,
    attribution: site.dataAttribution,
    ...(conformance.generatedAt ? { generatedAt: conformance.generatedAt } : {}),
    baseline: {
      slug: "dynamodb",
      region: "all",
      description: "Live AWS DynamoDB. The ground truth: it agrees with itself by definition, so it diverges nowhere in every region, and it is what every other target is measured against.",
    },
  };
}

// Discovery manifest: a neutral map of the data surface, the tier and capability
// vocabularies, and where the documentation lives.
export function buildIndex(conformance, site) {
  const { latest, runs = [] } = conformance;
  return {
    ...envelope(conformance, site),
    name: "DynamoDB emulator conformance results",
    description:
      "Divergence and coverage for DynamoDB-compatible emulators, overall and per tier, measured against live AWS DynamoDB and recorded run over run. Divergence is failed / total and coverage is implemented / total, reported apart and never summed. Identical schema for every target, including the live-AWS baseline. Use these endpoints instead of scraping the pages.",
    documentation: site.url + "/for-agents",
    latestRun: latest?.id ?? null,
    runCount: runs.length,
    suiteSize: latest?.suiteSize ?? null,
    tiers: TIERS,
    capabilities: CAPABILITIES.map((c) => ({ key: c.key, label: c.label, group: c.group })),
    regions: {
      pinned: "eu-west-2",
      description:
        "Each target is scored against every observed region, and its headline is its best-matching region. A target's `region` field says how that headline relates to the pinned region (all, pinned-plus or beats-pinned) and lists the cohort it was measured against; the latest endpoint carries every region's divergence and tier split per target, plus the run's region health.",
      health: [
        { key: "observed", description: "Completed this sweep and counts towards scores." },
        { key: "unresolved", description: "Missed this sweep but still trusted." },
        { key: "dropped", description: "Missed twice; out of scoring until it returns." },
      ],
    },
    endpoints: [
      { name: "Latest run", format: "application/json", url: site.url + "/data/latest.json", description: "Current standings: per target, divergence and coverage overall and per tier, capabilities, operation areas, and the full per-region breakdown." },
      { name: "All runs", format: "application/json", url: site.url + "/data/runs.json", description: "Full history: every run's per-target divergence and coverage, overall and per tier, plus movement and headline region." },
      { name: "Runs feed", format: "application/atom+xml", url: site.url + "/feed.xml", description: "Atom feed, one entry per run." },
    ],
  };
}

// The latest run in full: per target, divergence and coverage per tier, plus the per-capability
// and per-operation-area state the matrix and capability grid are built from.
export function buildLatest(conformance, site) {
  const { latest, perTarget = {} } = conformance;
  if (!latest) return { ...envelope(conformance, site), run: null, tiers: TIERS, regionHealth: null, targets: [] };
  return {
    ...envelope(conformance, site),
    run: { id: latest.id, date: latest.date, suiteSize: latest.suiteSize, emulatorCount: latest.emulatorCount },
    tiers: TIERS,
    // Which regions scored this run: observed count towards scores, unresolved
    // missed this sweep but are still trusted, dropped are out of scoring. null
    // before the per-region data begins.
    regionHealth: conformance.regionHealth ?? null,
    targets: latest.standings.map((row) => {
      const pt = perTarget[row.slug] || {};
      return {
        ...targetRow(row),
        capabilities: (pt.capabilities || []).map((c) => ({
          key: c.key, label: c.label, group: groupByCapability[c.key] ?? null, state: c.state,
          passed: c.passed, failed: c.failed, skipped: c.skipped, total: c.total,
        })),
        // Per-operation areas carry the computed pair too. Every other level of
        // the schema pre-computes it, so leaving areas as raw counts alone made
        // a consumer reimplement the one formula the schema otherwise gives them.
        areas: (pt.areas || []).map((a) => ({
          key: a.key, tier: a.tier, group: a.group, state: a.state,
          divergence: { pct: asPct(divergenceOf(a)), value: divergenceOf(a) },
          coverage: { pct: asPct(coverageOf(a)), value: coverageOf(a) },
          passed: a.passed, failed: a.failed, skipped: a.skipped, total: a.total,
        })),
        regions: regionDetail(pt),
      };
    }),
  };
}

// The full history: every run's standings, newest first. Tier divergence, coverage
// and movement per target; the per-capability and per-area detail lives on the
// latest endpoint, since the model only carries it for the latest snapshot.
export function buildRuns(conformance, site) {
  const { runs = [], latest } = conformance;
  return {
    ...envelope(conformance, site),
    tiers: TIERS,
    latestRun: latest?.id ?? null,
    runs: runs.map((r) => ({
      id: r.id,
      date: r.date,
      sha: r.sha,
      suiteSize: r.suiteSize,
      emulatorCount: r.emulatorCount,
      targets: r.standings.map(targetRow),
    })),
  };
}
