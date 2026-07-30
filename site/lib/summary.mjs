// Build the per-region model from the suite's results/summary.json.
//
// summary.json (schemaVersion 1) is the suite's 2.0.0 addition: every target
// scored against every observed region, its headline the best-matching region.
// This module turns one raw summary into the shape the templates and the
// history join consume, and it owns the one presentation rule that the raw data
// can't express on its own: how to name the region a headline matched.
//
// The region label follows the "honest cohort" rule. The suite's own Region
// column crowns a single tie-break winner even when a target scores identically
// everywhere, which reads as "conformant to af-south-1" when the truth is
// "conformant to every region equally". Instead:
//   - all         every observed region ties at the top rate -> "all regions"
//   - pinned-plus eu-west-2 is in the top cohort              -> "eu-west-2 + N regions"
//   - beats-pinned the top cohort excludes eu-west-2          -> name it, and flag
//                 that the target beats eu-west-2 there (the release's reason to
//                 exist; no live instances today, but the display must handle it)
// This never disagrees with the suite on a figure - the rate is the suite's
// headline rate - only on how the matched region is described.

import { asPct, pct } from "./scoring.mjs";

const PINNED = "eu-west-2";

// Normalise the summary's compact tier shape { p, f, s, i } into the per-tier
// counts plus divergence and coverage, matching how scoreEmulator reports tiers
// so the drilldown and the standings read the same. Correctness is kept under
// its own name for anyone who still wants it.
//
// The denominator counts indeterminates, which this used to leave out of
// `total` while scoreEmulator counted them. A tier's divergence has to be over
// the same denominator on both sides or the drilldown would disagree with the
// tier bar above it on the same page.
function tierOf(t) {
  const passed = t?.p ?? 0;
  const failed = t?.f ?? 0;
  const skipped = t?.s ?? 0;
  const indeterminate = t?.i ?? 0;
  const total = passed + failed + skipped + indeterminate;
  const implemented = passed + failed;
  const divergenceValue = total === 0 || implemented === 0 ? null : (failed / total) * 100;
  const coverageValue = total === 0 ? null : (implemented / total) * 100;
  return {
    passed,
    failed,
    skipped,
    indeterminate,
    total,
    divergenceValue,
    divergence: asPct(divergenceValue),
    coverageValue,
    coverage: asPct(coverageValue),
    correctness: pct(passed, failed),
    correctnessValue: passed + failed === 0 ? null : (passed / (passed + failed)) * 100,
  };
}

// One region's entry for a target, from summary.targets[slug].regions[region].
// `rate` stays as the suite published it (correctness in that region); the
// divergence beside it is what the drilldown now shows.
function regionEntry(region, r) {
  const count = r?.count ?? 0;
  const failed = r?.failed ?? 0;
  const implemented = (r?.passed ?? 0) + failed;
  // The same guard every other divergence figure carries: diverging nowhere
  // because you attempted nothing is the absence of a score, not a good one.
  // Without it this row read 0.0% while the tier directly beneath it, which does
  // guard, read "-" for the identical target.
  const divergenceValue = count === 0 || implemented === 0 ? null : (failed / count) * 100;
  return {
    region,
    rate: r?.rate ?? null,
    divergenceValue,
    divergence: asPct(divergenceValue),
    passed: r?.passed ?? 0,
    failed: r?.failed ?? 0,
    skipped: r?.skipped ?? 0,
    indeterminate: r?.indeterminate ?? 0,
    count: r?.count ?? 0,
    tiers: {
      tier1: tierOf(r?.tiers?.tier1),
      tier2: tierOf(r?.tiers?.tier2),
      tier3: tierOf(r?.tiers?.tier3),
    },
  };
}

// Classify how a target's headline region should read, from its per-region
// rates. `entries` is every observed region's entry; `pinned` is the historical
// baseline region (eu-west-2). Returns the structured label; regionLabel() turns
// it into display text. See the module header for the three kinds.
export function cohortOf(entries, pinned = PINNED) {
  const rated = entries.filter((e) => e.rate != null);
  if (rated.length === 0) return { kind: "none", regions: [], rate: null, others: 0, observed: 0 };

  const top = Math.max(...rated.map((e) => e.rate));
  const cohort = rated
    .filter((e) => e.rate === top)
    .map((e) => e.region)
    .sort();

  // Every observed region ties at the top: the target conforms equally
  // everywhere, so no region is worth singling out.
  if (cohort.length === rated.length) {
    return { kind: "all", regions: cohort, rate: top, others: cohort.length - 1, observed: rated.length };
  }
  // eu-west-2 is among the best: anchor on it (the reader knows it as the
  // historical baseline) and count the rest of the cohort.
  if (cohort.includes(pinned)) {
    return { kind: "pinned-plus", regions: cohort, rate: top, others: cohort.length - 1, pinned, observed: rated.length };
  }
  // The best cohort excludes eu-west-2: the target genuinely matches a real
  // region eu-west-2 disagrees with. Name it and carry the pinned rate so the
  // display can show the gap.
  const pinnedEntry = rated.find((e) => e.region === pinned);
  return { kind: "beats-pinned", regions: cohort, rate: top, others: cohort.length - 1, pinned, pinnedRate: pinnedEntry?.rate ?? null, observed: rated.length };
}

// Display text for a cohort label. Kept here (not in a template) so the phrasing
// is unit-tested and identical across the standings, run and target pages.
export function regionLabel(label) {
  if (!label || label.kind === "none") return "-";
  switch (label.kind) {
    case "all":
      return "all regions";
    case "pinned-plus":
      return label.others === 0 ? label.pinned : `${label.pinned} + ${label.others} region${label.others === 1 ? "" : "s"}`;
    case "beats-pinned":
      // One region is named; a larger beating cohort is a count, since no single
      // region in it is more representative than the rest.
      return label.regions.length === 1 ? label.regions[0] : `${label.regions.length} regions`;
    default:
      return "-";
  }
}

// How many of the observed regions a target matches, as a count rather than a
// label. "all regions" reads as breadth when it can equally mean no region can
// tell the target apart - a count next to a divergence figure cannot. The
// cohort label stays alongside it, naming which regions those are.
export function regionCount(label) {
  if (!label || label.kind === "none" || !label.observed) return null;
  return `${label.regions.length} of ${label.observed}`;
}

// Per-target model: every region's entry (rate desc, then name), the headline
// rate and its cohort label, and flags the drilldown reads.
function targetOf(slug, t, pinned) {
  // Best first, which on divergence means lowest first. A region with no
  // figure sorts last rather than ahead of every measured one.
  const entries = Object.entries(t?.regions ?? {})
    .map(([region, r]) => regionEntry(region, r))
    .sort((a, b) => (a.divergenceValue ?? Infinity) - (b.divergenceValue ?? Infinity) || a.region.localeCompare(b.region));

  const label = cohortOf(entries, pinned);
  const cohortSet = new Set(label.regions);
  const regions = entries.map((e) => ({
    ...e,
    pinned: e.region === pinned,
    inCohort: cohortSet.has(e.region),
    indeterminatePresent: e.indeterminate > 0,
  }));

  // How much the regions disagree about this target's divergence. Carried as a
  // spread on the figure itself rather than a count of matching regions: a
  // count is not a quality measure and was read as one, since a target equally
  // wrong in all 33 regions counted higher than one perfect in 6 and
  // near-perfect in the rest.
  const perRegion = regions.map((e) => e.divergenceValue).filter((v) => v != null);

  return {
    slug,
    // The suite's headline rate is authoritative; cohortOf derives the label
    // from the same per-region data, so the two agree by construction.
    rate: t?.headline?.rate ?? label.rate,
    suiteHeadlineRegion: t?.headline?.region ?? null,
    label,
    regions,
    divergenceBest: perRegion.length ? Math.min(...perRegion) : null,
    divergenceWorst: perRegion.length ? Math.max(...perRegion) : null,
    version: t?.version ?? null,
    runDate: t?.runDate ?? null,
  };
}

// Group a target's region entries by divergence (lowest, i.e. best, first) for
// the drilldown. The 33-region set clusters into a handful of figures, so
// grouping is far more scannable than a flat list, and it makes a genuine split
// obvious.
export function groupRegionsByDivergence(regions) {
  const byValue = new Map();
  for (const r of regions) {
    if (r.divergenceValue == null) continue;
    if (!byValue.has(r.divergenceValue)) byValue.set(r.divergenceValue, []);
    byValue.get(r.divergenceValue).push(r);
  }
  return [...byValue.entries()]
    .sort((a, b) => a[0] - b[0])
    .map(([divergenceValue, rs]) => ({
      divergenceValue,
      divergence: asPct(divergenceValue),
      count: rs.length,
      regions: rs,
    }));
}

const esc = (s) =>
  String(s).replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);

// Render the per-region drilldown for a target as grouped rate bands. WebC can't
// nest a webc:for over a property of an outer loop variable (the groups, then
// the regions in each), so the grouped view is built here as HTML, the same way
// the support matrix renders its cards.
export function renderRegionGroups(regions) {
  const groups = groupRegionsByDivergence(regions);
  if (groups.length === 0) return "";
  return groups
    .map((g) => {
      const chips = g.regions
        .map((r) => {
          const marks = [];
          if (r.pinned) marks.push("baseline");
          if (r.indeterminatePresent) marks.push(`${r.indeterminate} indeterminate`);
          const suffix = marks.length ? ` <span class="text-zinc-400 dark:text-zinc-500">· ${esc(marks.join(" · "))}</span>` : "";
          const strong = r.pinned || r.inCohort ? "text-zinc-700 dark:text-zinc-200 font-medium" : "text-zinc-500 dark:text-zinc-400";
          return `<span class="font-mono ${strong}">${esc(r.region)}${suffix}</span>`;
        })
        .join("");
      return `
      <div class="rounded-xl border border-zinc-200 dark:border-white/10 bg-zinc-50/70 dark:bg-white/[0.03] p-4">
        <div class="flex items-baseline justify-between mb-2">
          <span class="font-mono font-bold tnum text-zinc-900 dark:text-zinc-100">${esc(g.divergence)}</span>
          <span class="text-xs text-zinc-500 dark:text-zinc-400 tnum">${g.count} ${g.count === 1 ? "region" : "regions"}</span>
        </div>
        <div class="flex flex-wrap gap-x-3 gap-y-1 text-xs">${chips}</div>
      </div>`;
    })
    .join("");
}

// Turn a raw summary.json into the per-region model, or an unavailable marker
// when the payload is missing or not the schema we understand. Never throws: an
// absent or malformed summary degrades the site to its eu-west-2-only story
// rather than failing the build.
export function buildSummaryModel(raw, { pinned = PINNED } = {}) {
  if (!raw || raw.schemaVersion !== 1 || !raw.targets) {
    return { available: false, targets: {}, regions: { observed: [], unresolved: [], dropped: [], detail: {} } };
  }

  const targets = {};
  for (const [slug, t] of Object.entries(raw.targets)) targets[slug] = targetOf(slug, t, pinned);

  return {
    available: true,
    schemaVersion: raw.schemaVersion,
    groundTruth: raw.groundTruth ?? null,
    runDate: raw.groundTruth?.runDate ?? null,
    regions: {
      observed: raw.regions?.observed ?? [],
      unresolved: raw.regions?.unresolved ?? [],
      dropped: raw.regions?.dropped ?? [],
      detail: raw.regions?.detail ?? {},
    },
    targets,
    pinned,
  };
}
