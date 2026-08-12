import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { buildSummaryModel, cohortOf, regionLabel, regionCount, groupRegionsByDivergence, renderRegionGroups, controlObservation, controlProvenance, controlSplit } from "./summary.mjs";
import { GRADING_CRITERIA_EFFECTIVE, METRIC_CHANGED_ON } from "./scoring.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const raw = JSON.parse(readFileSync(join(here, "..", "test", "fixtures", "regions", "summary.json"), "utf8"));
const model = buildSummaryModel(raw);

test("a real summary builds an available model with the observed region set", () => {
  assert.equal(model.available, true);
  assert.equal(model.schemaVersion, 1);
  assert.ok(model.regions.observed.length > 3, "reads the full observed set, not a hardcoded few");
  assert.ok(model.groundTruth.rate === 100, "ground truth is 100 (earned self-agreement)");
});

test("region health separates observed, unresolved and dropped", () => {
  assert.ok(Array.isArray(model.regions.dropped));
  assert.ok(model.regions.dropped.includes("me-south-1"), "the dropped region is surfaced, not hidden");
  assert.ok(!model.regions.observed.includes("me-south-1"), "a dropped region is not also observed");
});

test("a target tied across every region reads as 'all regions', not a tie-break winner", () => {
  // Dynalite scores identically in all observed regions in the fixture.
  const dynalite = model.targets.dynalite;
  assert.equal(dynalite.label.kind, "all");
  assert.equal(regionLabel(dynalite.label), "all regions");
});

test("a target whose best cohort includes eu-west-2 anchors on eu-west-2", () => {
  // Dynoxide scores highest in a six-region cohort that includes eu-west-2.
  const dynoxide = model.targets.dynoxide;
  assert.equal(dynoxide.label.kind, "pinned-plus");
  assert.equal(dynoxide.label.regions.includes("eu-west-2"), true);
  assert.equal(regionLabel(dynoxide.label), `eu-west-2 + ${dynoxide.label.others} regions`);
  // The headline rate is the suite's, and equals the top of the per-region rates.
  const top = Math.max(...dynoxide.regions.map((r) => r.rate));
  assert.equal(dynoxide.rate, top);
});

test("regions are ordered by rate then name, and the cohort is flagged", () => {
  const dynoxide = model.targets.dynoxide;
  for (let i = 1; i < dynoxide.regions.length; i++) {
    assert.ok(dynoxide.regions[i - 1].rate >= dynoxide.regions[i].rate, "sorted by rate desc");
  }
  const euw2 = dynoxide.regions.find((r) => r.region === "eu-west-2");
  assert.equal(euw2.pinned, true);
  assert.equal(euw2.inCohort, true);
});

test("cohortOf names a non-pinned region only when it beats eu-west-2", () => {
  // No live instance today, so exercise the branch directly: an engine that
  // matches us-east-1 (which eu-west-2 disagrees with) scores higher there.
  const entries = [
    { region: "eu-west-2", rate: 90 },
    { region: "eu-central-1", rate: 90 },
    { region: "us-east-1", rate: 92 },
  ];
  const label = cohortOf(entries);
  assert.equal(label.kind, "beats-pinned");
  assert.equal(label.regions[0], "us-east-1");
  assert.equal(label.pinnedRate, 90);
  assert.equal(regionLabel(label), "us-east-1");
});

test("beats-pinned uses a count when several regions beat eu-west-2 (no arbitrary representative)", () => {
  const entries = [
    { region: "eu-west-2", rate: 90 },
    { region: "us-east-1", rate: 92 },
    { region: "us-east-2", rate: 92 },
  ];
  const label = cohortOf(entries);
  assert.equal(label.kind, "beats-pinned");
  assert.equal(regionLabel(label), "2 regions");
});

test("a single-region pinned cohort drops the '+ N' suffix", () => {
  const entries = [
    { region: "eu-west-2", rate: 95 },
    { region: "us-east-1", rate: 90 },
  ];
  const label = cohortOf(entries);
  assert.equal(label.kind, "pinned-plus");
  assert.equal(regionLabel(label), "eu-west-2");
});

test("indeterminate results are surfaced per region, not read as a disagreement", () => {
  const raw2 = {
    schemaVersion: 1,
    groundTruth: { slug: "dynamodb", rate: 100, runDate: "2026-07-16" },
    regions: { observed: ["eu-west-2", "ap-east-1"], unresolved: [], dropped: [], detail: {} },
    targets: {
      foo: {
        headline: { region: "eu-west-2", rate: 90 },
        regions: {
          "eu-west-2": { rate: 90, passed: 9, failed: 1, skipped: 0, indeterminate: 0, count: 10, tiers: { tier1: { p: 9, f: 1, s: 0, i: 0 }, tier2: { p: 0, f: 0, s: 0, i: 0 }, tier3: { p: 0, f: 0, s: 0, i: 0 } } },
          "ap-east-1": { rate: 90, passed: 9, failed: 1, skipped: 0, indeterminate: 2, count: 10, tiers: { tier1: { p: 9, f: 1, s: 0, i: 2 }, tier2: { p: 0, f: 0, s: 0, i: 0 }, tier3: { p: 0, f: 0, s: 0, i: 0 } } },
        },
      },
    },
  };
  const m = buildSummaryModel(raw2);
  const ap = m.targets.foo.regions.find((r) => r.region === "ap-east-1");
  assert.equal(ap.indeterminate, 2);
  assert.equal(ap.indeterminatePresent, true);
});

// The build checks the A+ premise from the names the suite publishes, so the
// model has to carry them through. It is a whitelist, and a field it does not
// name is a field the build cannot see - which reads as an artefact too old to
// check rather than as a passing check, but is wrong either way.
test("the model carries the failing test identities a zero-divergence row publishes", () => {
  const region = { rate: 90, passed: 9, failed: 1, skipped: 0, indeterminate: 0, count: 10, tiers: { tier1: { p: 9, f: 1, s: 0, i: 0 }, tier2: { p: 0, f: 0, s: 0, i: 0 }, tier3: { p: 0, f: 0, s: 0, i: 0 } } };
  const raw2 = {
    schemaVersion: 1,
    regions: { observed: ["eu-west-2"], unresolved: [], dropped: [], detail: {} },
    targets: {
      foo: {
        headline: { region: "eu-west-2", rate: 90 },
        regions: { "eu-west-2": region },
        regionFailures: { "eu-west-2": ["Some behaviour the registry splits on"] },
      },
    },
  };
  const m = buildSummaryModel(raw2);
  assert.deepEqual(m.targets.foo.regionFailures, { "eu-west-2": ["Some behaviour the registry splits on"] });
});

test("a target that published no identities carries null, not an empty object", () => {
  // An empty object would read as "checked, nothing failed"; null reads as
  // "the artefact did not say", which is the state the build has to treat as
  // uncheckable rather than clean.
  const raw2 = {
    schemaVersion: 1,
    regions: { observed: ["eu-west-2"], unresolved: [], dropped: [], detail: {} },
    targets: { foo: { headline: { region: "eu-west-2", rate: 90 }, regions: {} } },
  };
  assert.equal(buildSummaryModel(raw2).targets.foo.regionFailures, null);
});

test("a missing or wrong-schema payload degrades to unavailable rather than throwing", () => {
  assert.equal(buildSummaryModel(null).available, false);
  assert.equal(buildSummaryModel({ schemaVersion: 2, targets: {} }).available, false);
  assert.equal(buildSummaryModel(undefined).targets && Object.keys(buildSummaryModel(undefined).targets).length, 0);
});

// Best first now means lowest first: the drilldown is on divergence, so a
// descending sort would have put a target's worst regions at the top of the
// list under a heading that reads as its best.
test("groupRegionsByDivergence clusters regions into bands, lowest first", () => {
  const regions = [
    { region: "eu-west-2", divergenceValue: 0.6, pinned: true, inCohort: true },
    { region: "eu-central-1", divergenceValue: 0.6, inCohort: true },
    { region: "af-south-1", divergenceValue: 1.0 },
    { region: "ap-east-1", divergenceValue: 1.0 },
  ];
  const groups = groupRegionsByDivergence(regions);
  assert.equal(groups.length, 2);
  assert.deepEqual([groups[0].divergenceValue, groups[1].divergenceValue], [0.6, 1.0]);
  assert.deepEqual([groups[0].divergence, groups[1].divergence], ["0.6%", "1.0%"]);
  assert.equal(groups[0].count, 2);
  assert.equal(groups[1].count, 2);
});

test("renderRegionGroups shows divergence, and marks the baseline and indeterminate regions", () => {
  const html = renderRegionGroups([
    { region: "eu-west-2", divergenceValue: 10, divergence: "10.0%", pinned: true, inCohort: true, indeterminate: 0, indeterminatePresent: false },
    { region: "ap-east-1", divergenceValue: 12, divergence: "12.0%", pinned: false, inCohort: false, indeterminate: 3, indeterminatePresent: true },
  ]);
  assert.match(html, /eu-west-2/);
  assert.match(html, /baseline/);
  assert.match(html, /3 indeterminate/);
  assert.match(html, /10\.0%/);
  assert.match(html, /12\.0%/);
});

test("a region's divergence is failures over that region's whole count", () => {
  const m = buildSummaryModel(raw);
  const eu = m.targets.dynalite.regions.find((r) => r.region === "eu-west-2");
  assert.equal(eu.divergenceValue, (eu.failed / eu.count) * 100);
  assert.equal(eu.divergence, `${eu.divergenceValue.toFixed(1)}%`);
});

// The cohort label alone can't say how broad the cohort is,
// because "all regions" over six observed and over thirty-three read the same.
test("regionCount states the cohort size against the observed total", () => {
  assert.equal(regionCount(cohortOf([{ region: "eu-west-2", rate: 90 }, { region: "us-east-1", rate: 92 }])), "1 of 2");
  assert.equal(regionCount(cohortOf([{ region: "eu-west-2", rate: 90 }, { region: "us-east-1", rate: 90 }])), "2 of 2");
  assert.equal(regionCount(null), null);
  assert.equal(regionCount(cohortOf([])), null);
});

test("renderRegionGroups is empty for a target with no regions", () => {
  assert.equal(renderRegionGroups([]), "");
});

// The guard every other divergence figure carries. Without it this row reported
// 0.0% - reading as flawless - for a target that implemented nothing, while the
// tier directly beneath it in the same drilldown correctly reported "-".
test("a region where a target implements nothing has no divergence, not zero", () => {
  const m = buildSummaryModel({
    schemaVersion: 1,
    groundTruth: { slug: "dynamodb", rate: 100, runDate: "2026-07-29" },
    regions: { observed: ["eu-west-2"], unresolved: [], dropped: [], detail: {} },
    targets: {
      nothing: {
        headline: { region: "eu-west-2", rate: null },
        regions: {
          "eu-west-2": {
            rate: null, passed: 0, failed: 0, skipped: 10, indeterminate: 0, count: 10,
            tiers: { tier1: { p: 0, f: 0, s: 10, i: 0 }, tier2: { p: 0, f: 0, s: 0, i: 0 }, tier3: { p: 0, f: 0, s: 0, i: 0 } },
          },
        },
      },
    },
  });
  const r = m.targets.nothing.regions[0];
  assert.equal(r.divergenceValue, null);
  assert.equal(r.divergence, "-");
  // And it agrees with the tier beneath it rather than contradicting it.
  assert.equal(r.tiers.tier1.divergence, "-");
});

// The methodology page now states that coverage is a property of the target
// while divergence is a property of the target measured against a region. That
// rests on verdictsForRegion only ever swapping a pass for a fail: skips and
// indeterminates pass through untouched, so implemented and total are the same in
// every region. If that ever stops holding, the page is wrong and the row's
// counts stop reproducing its published divergence.
test("coverage is region-invariant, so a row's counts reproduce its divergence", () => {
  const m = buildSummaryModel(raw);
  for (const [slug, t] of Object.entries(m.targets)) {
    const implemented = new Set(t.regions.map((r) => r.passed + r.failed));
    const total = new Set(t.regions.map((r) => r.count));
    assert.equal(implemented.size, 1, `${slug}: implemented varies by region`);
    assert.equal(total.size, 1, `${slug}: suite size varies by region`);
    // And divergence does vary, which is the asymmetry the page describes.
    for (const r of t.regions) {
      if (r.divergenceValue == null) continue;
      assert.ok(Math.abs(r.failed / r.count * 100 - r.divergenceValue) < 1e-9, `${slug}/${r.region}`);
    }
  }
});

// ── The control strip's observation ─────────────────────────────────────────
//
// Real AWS is measured in three lanes and the baseline row is pinned at the
// full suite until all three publish. The strip read that row and so claimed
// "998 of 998 reproduced" for a run that recorded 981.

const gt = (over = {}) => ({
  slug: "dynamodb",
  suiteSize: 998,
  testsObserved: 981,
  derived: false,
  lanes: [{ name: "gating", runDate: "2026-08-09", tests: 981 }],
  missingLanes: ["integrations", "gsi"],
  ...over,
});

test("the strip counts what the lanes observed, not what the row is pinned at", () => {
  const obs = controlObservation(gt());
  assert.equal(obs.observed, 981);
  assert.equal(obs.suite, 998);
  assert.equal(obs.shortfall, 17);
});

test("a lane's capture date reaches the model the strip reads", () => {
  assert.deepEqual(controlObservation(gt()).dated, [
    { name: "gating", runDate: "2026-08-09", tests: 981 },
  ]);
});

test("the provenance names the lanes seen and the ones still missing", () => {
  const note = controlProvenance(controlObservation(gt()));
  assert.match(note, /the main run on 2026-08-09/);
  assert.match(note, /17 tests sit in the integrations and GSI passes, which have not reported yet/);
});

test("three lanes spanning the suite read as one dated observation each", () => {
  const obs = controlObservation(gt({
    testsObserved: 998,
    derived: true,
    missingLanes: [],
    lanes: [
      { name: "gating", runDate: "2026-08-09", tests: 981 },
      { name: "integrations", runDate: "2026-08-02", tests: 3 },
      { name: "gsi", runDate: "2026-07-30", tests: 14 },
    ],
  }));
  assert.equal(obs.shortfall, 0);
  const note = controlProvenance(obs);
  // Each capture dated separately: one date over three captures would read as
  // a single measurement.
  assert.match(note, /the main run on 2026-08-09/);
  assert.match(note, /integrations on 2026-08-02/);
  assert.match(note, /GSI on 2026-07-30/);
  assert.doesNotMatch(note, /have not reported/);
});

test("no ground truth degrades to no strip claim rather than throwing", () => {
  assert.equal(controlObservation(null), null);
  assert.equal(controlProvenance(null), "");
});


// The strip's compact split.

test("the split names the gating run by name, not by array position", () => {
  const obs = controlObservation(gt({
    lanes: [{ name: "gsi", runDate: "2026-07-30", tests: 14 }, { name: "gating", runDate: "2026-08-09", tests: 981 }],
    missingLanes: ["integrations"],
  }));
  assert.equal(obs.mainRun.name, "gating");
  assert.equal(obs.mainRun.runDate, "2026-08-09");
});

test("the split counts passes in words and carries no date of its own", () => {
  assert.equal(controlSplit(controlObservation(gt())), "981 in that run, 17 in two slower passes");
  assert.doesNotMatch(controlSplit(controlObservation(gt())), /2026/);
});

test("one outstanding pass reads singular", () => {
  const obs = controlObservation(gt({ missingLanes: ["gsi"] }));
  assert.match(controlSplit(obs), /in one slower pass$/);
});

test("nothing outstanding means no split line", () => {
  assert.equal(controlSplit(controlObservation(gt({ testsObserved: 998, derived: true, missingLanes: [] }))), "");
  assert.equal(controlSplit(null), "");
});


test("the metric change and the criteria take effect on the same day", () => {
  // Set apart, the runs in between are published under the retired metric and
  // carry no notice saying so, which is the disclosure this release is about.
  assert.equal(METRIC_CHANGED_ON, GRADING_CRITERIA_EFFECTIVE);
});
