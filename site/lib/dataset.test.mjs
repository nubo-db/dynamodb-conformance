import { test } from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { CAPABILITIES } from "./scoring.mjs";
import { buildIndex, buildLatest, buildRuns, TIERS, DATA_SCHEMA_VERSION } from "./dataset.mjs";

// The committed history fallback is a real, fully-built model, so it doubles as
// a fixture for the data endpoints without re-deriving one here.
const MODEL_PATH = join(dirname(fileURLToPath(import.meta.url)), "..", "data", "conformance-history.json");
const model = JSON.parse(await readFile(MODEL_PATH, "utf8"));

const site = {
  url: "https://paritysuite.org",
  sourceRepo: "https://github.com/paritysuite/dynamodb-conformance",
  dataLicense: "https://creativecommons.org/licenses/by/4.0/",
  dataLicenseName: "CC BY 4.0",
  dataAttribution: "paritysuite.org",
};

const sortedKeys = (obj) => Object.keys(obj).sort();

test("every endpoint carries the self-describing envelope", () => {
  for (const doc of [buildIndex(model, site), buildLatest(model, site), buildRuns(model, site)]) {
    assert.equal(doc.schemaVersion, DATA_SCHEMA_VERSION);
    assert.equal(doc.source, site.url);
    assert.equal(doc.license, site.dataLicense);
    assert.equal(doc.licenseName, "CC BY 4.0");
    assert.equal(doc.attribution, "paritysuite.org");
    assert.equal(doc.baseline.slug, "dynamodb");
    assert.equal(doc.baseline.region, "all");
  }
});

test("latest exposes every target on an identical schema, baseline included", () => {
  const latest = buildLatest(model, site);
  assert.ok(latest.targets.length >= 2);

  const shape = sortedKeys(latest.targets[0]);
  for (const t of latest.targets) {
    assert.deepEqual(sortedKeys(t), shape, `target ${t.slug} diverges from the shared schema`);
    assert.deepEqual(Object.keys(t.tiers).sort(), ["tier1", "tier2", "tier3"]);
    assert.equal(t.capabilities.length, CAPABILITIES.length);
    // The two published axes always travel together, and the legacy
    // correctness percentage sits beside them under its own name rather than
    // sharing "total" with the raw count in `counts`.
    assert.ok("divergence" in t && "coverage" in t);
    assert.ok("correctness" in t && !("total" in t));
    assert.equal(typeof t.counts.total, "number");
    // Every tier reports on the same axes as the headline, so a tier figure
    // and the figure above it can't run in opposite directions.
    for (const tier of Object.values(t.tiers)) {
      if (!tier) continue;
      assert.ok("divergence" in tier && "coverage" in tier && "correctness" in tier);
      assert.ok(!("pct" in tier) && !("value" in tier));
    }
  }

  const baselines = latest.targets.filter((t) => t.baseline);
  assert.equal(baselines.length, 1, "exactly one baseline row");
  assert.equal(baselines[0].slug, "dynamodb");
});

test("runs endpoint mirrors the model's runs, newest first, shared target schema", () => {
  const runs = buildRuns(model, site);
  assert.equal(runs.runs.length, model.runs.length);
  assert.equal(runs.runs[0].id, model.latest.id);
  assert.equal(runs.latestRun, model.latest.id);

  const shape = sortedKeys(runs.runs[0].targets[0]);
  for (const run of runs.runs) {
    for (const t of run.targets) {
      assert.deepEqual(sortedKeys(t), shape, `run ${run.id} target ${t.slug} diverges`);
    }
  }
});

test("index documents the tier and capability vocabularies and the endpoints", () => {
  const index = buildIndex(model, site);
  assert.equal(index.tiers.length, 3);
  assert.deepEqual(index.tiers.map((t) => t.key), TIERS.map((t) => t.key));
  assert.equal(index.capabilities.length, CAPABILITIES.length);
  assert.equal(index.latestRun, model.latest.id);
  assert.equal(index.runCount, model.runs.length);
  assert.equal(index.documentation, site.url + "/for-agents");

  const urls = index.endpoints.map((e) => e.url);
  assert.ok(urls.includes(site.url + "/data/latest.json"));
  assert.ok(urls.includes(site.url + "/data/runs.json"));
  assert.ok(urls.includes(site.url + "/feed.xml"));
});

// 3 is the tier conversion plus the `total` -> `correctness` rename. Both
// change what a field means rather than adding one, so a consumer pinned to 2
// has to be told rather than left to read an inverted figure. 4 adds the
// letter grade and its criteria, additively.
test("schema version 4 reflects the grade addition", () => {
  assert.equal(DATA_SCHEMA_VERSION, 4);
});

// The grade travels with the two values it reads, and the criteria travel in
// the envelope, so a consumer can regrade and check the letter it was handed.
// This is the methodology's testability claim made literal: every published
// letter is recomputed here from the envelope's own criteria - a separate
// implementation from gradeOf - and must match. No graded target is graded by
// different rules. The baseline is the one row with no letter to reproduce, and
// it is asserted to be that rather than skipped, so an ungraded row can only
// ever be the yardstick and never a target that quietly lost its grade.
test("every published letter is reproducible from the envelope's criteria alone", () => {
  const latest = buildLatest(model, site);
  const criteria = latest.metrics.grade;
  assert.ok(criteria.bands.length > 0);
  assert.equal(criteria.gradingVersion, 1);
  const ORDER = ["A+", "A", "B", "C", "D", "F"];
  const regrade = (d, c) => {
    if (d == null || c == null) return { letter: null, capAt: null };
    // The bands read the figures at the published one-decimal precision; only
    // the A+ gate reads raw values, on both axes. Mirrors
    // metrics.grade.description, deliberately written from the published
    // criteria rather than by importing gradeOf.
    const d1 = Number(d.toFixed(1));
    const c1 = Number(c.toFixed(1));
    const band = (x) => criteria.bands.find((b) => x < b.under)?.letter ?? "F";
    const effective = Number((d1 + (100 - c1) / criteria.coverageDivisor).toFixed(1));
    const perfect =
      d === criteria.aPlus.divergence && c === criteria.aPlus.coverage;
    const base = d === criteria.aPlus.divergence ? "A+" : band(d1);
    const letter = perfect ? "A+" : band(effective);
    // The ceiling is published only where coverage lowered the letter.
    const capped = letter !== base;
    return { letter, capAt: capped ? letter : null };
  };

  let baselines = 0;
  for (const t of latest.targets) {
    assert.ok("grade" in t, `${t.slug} missing grade`);
    if (t.baseline) {
      baselines++;
      assert.equal(t.grade.letter, null, "the baseline must carry no letter");
      assert.equal(t.grade.qualifier, "baseline");
      assert.ok(t.divergence.value != null && t.coverage.value != null,
        "the baseline's figures still publish - they are what every other row is read against");
      continue;
    }
    const expected = regrade(t.divergence.value, t.coverage.value);
    assert.equal(
      t.grade.letter,
      expected.letter,
      `${t.slug}'s published letter must equal one rederived from its own figures`,
    );
    assert.equal(
      t.grade.capAt,
      expected.capAt,
      `${t.slug}'s published ceiling must equal one rederived from its own coverage`,
    );
  }
  assert.equal(baselines, 1, "exactly one row is the baseline");
});

// Every level of this schema pre-computes the published pair. Leaving one level
// as raw counts made a consumer reimplement the one formula the schema otherwise
// hands them, and a consumer computing it themselves can silently desync.
test("every level publishes the same divergence/coverage pair, not just some", () => {
  const latest = buildLatest(model, site);
  for (const t of latest.targets) {
    for (const tier of Object.values(t.tiers)) {
      if (!tier) continue;
      assert.ok("value" in tier.correctness, "a tier's correctness carries a value");
    }
    for (const a of t.areas) {
      assert.ok(a.divergence && a.coverage, `area ${a.key} carries both figures`);
      if (a.total && a.passed + a.failed > 0) {
        assert.ok(Math.abs(a.divergence.value - (a.failed / a.total) * 100) < 0.001);
        assert.ok(Math.abs(a.coverage.value - ((a.passed + a.failed) / a.total) * 100) < 0.001);
      } else {
        assert.equal(a.divergence.value, null, `area ${a.key} implements nothing, so has no divergence`);
      }
    }
    for (const r of t.regions) {
      assert.ok(r.divergence && r.coverage, `region ${r.region} carries both figures`);
      for (const tier of Object.values(r.tiers)) {
        if (tier) assert.ok("value" in tier.correctness, "a region tier's correctness carries a value");
      }
    }
  }
});

test("a target's region summary says how wide the cohort behind its headline is", () => {
  const latest = buildLatest(model, site);
  for (const t of latest.targets) {
    if (!t.region) continue;
    assert.equal(t.region.cohortSize, t.region.cohort.length);
    assert.ok(t.region.observed === null || t.region.cohortSize <= t.region.observed);
  }
});

test("every target carries a uniform region summary; baseline is 'all'", () => {
  const latest = buildLatest(model, site);
  for (const t of latest.targets) {
    assert.ok("region" in t, `${t.slug} missing region key`);
    if (t.region) {
      assert.ok(["all", "pinned-plus", "beats-pinned"].includes(t.region.kind));
      assert.ok(Array.isArray(t.region.cohort));
      assert.equal(t.region.pinned, "eu-west-2");
      assert.equal(t.region.beatsPinned, t.region.kind === "beats-pinned");
    }
  }
  const ddb = latest.targets.find((t) => t.baseline);
  assert.equal(ddb.region.kind, "all");
});

test("latest exposes the per-region breakdown and the run's region health", () => {
  const latest = buildLatest(model, site);
  assert.ok("regionHealth" in latest);
  if (latest.regionHealth) {
    for (const k of ["observed", "unresolved", "dropped"]) assert.ok(Array.isArray(latest.regionHealth[k]));
  }
  const dz = latest.targets.find((t) => t.slug === "dynoxide");
  assert.ok(Array.isArray(dz.regions) && dz.regions.length > 0, "dynoxide has a per-region breakdown");
  const r = dz.regions[0];
  for (const k of ["region", "rate", "pinned", "inCohort", "indeterminate", "total", "tiers"]) assert.ok(k in r, `region entry missing ${k}`);
  assert.deepEqual(Object.keys(r.tiers).sort(), ["tier1", "tier2", "tier3"]);
  assert.ok(dz.regions.some((x) => x.region === "eu-west-2" && x.pinned === true), "eu-west-2 flagged as the baseline region");
});

test("index documents the region model and health vocabulary", () => {
  const index = buildIndex(model, site);
  assert.equal(index.regions.pinned, "eu-west-2");
  assert.deepEqual(index.regions.health.map((h) => h.key), ["observed", "unresolved", "dropped"]);
});

test("targets carry the maintainer disclosure, derived from the slug", () => {
  const latest = buildLatest(model, site);
  assert.equal(latest.targets.find((t) => t.slug === "dynoxide").maintainedByAuthor, true);
  assert.equal(latest.targets.find((t) => t.slug === "dynalite").maintainedByAuthor, false);
});

// The baseline row is synthesised at zero divergence across the whole suite,
// which is only honest while every real-AWS pass has reported. The homepage and
// /ground-truth disclose when one has not; without the same fact here, an agent
// reading the endpoints could not tell a fully measured baseline from a pinned
// one, and would have to know about a build-time file that is not a site path.
const summaryWith = (groundTruth) => ({ latest: { groundTruth } });

test("every endpoint discloses how much of the suite the baseline was measured on", () => {
  const summary = summaryWith({
    suiteSize: 1000,
    testsObserved: 983,
    lanes: [
      { name: "gating", runDate: "2026-08-09", tests: 983 },
      { name: "gsi", runDate: "2026-08-10", tests: 0 },
    ],
    missingLanes: ["integrations"],
  });

  for (const [name, doc] of [
    ["index", buildIndex(model, site, summary)],
    ["latest", buildLatest(model, site, summary)],
    ["runs", buildRuns(model, site, summary)],
  ]) {
    const obs = doc.baseline.observation;
    assert.ok(obs, `${name} carries no baseline observation`);
    assert.equal(obs.suiteSize, 1000);
    assert.equal(obs.testsObserved, 983);
    assert.equal(obs.unobserved, 17, `${name} must publish the shortfall, not just the counts`);
    assert.deepEqual(obs.missingLanes, ["integrations"]);
    // One capture date per pass: three passes under a single date would read
    // as one measurement.
    assert.deepEqual(
      obs.lanes.map((l) => l.name),
      ["gating", "gsi"],
    );
    assert.equal(obs.lanes[0].runDate, "2026-08-09");
  }
});

test("a fully observed baseline reports nothing outstanding rather than omitting the block", () => {
  const doc = buildLatest(
    model,
    site,
    summaryWith({
      suiteSize: 1000,
      testsObserved: 1000,
      lanes: [{ name: "gating", runDate: "2026-08-12", tests: 1000 }],
      missingLanes: [],
    }),
  );
  assert.equal(doc.baseline.observation.unobserved, 0);
  assert.deepEqual(doc.baseline.observation.missingLanes, []);
});

test("the split registry is discoverable from the data index", () => {
  // The methodology's grading section leans on it and the A+ premise is checked
  // against it, but it was reachable only by knowing the raw URL.
  const endpoint = buildIndex(model, site).endpoints.find((e) => /split/i.test(e.name));
  assert.ok(endpoint, "the index does not name the split registry");
  assert.match(endpoint.url, /registry\/splits\.json$/);
});
