import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import {
  DISPLAY,
  REPO,
  areaOf,
  areaState,
  areaTallies,
  breakdownOf,
  display,
  dynamodbRow,
  isSelfMaintained,
  label,
  pct,
  scoreEmulator,
  tierFigures,
  tierOf,
} from "./scoring.mjs";
import * as suite from "dynamodb-conformance/scripts/summarise.mjs";
import * as suiteScore from "dynamodb-conformance/scripts/lib/score.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const fixtures = join(here, "..", "test", "fixtures");

test("tierOf classifies by /tierN/ regardless of path prefix", () => {
  assert.equal(tierOf("/home/runner/work/x/tests/tier1/foo.test.ts"), "tier1");
  assert.equal(tierOf("/Users/martin/Projects/x/tests/tier2/foo.test.ts"), "tier2");
  assert.equal(tierOf("/anything/tier3/foo.test.ts"), "tier3");
  assert.equal(tierOf("/no/tier/here.test.ts"), "other");
});

test("pct is correctness over implemented (passed, failed) - skips excluded", () => {
  assert.equal(pct(1, 1), "50.0%"); // 1 passed, 1 failed
  assert.equal(pct(625, 0), "100.0%"); // all implemented passed
  assert.equal(pct(0, 0), "-"); // nothing implemented
  assert.equal(pct(2, 1), "66.7%"); // 2 of 3 implemented pass
});

test("display proper-cases known slugs and humanises unknown ones", () => {
  assert.equal(display("dynamodb-local"), "DynamoDB Local");
  assert.equal(display("dynoxide"), "Dynoxide");
  assert.equal(display("some-new-thing"), "some new thing");
});

test("label links known targets and leaves unknown ones bare", () => {
  assert.equal(label("dynoxide"), "[Dynoxide](https://github.com/nubo-db/dynoxide)");
  assert.equal(label("some-new-thing"), "some new thing");
});

test("scoreEmulator buckets tiers, counts statuses, derives date + version", () => {
  const raw = {
    startTime: Date.parse("2026-05-24T07:18:15.825Z"),
    testResults: [
      { name: "/x/tier1/a.test.ts", assertionResults: [{ status: "passed" }, { status: "failed" }] },
      { name: "/x/tier2/b.test.ts", assertionResults: [{ status: "passed" }, { status: "skipped" }] },
      { name: "/x/tier3/c.test.ts", assertionResults: [{ status: "passed" }] },
    ],
  };
  const r = scoreEmulator("dynoxide", raw, "0.9.13");
  assert.equal(r.passed, 3);
  assert.equal(r.failed, 1);
  assert.equal(r.skipped, 1);
  assert.equal(r.count, 5); // count still includes the skip
  // Tiers report divergence over the whole tier, the same axis as the headline.
  assert.equal(r.tiers.tier1.divergence, "50.0%"); // 1 of 2 failed
  assert.equal(r.tiers.tier2.divergence, "0.0%"); // nothing failed; the skip is coverage, not divergence
  assert.equal(r.tiers.tier3.divergence, "0.0%");
  // Coverage sits beside it, and is what shows tier 2's skip.
  assert.equal(r.tiers.tier1.coverage, "100.0%");
  assert.equal(r.tiers.tier2.coverage, "50.0%");
  // Correctness is still available, under its own name.
  assert.equal(r.tiers.tier1.correctness, "50.0%");
  assert.equal(r.tiers.tier2.correctness, "100.0%");
  assert.equal(r.total, "75.0%"); // 3 passed / (3 + 1 failed); skip excluded
  assert.equal(r.totalValue, 75);
  assert.equal(r.version, "0.9.13");
  assert.equal(r.runDate, "2026-05-24");
});

test("scoreEmulator treats any non-passed/failed status as a skip", () => {
  const raw = {
    startTime: Date.parse("2026-01-01T00:00:00Z"),
    testResults: [
      { name: "/x/tier1/a.test.ts", assertionResults: [{ status: "todo" }, { status: "pending" }] },
    ],
  };
  const r = scoreEmulator("x", raw, "-");
  assert.equal(r.skipped, 2);
  assert.equal(r.failed, 0);
});

test("scoreEmulator surfaces a missing version as '-' and missing startTime as '-'", () => {
  const raw = { testResults: [] };
  const r = scoreEmulator("x", raw, "-");
  assert.equal(r.version, "-");
  assert.equal(r.runDate, "-");
  assert.equal(r.total, "-");
  assert.equal(r.totalValue, null);
});

test("skips are excluded from the score, raising it above the skip-inclusive figure", () => {
  // 8 passed, 2 failed, 90 skipped: correctness is 8/10 = 80%, not 8/100.
  const raw = {
    startTime: Date.parse("2026-05-24T00:00:00Z"),
    testResults: [
      {
        name: "/x/tier1/a.test.ts",
        assertionResults: [
          ...Array(8).fill({ status: "passed" }),
          ...Array(2).fill({ status: "failed" }),
          ...Array(90).fill({ status: "skipped" }),
        ],
      },
    ],
  };
  const r = scoreEmulator("x", raw, "-");
  assert.equal(r.total, "80.0%"); // 8 / (8 + 2)
  assert.equal(r.totalValue, 80);
  assert.equal(r.skipped, 90); // still reported
  assert.equal(r.count, 100); // count still includes skips
  // Scope axis: 10 of 100 operations implemented, 90 unsupported.
  assert.equal(r.implemented, 10);
  assert.equal(r.unsupported, 90);
  assert.equal(r.coverage, "10.0%");
  assert.equal(r.coverageValue, 10);
});

test("a target with everything skipped has no score (passed + failed === 0)", () => {
  const raw = {
    startTime: Date.parse("2026-05-24T00:00:00Z"),
    testResults: [{ name: "/x/tier2/partiql/a.test.ts", assertionResults: [{ status: "skipped" }, { status: "skipped" }] }],
  };
  const r = scoreEmulator("x", raw, "-");
  assert.equal(r.total, "-");
  assert.equal(r.totalValue, null);
  assert.equal(r.skipped, 2);
});

test("areaOf extracts the tier/group from a test path", () => {
  assert.deepEqual(areaOf("/x/tests/tier2/transactions/basic.test.ts"), {
    tier: "tier2",
    group: "transactions",
    key: "tier2/transactions",
  });
  assert.equal(areaOf("/no/tier/here.test.ts"), null);
});

test("breakdownOf lists only areas with gaps, with titles, sorted by gap size", () => {
  const raw = {
    testResults: [
      {
        name: "/x/tier2/transactions/a.test.ts",
        assertionResults: [
          { status: "failed", fullName: "Transactions writes atomically" },
          { status: "failed", fullName: "Transactions roll back" },
          { status: "skipped", fullName: "Transactions support idempotency" },
        ],
      },
      {
        name: "/x/tier1/putItem/b.test.ts",
        assertionResults: [
          { status: "passed", fullName: "PutItem stores an item" },
          { status: "failed", fullName: "PutItem rejects oversized items" },
        ],
      },
      {
        name: "/x/tier1/getItem/c.test.ts",
        assertionResults: [{ status: "passed", fullName: "GetItem returns an item" }],
      },
    ],
  };
  const b = breakdownOf(raw);
  // getItem is all-passing, so it's excluded; transactions (3 gaps) before putItem (1 gap).
  assert.deepEqual(b.map((a) => a.key), ["tier2/transactions", "tier1/putItem"]);
  assert.equal(b[0].failed, 2);
  assert.equal(b[0].skipped, 1);
  assert.deepEqual(b[0].skips, ["Transactions support idempotency"]);
  assert.equal(b[1].failures[0], "PutItem rejects oversized items");
});

test("areaState classifies supported / partial / unsupported / failing", () => {
  assert.equal(areaState({ passed: 5, failed: 0, skipped: 0 }), "supported"); // clean pass
  assert.equal(areaState({ passed: 5, failed: 0, skipped: 2 }), "partial"); // passes what it runs, skips some
  assert.equal(areaState({ passed: 4, failed: 1, skipped: 0 }), "partial"); // mostly passes, one gap
  assert.equal(areaState({ passed: 4, failed: 1, skipped: 2 }), "partial"); // passes, fails and skips mixed
  assert.equal(areaState({ passed: 0, failed: 0, skipped: 3 }), "unsupported"); // implements none of it
  assert.equal(areaState({ passed: 0, failed: 2, skipped: 0 }), "failing"); // implemented, nothing passes
  assert.equal(areaState({ passed: 0, failed: 2, skipped: 9 }), "failing"); // implemented but every run fails
});

test("areaTallies keeps every area with counts + state, sorted by tier then group", () => {
  const raw = {
    testResults: [
      { name: "/x/tier1/getItem/a.test.ts", assertionResults: [{ status: "passed" }, { status: "passed" }] },
      { name: "/x/tier2/transactions/b.test.ts", assertionResults: [{ status: "skipped" }, { status: "skipped" }] },
      { name: "/x/tier1/putItem/c.test.ts", assertionResults: [{ status: "passed" }, { status: "failed" }] },
    ],
  };
  const a = areaTallies(raw);
  assert.deepEqual(a.map((x) => x.key), ["tier1/getItem", "tier1/putItem", "tier2/transactions"]);
  assert.equal(a.find((x) => x.group === "getItem").state, "supported");
  assert.equal(a.find((x) => x.group === "putItem").state, "partial"); // 1 pass, 1 fail: a mix
  assert.equal(a.find((x) => x.group === "transactions").state, "unsupported");
});

// The ground-truth row is synthesised, never scored from a file, so it must
// appear at a definitional 100% across the full suite size even on a run that
// never reached AWS. Asserted against the row itself: the markdown table this
// used to be read out of was a second renderer of the suite's own, and it is
// gone.
test("the DynamoDB row is synthesised at 100% across the suite size", () => {
  const row = dynamodbRow(526, "-");
  assert.equal(row.total, "100.0%");
  assert.equal(row.totalValue, 100);
  assert.equal(row.passed, 526);
  assert.equal(row.failed, 0);
  assert.equal(row.skipped, 0);
  assert.equal(row.count, 526);
  assert.equal(row.version, "live (AWS)");
  assert.equal(row.runDate, "-");
  assert.equal(row.baseline, true);
  for (const t of ["tier1", "tier2", "tier3"]) {
    assert.equal(row.tiers[t].divergence, "0.0%", `${t} must diverge nowhere`);
    assert.equal(row.tiers[t].coverage, "100.0%", `${t} must cover the whole tier`);
  }
});

// The live consistency guard, and the only one that can observe a suite-side
// change. The headline number comes from the suite's summary.json so it cannot
// disagree by construction; what can drift is the number the site still derives
// itself, the eu-west-2 column. This pins that score for each target to
// summary.json's eu-west-2 rate for the same run, reading a captured summary
// plus its matching results verbatim. Keep it through any refactor.
test("parity: the port's score equals summary.json's eu-west-2 rate for every target", () => {
  const dir = join(fixtures, "regions");
  const summary = JSON.parse(readFileSync(join(dir, "summary.json"), "utf8"));
  const round1 = (n) => Math.round(n * 10) / 10;
  let checked = 0;
  for (const slug of Object.keys(summary.targets)) {
    const euw2 = summary.targets[slug].regions["eu-west-2"];
    if (!euw2) continue; // a target absent from eu-west-2 this run is not a mismatch
    const raw = JSON.parse(readFileSync(join(dir, "results", `${slug}.json`), "utf8"));
    const scored = scoreEmulator(slug, raw, "-");
    assert.equal(round1(scored.totalValue), euw2.rate, `${slug}: port ${scored.totalValue} vs summary eu-west-2 ${euw2.rate}`);
    checked++;
  }
  assert.ok(checked >= 7, `expected every target checked, got ${checked}`);
});

test("isSelfMaintained flags the board author's own engine for the disclosure", () => {
  assert.equal(isSelfMaintained("dynoxide"), true);
  // A build of the engine carries the same conflict of interest: the
  // disclosure must travel to the wasm page and its maintainedByAuthor field.
  assert.equal(isSelfMaintained("dynoxide-wasm"), true);
  assert.equal(isSelfMaintained("dynalite"), false);
});

// The maps must be the suite's own objects, not a copy that happens to agree
// today. Comparing values would pass the moment someone reintroduced a local
// literal with the same contents, which is precisely the drift this module was
// changed to prevent; comparing identity fails the instant the import is
// replaced by a declaration.
test("the target maps are the suite's objects rather than a local copy", () => {
  assert.strictEqual(DISPLAY, suite.DISPLAY);
  assert.strictEqual(REPO, suite.REPO);
  assert.strictEqual(display, suite.display);
  assert.strictEqual(label, suite.label);
  assert.strictEqual(tierOf, suiteScore.tierOf);
});

test("every scored target is nameable and linkable from the shared maps", () => {
  // A slug present in one map and not the other renders as a bare slug or an
  // unlinked name on the board. Cheap to assert, invisible until published.
  for (const slug of Object.keys(DISPLAY)) {
    assert.equal(display(slug), DISPLAY[slug], `${slug} lost its display name`);
    assert.ok(REPO[slug], `${slug} has a display name but no project URL`);
  }
  assert.deepEqual(Object.keys(REPO).sort(), Object.keys(DISPLAY).sort());
});

// The identity the methodology page now states, asserted from the arithmetic
// rather than from a fixture, and through the real code path.
//
// Two non-gameability claims have been published and both were false. What is
// true is narrower and checkable: because the denominator is the whole suite
// either way, a test moving from failing to skipped leaves the divergence
// numerator and the coverage numerator together, so both figures fall by exactly
// the same amount. If that ever stops holding, the page is wrong again.
test("moving a fail to a skip moves divergence and coverage by identical deltas", () => {
  const cases = [];
  for (const p of [0, 1, 7, 130, 673]) {
    for (const f of [1, 2, 41, 213]) {
      for (const s of [0, 3, 112]) {
        for (const i of [0, 5]) cases.push({ p, f, s, i });
      }
    }
  }

  for (const t of cases) {
    const before = tierFigures(t);
    // One fail becomes a skip. Nothing is fixed; the tally size is unchanged.
    const after = tierFigures({ ...t, f: t.f - 1, s: t.s + 1 });
    const total = t.p + t.f + t.s + t.i;

    assert.equal(before.total, after.total, "the denominator must not move");

    const dDiv = after.divergenceValue - before.divergenceValue;
    const dCov = after.coverageValue - before.coverageValue;

    // Both fall, by the same amount, and that amount is one test's worth.
    assert.ok(Math.abs(dDiv - dCov) < 1e-9, `deltas differ for ${JSON.stringify(t)}: ${dDiv} vs ${dCov}`);
    assert.ok(Math.abs(dDiv - -100 / total) < 1e-9, `delta is not -1/total for ${JSON.stringify(t)}`);
    assert.ok(dDiv < 0 && dCov < 0, "withdrawal lowers both, never raises either");
  }
});

// The contrast the page draws, on the same tallies: the figure this replaced
// moved the other way, because those tests left its denominator too.
test("the correctness figure this replaced rises on the same withdrawal", () => {
  for (const t of [{ p: 673, f: 213, s: 112, i: 0 }, { p: 7, f: 41, s: 3, i: 5 }, { p: 130, f: 2, s: 0, i: 0 }]) {
    const before = tierFigures(t);
    const after = tierFigures({ ...t, f: t.f - 1, s: t.s + 1 });
    assert.ok(
      after.correctnessValue > before.correctnessValue,
      `correctness should rise on withdrawal for ${JSON.stringify(t)}`,
    );
    // Which is the whole problem: it rose while divergence fell, so neither
    // figure alone could tell a withdrawal from a fix.
    assert.ok(after.divergenceValue < before.divergenceValue);
  }
});

// A target that withdraws its last remaining fail has no divergence left to
// report, so the identity's endpoint is null rather than a spurious zero.
test("withdrawing the last fail leaves no divergence, not zero", () => {
  const after = tierFigures({ p: 0, f: 0, s: 10, i: 0 });
  assert.equal(after.divergenceValue, null);
  assert.equal(after.divergence, "-");
  assert.equal(after.coverage, "0.0%");
});
