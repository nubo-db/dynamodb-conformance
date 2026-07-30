import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

// The committed fallback stores a *derived* model, not raw upstream data: each
// row carries figures and a movement object computed by lib/. So a change to
// how any of those are derived leaves the fallback serving the old values until
// someone regenerates it, and the site renders them without complaint.
//
// That is not hypothetical. Changing movement from correctness to divergence
// inverted its meaning - lower is now better - while the fallback still held
// movement computed the old way. The page showed a red-flavoured "fell 0.2
// percentage points" beside a legend that reads a fall as an improvement, on a
// target whose failures had just gone from 16 to 18. The board said a
// regression was an improvement.
//
// This checks the stored values against the row's own raw counts, so it fails
// on a stale fallback without needing the network or a rebuild. Regenerate with
// `node scripts/snapshot.mjs` from site/.

const here = dirname(fileURLToPath(import.meta.url));
const model = JSON.parse(readFileSync(join(here, "..", "data", "conformance-history.json"), "utf8"));

const divergenceOf = (row) => {
  const implemented = (row.passed ?? 0) + (row.failed ?? 0);
  if (!row.count || implemented === 0) return null;
  return (row.failed / row.count) * 100;
};

const scored = (row) => !row.synthesised && row.count > 0 && row.passed + row.failed > 0;

test("committed fallback: every row carries the figures the board publishes", () => {
  const missing = [];
  for (const run of model.runs ?? []) {
    for (const row of run.standings ?? []) {
      if (!scored(row)) continue;
      if (row.divergence === undefined || row.coverage === undefined) {
        missing.push(`${run.date}/${row.slug}`);
      }
    }
  }
  assert.deepEqual(
    missing.slice(0, 5),
    [],
    `rows with no divergence/coverage - regenerate the fallback (${missing.length} rows)`,
  );
});

test("committed fallback: stored divergence matches the row's own counts", () => {
  const wrong = [];
  for (const run of model.runs ?? []) {
    for (const row of run.standings ?? []) {
      if (!scored(row) || row.divergenceValue == null) continue;
      const derived = divergenceOf(row);
      // A row whose headline came from the region overlay legitimately differs
      // from its raw-count divergence, so only flag a row with no overlay.
      if (row.hasRegions) continue;
      if (Math.abs(derived - row.divergenceValue) > 0.05) {
        wrong.push(`${run.date}/${row.slug}: stored ${row.divergenceValue} vs ${derived}`);
      }
    }
  }
  assert.deepEqual(wrong.slice(0, 5), [], `stale divergence in the fallback (${wrong.length} rows)`);
});

test("committed fallback: movement direction agrees with the change in divergence", () => {
  // The one that actually broke. Movement is stored, so a change to how it is
  // derived can leave an arrow pointing the wrong way against a figure that
  // moved the other direction.
  const wrong = [];
  const byTarget = new Map();
  for (const run of [...(model.runs ?? [])].reverse()) {
    for (const row of run.standings ?? []) {
      if (!scored(row)) continue;
      const prev = byTarget.get(row.slug);
      byTarget.set(row.slug, row.divergenceValue ?? divergenceOf(row));
      const cur = row.divergenceValue ?? divergenceOf(row);
      const state = row.movement?.state;
      if (prev == null || cur == null || !state) continue;
      // `carried` (not re-tested this run), `new` and `baseline` describe the
      // run rather than a change, so there is no direction to check.
      if (["carried", "new", "baseline"].includes(state)) continue;
      if (!["improved", "regressed", "flat"].includes(state)) {
        wrong.push(`${run.date}/${row.slug}: unknown movement state "${state}"`);
        continue;
      }
      const delta = Math.round((cur - prev) * 10) / 10;
      const expected = delta < 0 ? "improved" : delta > 0 ? "regressed" : "flat";
      if (state !== expected) {
        wrong.push(`${run.date}/${row.slug}: ${state} but divergence moved ${delta}`);
      }
    }
  }
  assert.deepEqual(wrong.slice(0, 5), [], `movement disagrees with divergence (${wrong.length} rows)`);
});

// The same failure mode one level down. Tier figures are derived and stored, so
// a fallback written before the tier conversion serves correctness under the
// keys the pages now read as divergence - or, once the old keys are gone,
// serves nothing and renders blank tier bars.
test("committed fallback: every tier carries divergence, and none the pre-conversion keys", () => {
  const wrong = [];
  for (const run of model.runs ?? []) {
    for (const row of run.standings ?? []) {
      if (!scored(row)) continue;
      for (const [key, tier] of Object.entries(row.tiers ?? {})) {
        if (!tier) continue;
        if (tier.divergence === undefined || tier.coverage === undefined) {
          wrong.push(`${run.date}/${row.slug}/${key}: no divergence or coverage`);
        } else if ("pct" in tier || "value" in tier) {
          wrong.push(`${run.date}/${row.slug}/${key}: still carries pct/value`);
        }
      }
    }
  }
  assert.deepEqual(
    wrong.slice(0, 5),
    [],
    `stale tier figures in the fallback - regenerate it (${wrong.length} tiers)`,
  );
});

test("committed fallback: stored tier divergence matches that tier's own counts", () => {
  const wrong = [];
  for (const run of model.runs ?? []) {
    for (const row of run.standings ?? []) {
      if (!scored(row)) continue;
      for (const [key, tier] of Object.entries(row.tiers ?? {})) {
        if (!tier || tier.divergenceValue == null || !tier.total) continue;
        const derived = (tier.failed / tier.total) * 100;
        if (Math.abs(derived - tier.divergenceValue) > 0.05) {
          wrong.push(`${run.date}/${row.slug}/${key}: stored ${tier.divergenceValue} vs ${derived}`);
        }
      }
    }
  }
  assert.deepEqual(wrong.slice(0, 5), [], `tier divergence disagrees with its counts (${wrong.length} tiers)`);
});

test("committed fallback: no row still carries a pre-rename movement state", () => {
  const stale = new Set();
  for (const run of model.runs ?? []) {
    for (const row of run.standings ?? []) {
      const s = row.movement?.state;
      if (s === "up" || s === "down") stale.add(`${run.date}/${row.slug}`);
    }
  }
  assert.deepEqual([...stale].slice(0, 5), [], `movement states from before the rename (${stale.size} rows)`);
});
