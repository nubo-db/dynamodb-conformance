#!/usr/bin/env node
// Regenerate every committed fallback the site falls back to when the live
// fetch can't reach the suite (offline, or CI without a token):
//   - data/conformance-history.json  (the per-target results timeline)
//   - data/summary-history.json      (the per-region overlay)
//   - data/changelog-fallback.md     (the suite history log)
//   - data/splits-fallback.json      (the confirmed regional splits)
//   - data/tag-manifest.json         (results grouped by capability)
//
// The first two are derived models - exactly what the data files produce from a
// live fetch - never raw API responses. The last three are the upstream source
// files verbatim, because that's what their data files parse at build time.
// Run this whenever upstream has meaningfully moved:
//
//   npm run snapshot
//
// A GITHUB_TOKEN in the environment lifts the commits-API rate limit. Each
// write is guarded: a fetch that fails or returns something unusable leaves the
// committed copy alone, because a stale fallback beats an empty one.

import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { fetchSnapshots } from "../lib/fetch.mjs";
import { buildModel, leanForFallback } from "../lib/history.mjs";
import { historyDigest } from "../lib/digest.mjs";
import { fetchSummaries } from "../lib/summary-fetch.mjs";
import { assemble } from "../lib/summary-source.mjs";
import { parseChangelog } from "../lib/changelog.mjs";
import { buildSplitsModel } from "../lib/splits.mjs";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const token = process.env.GITHUB_TOKEN;
const timeoutMs = 15000;
const log = (msg) => console.error(`[snapshot] ${msg}`);

// The per-region overlay first, so buildModel can join it into the conformance
// fallback and the committed model matches what a live build produces.
//
// The checkout's own results/summary.json leads the fetched history. Everything
// fetched comes from main, so on a branch that changes the artefact the fallback
// would otherwise mirror a shape the branch has already moved past - and the
// hermetic build check, which renders the fallback, would be checking the old
// contract. Whatever this working tree publishes is what main carries once the
// branch lands, so it belongs at the front. assemble() keeps the first snapshot
// it sees for a run date, so on main this changes nothing: the local file and
// the newest fetched commit are the same document.
async function localSummarySnapshot() {
  try {
    const raw = JSON.parse(await readFile(join(root, "..", "results", "summary.json"), "utf8"));
    return [{ sha: "working-tree", raw }];
  } catch (err) {
    log(`no local results/summary.json to lead the history (${err.message})`);
    return [];
  }
}

const summarySnaps = [...(await localSummarySnapshot()), ...(await fetchSummaries({ token, timeoutMs, log }))];
const summary = assemble(summarySnaps);
if (!summary.latest) {
  console.error("[snapshot] no summary snapshots reconstructed - refusing to write an empty overlay");
  process.exit(1);
}
const summaryPayload = { available: true, source: "fallback", ...summary, generatedAt: null };
await writeFile(join(root, "data", "summary-history.json"), `${JSON.stringify(summaryPayload, null, 2)}\n`, "utf8");
log(`wrote summary overlay for run dates ${summary.runDates.join(", ")} to data/summary-history.json`);

const snapshots = await fetchSnapshots({ token, timeoutMs, log });

// Pass the payload (which carries `available: true`) so buildModel applies the
// overlay; the bare assemble() result has no availability flag and would be
// treated as no overlay, baking a region-less fallback.
const model = buildModel(snapshots, summaryPayload);
if (!model.latest) {
  console.error("[snapshot] no runs reconstructed - refusing to write an empty fallback");
  process.exit(1);
}

// Thin the findings to what the fallback needs (see leanForFallback for why).
const lean = leanForFallback(model);
// Hash what is actually written, not the model it came from. A build that falls
// back recomputes the digest from this file, so a stored hash taken before the
// strip could never match and would be misleading to anyone checking.
//
// The two paths do produce different digests for identical data, because a live
// build's projection sees findings and a fallback's does not. That only costs a
// redundant deploy, never a skipped one: the scheduled build is the only one that
// skips, and it fails rather than falling back.
const payload = { ...lean, historyHash: historyDigest(lean), capturedAt: new Date().toISOString() };
await writeFile(join(root, "data", "conformance-history.json"), `${JSON.stringify(payload, null, 2)}\n`, "utf8");

console.error(
  `[snapshot] wrote ${model.runs.length} runs, ${model.targets.length} targets ` +
    `(latest ${model.latest.id}, digest ${payload.historyHash}) to data/conformance-history.json`,
);

// The three verbatim mirrors. Each is fetched from the raw CDN (no API limit,
// no token) and checked with the same parser its data file uses, so a fetch
// that succeeds but returns something the site can't read is caught here rather
// than on the day the fallback is finally needed.
const RAW_BASE = "https://raw.githubusercontent.com/paritysuite/dynamodb-conformance/main";

const mirrors = [
  {
    path: "CHANGELOG.md",
    into: "changelog-fallback.md",
    check(body) {
      const { entries, skipped } = parseChangelog(body);
      if (!entries.length) return "no dated entries";
      if (skipped.length) return `unreadable heading(s): ${skipped.join(", ")}`;
      return { ok: `${entries.length} entries, latest ${entries[0].date}` };
    },
  },
  {
    path: "registry/splits.json",
    into: "splits-fallback.json",
    check(body) {
      const model = buildSplitsModel(JSON.parse(body));
      if (!model.available) return "registry carries no splits";
      return { ok: `${model.count} splits` };
    },
  },
  {
    path: "results/tag-manifest.json",
    into: "tag-manifest.json",
    check(body) {
      // `describes` is the payload; an empty one degrades the capability grid
      // to n/a, so a manifest that parses but carries nothing is a failure.
      const describes = JSON.parse(body)?.describes;
      const count = Object.keys(describes ?? {}).length;
      if (!count) return "manifest carries no describes";
      return { ok: `${count} describes` };
    },
  },
];

let failed = 0;
for (const { path, into, check } of mirrors) {
  try {
    const res = await fetch(`${RAW_BASE}/${path}`, { signal: AbortSignal.timeout(timeoutMs) });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const body = await res.text();

    const verdict = check(body);
    if (typeof verdict === "string") throw new Error(verdict);

    await writeFile(join(root, "data", into), body, "utf8");
    log(`wrote ${path} (${verdict.ok}) to data/${into}`);
  } catch (err) {
    failed += 1;
    log(`left data/${into} untouched: ${err.message}`);
  }
}

if (failed) {
  log(`${failed} of ${mirrors.length} mirrors could not be refreshed`);
  process.exit(1);
}
