import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";

import { buildSplitsModel, shapeSplit, renderSplitEvidence, splitCoverage, splitCoverageNote } from "./splits.mjs";

const here = dirname(fileURLToPath(import.meta.url));
const raw = JSON.parse(readFileSync(join(here, "..", "data", "splits-fallback.json"), "utf8"));

test("buildSplitsModel shapes the registry and features the most divergent split", () => {
  const model = buildSplitsModel(raw);
  assert.equal(model.available, true);
  assert.ok(model.count >= 1);
  assert.ok(model.featured.groups.length >= 2, "a split has at least two answer cohorts");
  // The featured split has at least as many distinct error kinds as any other.
  const kinds = (s) => new Set(s.groups.map((g) => g.error?.name ?? g.outcome)).size;
  for (const s of model.splits) assert.ok(kinds(model.featured) >= kinds(s));
});

test("shapeSplit groups regions by their distinct answer, largest cohort first", () => {
  const split = shapeSplit({
    id: "x",
    behaviour: "b",
    pinned: "eu-west-2",
    regions: {
      "eu-west-2": { outcome: "rejected", error: { name: "ValidationException", message: "one error" } },
      "us-east-1": { outcome: "rejected", error: { name: "ValidationException", message: "two errors" } },
      "us-east-2": { outcome: "rejected", error: { name: "ValidationException", message: "two errors" } },
    },
  });
  assert.equal(split.groups.length, 2);
  assert.equal(split.groups[0].count, 2, "largest cohort first");
  assert.equal(split.groups[1].hasPinned, true, "eu-west-2 cohort is flagged");
});

test("renderSplitEvidence marks the baseline region and shows cohort counts", () => {
  const model = buildSplitsModel(raw);
  const html = renderSplitEvidence(model.featured);
  assert.match(html, /baseline/);
  assert.match(html, /regions?</);
});

test("an empty registry degrades to unavailable", () => {
  assert.equal(buildSplitsModel({ splits: [] }).available, false);
  assert.equal(buildSplitsModel(null).available, false);
  assert.equal(renderSplitEvidence(null), "");
});

// ── The arithmetic under the cohorts ────────────────────────────────────────
//
// The cohort counts add up to fewer regions than the board says it scores, for
// two reasons that look the same on the page: a region with no recorded answer
// at capture, and a region named in the row that has since been dropped.

const split = (groups) => ({ pinned: "eu-west-2", groups: groups.map((regions) => ({ regions })) });

test("names the observed regions a split has no answer for", () => {
  const c = splitCoverage(split([["eu-west-2"], ["us-east-1"]]), ["eu-west-2", "us-east-1", "eu-north-1"]);
  assert.equal(c.observed, 3);
  assert.equal(c.accounted, 2);
  assert.deepEqual(c.unrecorded, ["eu-north-1"]);
  assert.deepEqual(c.departed, []);
});

test("separates a region that has dropped out from one that never answered", () => {
  // me-central-1 answered when the evidence was captured and is no longer
  // scored, so it inflates the cohort count without being an observed region.
  const c = splitCoverage(split([["eu-west-2", "me-central-1"], ["us-east-1"]]), ["eu-west-2", "us-east-1", "eu-north-1"]);
  assert.deepEqual(c.unrecorded, ["eu-north-1"]);
  assert.deepEqual(c.departed, ["me-central-1"]);
});

test("the note stays silent when the cohorts account for every observed region", () => {
  assert.equal(splitCoverageNote(split([["eu-west-2"], ["us-east-1"]]), ["eu-west-2", "us-east-1"]), "");
});

test("the note states the accounted count and names both kinds of gap", () => {
  const note = splitCoverageNote(split([["eu-west-2", "me-central-1"], ["us-east-1"]]), ["eu-west-2", "us-east-1", "eu-north-1"]);
  assert.match(note, /account for 2 of them/);
  assert.match(note, /eu-north-1/);
  assert.match(note, /me-central-1 answered at capture/);
});

test("splitCoverage degrades on an absent split rather than throwing", () => {
  assert.equal(splitCoverage(null, ["eu-west-2"]), null);
  assert.equal(splitCoverageNote(null, ["eu-west-2"]), "");
  assert.equal(splitCoverageNote(split([["eu-west-2"]]), []), "");
});

// ── Escaping ────────────────────────────────────────────────────────────────
//
// renderSplitEvidence builds HTML by hand from registry data, and that data is
// AWS error text: a real DynamoDB message, verbatim, in the one render helper
// carrying it. Nothing else in this file's suite checks the escaping.

test("renderSplitEvidence escapes the error text it renders", () => {
  const html = renderSplitEvidence(
    shapeSplit({
      id: "x",
      pinned: "eu-west-2",
      regions: {
        "eu-west-2": {
          outcome: "rejected",
          error: { name: "<img src=x onerror=alert(1)>", message: 'broke on "value" & <b>more</b>' },
        },
      },
    }),
  );
  assert.ok(!html.includes("<img src=x"), "an error name reached the page as markup");
  assert.ok(html.includes("&lt;img src=x"), "the name should be escaped, not dropped");
  assert.ok(!html.includes("<b>more</b>"), "an error message reached the page as markup");
  assert.ok(html.includes("&amp;") && html.includes("&quot;"), "ampersand and quote should be escaped");
});

test("renderSplitEvidence escapes a region name", () => {
  // Region names come from the registry rather than AWS, but they are
  // interpolated by the same helper and into the same context.
  const html = renderSplitEvidence(
    shapeSplit({
      id: "x",
      pinned: "eu-west-2",
      regions: { '"><script>': { outcome: "accepted", detail: "ok" } },
    }),
  );
  assert.ok(!html.includes("<script>"), "a region name reached the page as markup");
  assert.ok(html.includes("&lt;script&gt;"));
});
