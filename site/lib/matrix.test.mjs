import { test } from "node:test";
import assert from "node:assert/strict";

import { buildMatrix, renderSupportCards } from "./matrix.mjs";

// A minimal model shaped like buildModel's output, enough to exercise the grid.
const model = {
  targets: ["dynamodb", "dynoxide", "dynalite"],
  allAreas: [
    { key: "tier1/getItem", tier: "tier1", group: "getItem" },
    { key: "tier2/transactions", tier: "tier2", group: "transactions" },
  ],
  perTarget: {
    dynamodb: { display: "DynamoDB", baseline: true, currentVersion: "live (AWS)", areas: [
      { key: "tier1/getItem", state: "supported" },
      { key: "tier2/transactions", state: "supported" },
    ] },
    dynoxide: { display: "Dynoxide", currentVersion: "0.9.13", areas: [
      { key: "tier1/getItem", state: "supported" },
      { key: "tier2/transactions", state: "failing" },
    ] },
    dynalite: { display: "Dynalite", currentVersion: "4.0.0", areas: [
      { key: "tier1/getItem", state: "supported" },
      { key: "tier2/transactions", state: "unsupported" },
    ] },
  },
};

test("buildMatrix lays out corner, column headers, and ncols", () => {
  const m = buildMatrix(model);
  // The baseline is excluded, so it is the operation column plus 2 emulators.
  assert.equal(m.ncols, 3);
  assert.equal(m.items[0].type, "corner");
  assert.deepEqual(m.items.slice(1, 3).map((i) => [i.type, i.slug]), [
    ["col", "dynoxide"], ["col", "dynalite"],
  ]);
});

test("the synthesised baseline is not a column", () => {
  // Every one of its cells would be supported by definition, so the column
  // carried no information a reader could act on.
  const m = buildMatrix(model);
  assert.ok(!m.items.some((i) => i.type === "col" && i.slug === "dynamodb"));
  assert.ok(!m.targets.some((t) => t.slug === "dynamodb"));
});

test("column headers carry each target's current version", () => {
  const m = buildMatrix(model);
  const cols = m.items.filter((i) => i.type === "col");
  assert.deepEqual(cols.map((c) => [c.slug, c.version]), [
    ["dynoxide", "0.9.13"], ["dynalite", "4.0.0"],
  ]);
  // a target with no version recorded falls back to a dash, never undefined
  const noVersion = buildMatrix({
    targets: ["ghost"],
    allAreas: [{ key: "tier1/getItem", tier: "tier1", group: "getItem" }],
    perTarget: { ghost: { display: "Ghost", areas: [] } },
  });
  assert.equal(noVersion.items.find((i) => i.type === "col").version, "-");
});

test("each tier emits a header item then a rowhead + one cell per target", () => {
  const m = buildMatrix(model);
  const tiers = m.items.filter((i) => i.type === "tier").map((i) => i.tier);
  assert.deepEqual(tiers, ["tier1", "tier2"]);
  // the transactions row: rowhead followed by one cell per emulator, in order
  // (the baseline is excluded, so DynamoDB is not among them)
  const txnIdx = m.items.findIndex((i) => i.type === "rowhead" && i.group === "transactions");
  const cells = m.items.slice(txnIdx + 1, txnIdx + 3);
  assert.deepEqual(cells.map((c) => [c.slug, c.state]), [
    ["dynoxide", "failing"], ["dynalite", "unsupported"],
  ]);
  assert.ok(cells.every((c) => c.group === "transactions")); // cell self-describes
});

test("a cell carries the per-area pass/fail/skip counts for its tooltip", () => {
  const counted = {
    targets: ["dynoxide"],
    allAreas: [{ key: "tier1/putItem", tier: "tier1", group: "putItem" }],
    perTarget: {
      dynoxide: { display: "Dynoxide", areas: [
        { key: "tier1/putItem", state: "partial", passed: 62, failed: 4, skipped: 0 },
      ] },
    },
  };
  const cell = buildMatrix(counted).items.find((i) => i.type === "cell");
  assert.deepEqual(
    [cell.state, cell.passed, cell.failed, cell.skipped],
    ["partial", 62, 4, 0],
  );
});

test("a group spanning tiers gets a tier qualifier on its row head; a single-tier group doesn't", () => {
  const spanning = {
    targets: ["dynoxide"],
    allAreas: [
      { key: "tier1/updateTable", tier: "tier1", group: "updateTable" },
      { key: "tier2/updateTable", tier: "tier2", group: "updateTable" },
      { key: "tier1/getItem", tier: "tier1", group: "getItem" },
    ],
    perTarget: { dynoxide: { display: "Dynoxide", areas: [] } },
  };
  const heads = buildMatrix(spanning).items.filter((i) => i.type === "rowhead");
  const update = heads.filter((h) => h.group === "updateTable");
  assert.deepEqual(update.map((h) => h.qualifier), ["Tier 1", "Tier 2"]);
  assert.equal(heads.find((h) => h.group === "getItem").qualifier, null);
});

test("an area absent from a target's results renders as n/a, not a crash", () => {
  const sparse = {
    targets: ["dynamodb", "ghost"],
    allAreas: [{ key: "tier1/getItem", tier: "tier1", group: "getItem" }],
    perTarget: {
      dynamodb: { display: "DynamoDB", areas: [{ key: "tier1/getItem", state: "supported" }] },
      ghost: { display: "Ghost", areas: [] },
    },
  };
  const m = buildMatrix(sparse);
  const ghostCell = m.items.find((i) => i.type === "cell" && i.slug === "ghost");
  assert.equal(ghostCell.state, "n/a");
});

test("sections mirror the grid as a nested tier -> row -> cell shape for mobile", () => {
  const m = buildMatrix(model);
  assert.deepEqual(m.sections.map((s) => s.tier), ["tier1", "tier2"]);
  const txn = m.sections[1].rows.find((r) => r.group === "transactions");
  assert.deepEqual(
    txn.cells.map((c) => [c.slug, c.state]),
    [["dynoxide", "failing"], ["dynalite", "unsupported"]],
  );
  // every row carries one cell per target, in target order
  for (const sec of m.sections) {
    for (const row of sec.rows) assert.equal(row.cells.length, m.targets.length);
  }
});

test("renderSupportCards emits one card per operation with every target and its glyph", () => {
  const html = renderSupportCards(buildMatrix(model));
  assert.match(html, /Tier 1 - Core/);
  assert.match(html, /Tier 2 - Complete/);
  assert.match(html, /getItem/);
  assert.match(html, /transactions/);
  // each emulator is labelled inside the transactions card; the baseline is not
  // a card, so it does not appear
  assert.doesNotMatch(html, /DynamoDB/);
  assert.match(html, /Dynoxide/);
  assert.match(html, /Dynalite/);
  // states map to the right glyph and a spoken label (colour never alone)
  assert.match(html, /✓/); // supported
  assert.match(html, /✗/); // failing (dynoxide on transactions)
  assert.match(html, /–/); // unsupported (dynalite on transactions)
  assert.match(html, /Dynoxide transactions: failing/); // sr-only describe text
  assert.match(html, /0\.9\.13/); // each target's version travels with it
  assert.match(html, /4\.0\.0/);
});

test("renderSupportCards escapes target and operation names rather than injecting markup", () => {
  const evil = {
    targets: ["x"],
    allAreas: [{ key: "tier1/<b>op</b>", tier: "tier1", group: "<b>op</b>" }],
    perTarget: {
      x: { display: "<script>alert(1)</script>", areas: [{ key: "tier1/<b>op</b>", state: "supported" }] },
    },
  };
  const html = renderSupportCards(buildMatrix(evil));
  assert.doesNotMatch(html, /<script>alert/);
  assert.match(html, /&lt;script&gt;/);
  assert.match(html, /&lt;b&gt;op&lt;\/b&gt;/);
});

// The per-operation table is the most detailed set of figures on a target page,
// so it is the last place a correctness percentage could have survived the
// conversion and read as though the target were improving while its headline
// said the opposite.
test("renderTargetOperations reports each area as divergence with coverage beside it", async () => {
  const { renderTargetOperations } = await import("./matrix.mjs");
  const areas = [
    { key: "tier1/getItem", tier: "tier1", group: "getItem", passed: 40, failed: 0, skipped: 0, total: 40, state: "supported" },
    { key: "tier1/createTable", tier: "tier1", group: "createTable", passed: 23, failed: 7, skipped: 0, total: 30, state: "partial" },
    { key: "tier2/transactions", tier: "tier2", group: "transactions", passed: 0, failed: 62, skipped: 0, total: 62, state: "failing" },
    { key: "tier2/streams", tier: "tier2", group: "streams", passed: 0, failed: 0, skipped: 18, total: 18, state: "unsupported" },
  ];
  const html = renderTargetOperations(areas);
  assert.match(html, /Tier 1 - Core/);
  assert.match(html, /Tier 2 - Complete/);
  assert.match(html, /getItem/);
  assert.match(html, /0\.0%/); // getItem diverges nowhere
  assert.match(html, /23\.3%/); // createTable: 7 of 30, not the 76.7% it read as correctness
  assert.match(html, /100\.0%/); // transactions diverges on all of them
  assert.match(html, /n\/a/); // streams: nothing implemented, so no divergence to report
  assert.match(html, /18 skip/);
  // Counts are fails over the operation's whole size, matching the figure.
  assert.match(html, /7\/30/);
});

// An operation a target declines has no divergence, and its coverage is what
// says so. Reporting 0.0% there would read as flawless.
test("renderTargetOperations gives an unimplemented operation no divergence and zero coverage", async () => {
  const { renderTargetOperations } = await import("./matrix.mjs");
  const html = await renderTargetOperations([
    { key: "tier2/streams", tier: "tier2", group: "streams", passed: 0, failed: 0, skipped: 18, total: 18, state: "unsupported" },
  ]);
  assert.match(html, /n\/a/);
  assert.match(html, /0\.0%/);
  assert.doesNotMatch(html, /100\.0%/);
});

test("renderTargetOperations is empty when a target has no areas", async () => {
  const { renderTargetOperations } = await import("./matrix.mjs");
  assert.equal(renderTargetOperations([]), "");
});
