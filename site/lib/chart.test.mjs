import { test } from "node:test";
import assert from "node:assert/strict";

import { chartGeometry } from "./chart.mjs";

const at = (v, date, runId, cov = null) => ({
  divergenceValue: v,
  divergence: v == null ? "-" : `${v.toFixed(1)}%`,
  coverageValue: cov,
  coverage: cov == null ? "-" : `${cov.toFixed(1)}%`,
  date,
  runId,
});

const cov = (v, date, runId) => at(null, date, runId, v);
const COVERAGE = { metric: "coverage" };

const series = [at(20, "2026-01-01", "2026-01-01"), at(8, "2026-02-01", "2026-02-01"), at(30, "2026-03-01", "2026-03-01")];

test("produces one point per series entry with increasing x", () => {
  const g = chartGeometry(series);
  assert.equal(g.pts.length, 3);
  assert.ok(g.pts[0].x < g.pts[1].x && g.pts[1].x < g.pts[2].x);
});

// The whole reason the axis inverted: the plot has to agree with the headline
// above it, so a target diverging more is plotted higher, and a falling line is
// a target getting better.
test("more divergence sits higher on the chart (smaller y)", () => {
  const g = chartGeometry(series);
  assert.ok(g.pts[2].y < g.pts[0].y); // 30 above 20
  assert.ok(g.pts[0].y < g.pts[1].y); // 20 above 8
});

test("tops above the worst value, to the nearest 5, and floors at 0", () => {
  const g = chartGeometry(series); // worst 30 -> top 35
  assert.equal(g.floor, 0);
  assert.equal(g.top, 35);
  for (const p of g.pts) {
    assert.ok(p.y >= g.y0 - 0.5 && p.y <= g.y1 + 0.5);
  }
});

test("a series bunched near zero tops to a finer step so movement reads", () => {
  const near = [at(0, "2026-01-01", "a"), at(1.8, "2026-01-02", "b"), at(0.4, "2026-01-03", "c")];
  const g = chartGeometry(near);
  assert.equal(g.top, 4); // nearest 2 above 1.8, not the nearest 5
  for (const p of g.pts) {
    assert.ok(p.y >= g.y0 - 0.5 && p.y <= g.y1 + 0.5);
  }
});

test("the top keeps headroom without adding a whole step it doesn't need", () => {
  const topAt = (worst) => chartGeometry([at(0, "2026-01-01", "a"), at(worst, "2026-01-02", "b")]).top;

  assert.equal(topAt(16.7), 20); // 3.3 of headroom is enough, no need to go to 25
  assert.equal(topAt(15), 20); // exactly on a step, so add one to clear the gridline
  assert.equal(topAt(30), 35);
});

test("the floor stays pinned at 0 so charts compare across targets", () => {
  for (const worst of [0.2, 15, 58]) {
    const g = chartGeometry([at(0, "2026-01-01", "a"), at(worst, "2026-01-02", "b")]);
    assert.equal(g.floor, 0);
  }
});

// A target that has never diverged still needs a plot with height, or the line
// is drawn along a zero-height axis and the page renders a collapsed figure.
test("a target that has never diverged still gets a plot with height", () => {
  const g = chartGeometry([at(0, "2026-01-01", "a"), at(0, "2026-01-02", "b")]);
  assert.ok(g.top > g.floor);
  for (const p of g.pts) assert.ok(Number.isFinite(p.y));
});

test("carries date labels and per-point run links", () => {
  const g = chartGeometry(series);
  assert.equal(g.pts[0].dateShort, "1 Jan");
  assert.equal(g.pts[2].runId, "2026-03-01");
  assert.equal(g.pts[0].label, "20.0%");
});

test("a single point centres on the chart and doesn't divide by zero", () => {
  const g = chartGeometry([at(0, "2026-01-01", "r")]);
  assert.equal(g.pts.length, 1);
  assert.ok(Number.isFinite(g.pts[0].x) && Number.isFinite(g.pts[0].y));
  assert.ok(g.pts[0].showDate);
});

// A run lands most days, so the axis has to stay legible as the series grows
// rather than labelling every point until they collide.
const manyRuns = (n) =>
  Array.from({ length: n }, (_, i) => at(5 + (i % 5), `2026-01-${String((i % 28) + 1).padStart(2, "0")}`, `r${i}`));

test("every point keeps a dot, however many runs there are", () => {
  const g = chartGeometry(manyRuns(90));
  assert.equal(g.pts.length, 90);
});

test("date labels never sit closer together than they are wide", () => {
  for (const n of [2, 12, 29, 90, 400]) {
    const g = chartGeometry(manyRuns(n));
    const xs = g.pts.filter((p) => p.showDate).map((p) => p.x);
    assert.ok(xs.length >= 2, `n=${n} should keep at least two date labels`);
    for (let i = 1; i < xs.length; i++) {
      assert.ok(xs[i] - xs[i - 1] >= 34, `n=${n} labels ${xs[i - 1]} and ${xs[i]} would collide`);
    }
  }
});

test("the most recent run is always dated, so the axis ends on a real date", () => {
  for (const n of [3, 29, 57, 90]) {
    const g = chartGeometry(manyRuns(n));
    assert.ok(g.pts[n - 1].showDate, `n=${n} should date the last point`);
  }
});

// The caption reads off the worst run, which on divergence is the highest one.
// Taking the minimum here would caption a target's best run as its worst.
test("marks the latest reading and the worst point for the caption", () => {
  const recovered = [at(20, "2026-01-01", "a"), at(30, "2026-02-01", "b"), at(8, "2026-03-01", "c")];
  const g = chartGeometry(recovered);
  assert.equal(g.latest.label, "8.0%");
  assert.equal(g.latest.dateShort, "1 Mar");
  assert.equal(g.worst.label, "30.0%");
  assert.equal(g.worst.dateShort, "1 Feb");
  // the caption's coordinates must be the same ones the plot drew
  assert.equal(g.latest.x, g.pts[2].x);
  assert.equal(g.worst.y, g.pts[1].y);
});

test("drops the worst when it is the latest reading, so the caption doesn't repeat itself", () => {
  const worsening = [at(1, "2026-01-01", "a"), at(4, "2026-01-02", "b")];
  const g = chartGeometry(worsening);
  assert.equal(g.latest.label, "4.0%");
  assert.equal(g.worst, null);
});

test("ignores unmeasured runs when picking the latest and the worst", () => {
  const gappy = [at(2, "2026-01-01", "a"), at(null, "2026-01-02", "b"), at(1, "2026-01-03", "c"), at(null, "2026-01-04", "d")];
  const g = chartGeometry(gappy);
  assert.equal(g.latest.label, "1.0%");
  assert.equal(g.worst.label, "2.0%");
});

// An unmeasured run implemented nothing, so it has no place on a divergence
// axis. Drawing it at the floor would render it as a flawless run.
test("an unmeasured run plots at the worst end, never as a perfect one", () => {
  const g = chartGeometry([at(2, "2026-01-01", "a"), at(null, "2026-01-02", "b")]);
  assert.ok(g.pts[1].y < g.pts[0].y);
  assert.equal(g.pts[1].y, g.y0);
});

// ── the coverage plot ────────────────────────────────────────────────────────
//
// The second series exists because divergence alone can show an improvement
// that is really a withdrawal. A fail becoming a skip leaves both numerators over
// the same fixed denominator, so the two figures fall by exactly the same amount:
// 88 of Dynalite's failing tests became skips on 2026-07-24 and both fell 8.8
// points, each of them 88/998. These guard
// the thing that would break silently - the coverage plot inheriting the
// divergence plot's sense, so a target attempting less rendered as improving.

test("coverage pins its top at 100 and floors below the lowest reading", () => {
  const g = chartGeometry([cov(100, "2026-01-01", "a"), cov(80, "2026-01-02", "b")], COVERAGE);
  assert.equal(g.top, 100);
  assert.equal(g.floor, 75);
  for (const p of g.pts) assert.ok(p.y >= g.y0 - 0.5 && p.y <= g.y1 + 0.5);
});

test("more coverage sits higher on the coverage chart", () => {
  const g = chartGeometry([cov(80, "2026-01-01", "a"), cov(95, "2026-01-02", "b")], COVERAGE);
  assert.ok(g.pts[1].y < g.pts[0].y);
});

// The whole point of the second plot. A run that lowers both figures must read
// as a regression on this chart even though it read as an improvement on the other.
test("a target that lowers divergence by covering less regresses on the coverage plot", () => {
  const series = [at(22.4, "2026-07-14", "a", 92.9), at(12.3, "2026-07-22", "b", 80.0)];
  const d = chartGeometry(series);
  const c = chartGeometry(series, COVERAGE);
  assert.ok(d.pts[1].y > d.pts[0].y, "divergence fell, so its line falls");
  assert.ok(c.pts[1].y > c.pts[0].y, "coverage fell too, so its line must also fall");
  // The lowest coverage it has ever had is where it is now, so there is no
  // separate worst to caption - the current figure already is it.
  assert.equal(c.latest.label, "80.0%");
  assert.equal(c.worst, null);
});

test("the worst coverage reading is the lowest, not the highest", () => {
  const g = chartGeometry([cov(90, "2026-01-01", "a"), cov(70, "2026-02-01", "b"), cov(95, "2026-03-01", "c")], COVERAGE);
  assert.equal(g.latest.label, "95.0%");
  assert.equal(g.worst.label, "70.0%");
});

test("each metric names its own axis sense and caption", () => {
  const d = chartGeometry([at(1, "2026-01-01", "a", 90), at(2, "2026-01-02", "b", 90)]);
  const c = chartGeometry([at(1, "2026-01-01", "a", 90), at(2, "2026-01-02", "b", 88)], COVERAGE);
  assert.match(d.axisLabel, /divergence - lower is better/);
  assert.match(c.axisLabel, /coverage - higher is better/);
  assert.notEqual(d.nowPrefix, c.nowPrefix);
  // The caption carries the identity, not just a direction.
  assert.match(c.sense, /costs exactly as much coverage as it gains divergence/);
});

test("an unmeasured coverage run plots at the bad end, which is the floor here", () => {
  const g = chartGeometry([cov(95, "2026-01-01", "a"), cov(null, "2026-01-02", "b")], COVERAGE);
  assert.equal(g.pts[1].y, g.y1);
  assert.ok(g.pts[1].y > g.pts[0].y);
});

// Two different runs can print the same figure, and the caption must not then
// state it twice. Dynalite's worst coverage (80.0% on 24 Jul) and its current
// coverage (80.0% on 29 Jul) are different runs with the same published value.
test("the worst is dropped when it reads the same as the current figure", () => {
  const g = chartGeometry(
    [cov(92.9, "2026-07-14", "a"), cov(80.0, "2026-07-24", "b"), cov(80.04, "2026-07-29", "c")],
    COVERAGE,
  );
  assert.equal(g.latest.label, "80.0%");
  assert.equal(g.worst, null, "same printed figure, so there is nothing separate to caption");
});

test("a worst that prints differently is still captioned", () => {
  const g = chartGeometry([cov(92.9, "2026-07-14", "a"), cov(80.0, "2026-07-24", "b"), cov(85.0, "2026-07-29", "c")], COVERAGE);
  assert.equal(g.latest.label, "85.0%");
  assert.equal(g.worst.label, "80.0%");
});
