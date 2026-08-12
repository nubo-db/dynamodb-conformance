// Build inline-SVG geometry for one of a target's two published series. Pure
// and presentation-only: takes the per-target series and returns coordinates the
// target-chart component renders directly (WebC component setup can't compute
// from props, so the maths lives here and is unit-tested).
//
// Either metric can be plotted, and both are, because neither is readable
// alone. A fail becoming a skip leaves the divergence numerator and the coverage
// numerator together over the same fixed denominator, so the two always fall by
// exactly the same amount: a divergence line on its own shows a withdrawal as an
// improvement, and only the coverage line beside it says which it was. 88 of
// Dynalite's failing tests became skips on 24 July 2026 and both figures fell
// 8.8 points, each of them exactly 88/998. That is the same
// thing the board refuses to do when it publishes the two figures side by side
// rather than summing them, applied to the time axis.
//
// Each metric keeps its own honest orientation and its own pinned end, so
// neither is squashed to share an axis with the other: divergence pins its floor
// at 0 and grows upward for a regression, coverage pins its top at 100 and falls
// for one. Both therefore render a regression as movement away from the pinned
// edge.

const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];
const shortDate = (iso) => {
  const [, m, d] = iso.split("-").map(Number);
  return `${d} ${MONTHS[m - 1]}`;
};

// A date label measures ~34 units at the chart's 10-unit type size, so labels
// closer together than this collide. Runs accumulate daily and the plot width
// is fixed, so the axis has to thin itself rather than label every point.
const MIN_DATE_GAP = 44;

// The two series this module can plot. Each names how to read a point, how to
// caption it, and which direction is bad - everything that differs between them,
// so the geometry below is written once.
const METRICS = {
  divergence: {
    key: "divergence",
    heading: "Divergence",
    value: (p) => p.divergenceValue,
    label: (p) => p.divergence,
    axisLabel: "divergence - lower is better",
    nowPrefix: "Diverging on",
    nowSuffix: "of the suite",
    worstPrefix: "at worst",
    sense: "Lower is better, so a falling line is a target getting closer to real DynamoDB.",
    // A regression raises divergence, so the worst reading is the highest.
    isWorse: (a, b) => a > b,
    pinned: "floor",
  },
  coverage: {
    key: "coverage",
    heading: "Coverage",
    value: (p) => p.coverageValue,
    label: (p) => p.coverage,
    axisLabel: "coverage - higher is better",
    nowPrefix: "Implementing",
    nowSuffix: "of the suite",
    worstPrefix: "as little as",
    sense: "Higher is better. This is the share of the suite's tests a target implements at all. A fail becoming a skip leaves both numerators over the same fixed denominator, so a fall here is matched point for point by a fall in divergence: withdrawal costs exactly as much coverage as it gains divergence.",
    isWorse: (a, b) => a < b,
    pinned: "top",
  },
};

// Round the top up to a step that leaves headroom above the worst value. A
// narrow series (everything near zero) gets a finer step, otherwise a run
// spanning 0-2% would sit squashed along the bottom of the plot. The floor
// stays pinned at 0 rather than floating to fit: a target 15 points off perfect
// should look 15 points off perfect, and the axis has to mean the same thing on
// every target's page for the charts to be comparable.
function topFor(maxV, floor) {
  const step = maxV - floor <= 10 ? 2 : 5;
  let top = Math.ceil(maxV / step) * step;
  // Add a step when the worst point would otherwise sit on, or almost on, the
  // top gridline. This also gives a target that has never diverged a plot with
  // height, instead of a flat line drawn along a zero-height axis.
  if (top - maxV < step / 2) top += step;
  return top <= floor ? floor + 10 : top;
}

// The mirror of topFor, for a series whose top is pinned at 100: round the floor
// down to a step that leaves headroom below the lowest value, so a run spanning
// 94-100% isn't squashed into the top of the plot.
function floorFor(minV, top) {
  const step = top - minV <= 10 ? 2 : 5;
  let floor = Math.max(0, Math.floor(minV / step) * step);
  if (minV - floor < step / 2) floor = Math.max(0, floor - step);
  return floor >= top ? top - 10 : floor;
}

// Which points get a date label: stride back from the last point so the most
// recent run is always labelled and the spacing reads evenly. The first point
// joins in only when the stride left it enough room, otherwise the axis would
// end up with the collision this is here to avoid.
function dateIndices(n, spacing) {
  const stride = Math.max(1, Math.ceil(MIN_DATE_GAP / spacing));
  const idx = new Set();
  for (let i = n - 1; i >= 0; i -= stride) idx.add(i);
  if (Math.min(...idx) * spacing >= MIN_DATE_GAP) idx.add(0);
  return idx;
}

export function chartGeometry(series, opts = {}) {
  const { W = 680, H = 250, padL = 40, padR = 18, padT = 22, padB = 44, metric = "divergence" } = opts;
  const m = METRICS[metric] ?? METRICS.divergence;
  const x0 = padL, x1 = W - padR, y0 = padT, y1 = H - padB;

  const vals = series.map(m.value).filter((v) => v != null);
  // Whichever end is pinned, the other is rounded away from the data so the
  // series has headroom and a flat run still gets a plot with height.
  let floor, top;
  if (m.pinned === "floor") {
    floor = 0;
    top = topFor(vals.length ? Math.max(...vals) : 0, floor);
  } else {
    top = 100;
    floor = floorFor(vals.length ? Math.min(...vals) : top, top);
  }

  const n = series.length;
  const xFor = (i) => (n <= 1 ? (x0 + x1) / 2 : x0 + (i / (n - 1)) * (x1 - x0));
  const yFor = (v) => y1 - ((v - floor) / (top - floor)) * (y1 - y0);
  // Where an unmeasured run is drawn: the bad end of this metric's axis. It must
  // never land on the good end, or a run nobody scored would render as the
  // target's best.
  const absent = m.pinned === "floor" ? top : floor;

  const spacing = n <= 1 ? x1 - x0 : (x1 - x0) / (n - 1);
  const dated = dateIndices(n, spacing);

  const pts = series.map((p, i) => {
    const v = m.value(p);
    return {
      x: +xFor(i).toFixed(1),
      y: +yFor(v == null ? absent : v).toFixed(1),
      label: m.label(p),
      dateShort: shortDate(p.date),
      runId: p.runId,
      showDate: dated.has(i),
    };
  });

  const polyline = pts.map((p) => `${p.x},${p.y}`).join(" ");
  const grid = [floor, Math.round((floor + top) / 2), top].map((v) => ({ v, y: +yFor(v).toFixed(1) }));

  return {
    W, H, x0, x1, y0, y1, floor, top, pts, polyline, grid,
    labelY: H - 24,
    metric: m.key,
    heading: m.heading,
    axisLabel: m.axisLabel,
    nowPrefix: m.nowPrefix,
    nowSuffix: m.nowSuffix,
    worstPrefix: m.worstPrefix,
    sense: m.sense,
    ...marks(series, pts, m),
  };
}

// The two figures worth stating in words: where the target stands now, and the
// furthest it has been in the bad direction. Every other per-run number lives in
// the run table below the chart, so the plot itself only has to carry the shape.
//
// Which end is bad depends on the metric - highest for divergence, lowest for
// coverage - so it comes from the metric rather than being assumed. Reading it
// off the wrong end captions a target's best run as its worst.
function marks(series, pts, m) {
  const measured = series
    .map((p, i) => ({ ...p, i }))
    .filter((p) => m.value(p) != null);
  if (!measured.length) return { latest: null, worst: null };

  const latest = measured[measured.length - 1];
  const worst = measured.reduce((a, b) => (m.isWorse(m.value(b), m.value(a)) ? b : a));
  const at = (p) => ({ ...pts[p.i], value: m.value(p) });

  const latestAt = at(latest);
  const worstAt = at(worst);
  return {
    latest: latestAt,
    // Suppress the worst when it reads the same as where the target stands now.
    // Comparing indices alone wasn't enough: Dynalite's worst coverage is 80.0%
    // on 24 July and its current coverage is 80.0% on 29 July, two different
    // runs, so the caption said "implementing 80.0% ... as little as 80.0%".
    // Compared on the published label, so it matches what a reader sees rather
    // than a difference too small to print.
    worst: worst.i === latest.i || worstAt.label === latestAt.label ? null : worstAt,
  };
}
