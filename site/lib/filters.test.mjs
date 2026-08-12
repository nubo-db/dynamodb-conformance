import test from "node:test";
import assert from "node:assert/strict";

import config from "../eleventy.config.js";

// Filters are the seam between the templates and lib/, and a filter registered
// as `(a) => fn(a)` drops every later argument without erroring. That is how the
// target page came to render its divergence plot twice: the coverage call passed
// `{ metric: "coverage" }`, the filter swallowed it, and the default came back.
// Every unit test in lib/ passed, because lib/ was correct. So the wiring is
// checked here, against the real config rather than a copy of it.

// A stand-in for Eleventy's config object that answers to anything: every
// property is a callable that also answers to anything, so this doesn't need
// updating each time the config starts using another Eleventy API.
function registeredFilters() {
  const filters = {};
  const anything = () =>
    new Proxy(function () {}, {
      get: () => anything(),
      apply: () => undefined,
    });
  const stub = new Proxy(
    {},
    {
      get: (_t, prop) =>
        prop === "addFilter" ? (name, fn) => void (filters[name] = fn) : anything(),
    },
  );
  config(stub);
  return filters;
}

test("the chartGeometry filter forwards its options, so both plots aren't the same metric", () => {
  const { chartGeometry } = registeredFilters();
  assert.ok(chartGeometry, "chartGeometry filter is registered");

  const series = [
    { divergenceValue: 22.4, divergence: "22.4%", coverageValue: 92.9, coverage: "92.9%", date: "2026-07-14", runId: "a" },
    { divergenceValue: 12.3, divergence: "12.3%", coverageValue: 80.0, coverage: "80.0%", date: "2026-07-22", runId: "b" },
  ];

  const divergence = chartGeometry(series, { metric: "divergence" });
  const coverage = chartGeometry(series, { metric: "coverage" });

  assert.equal(divergence.metric, "divergence");
  assert.equal(coverage.metric, "coverage");
  assert.notEqual(divergence.polyline, coverage.polyline);
  assert.notEqual(divergence.axisLabel, coverage.axisLabel);
  assert.notEqual(divergence.heading, coverage.heading);
});
