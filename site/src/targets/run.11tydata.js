// One page per (target, run) that was actually measured: this emulator's view of
// that run. Reached from the Run history table on the target page, so a row goes
// to that emulator's results for that date rather than to the whole run.
export default {
  layout: "layouts/base.webc",
  pagination: {
    data: "conformance.targetRuns",
    size: 1,
    alias: "pair",
    addAllPagesToCollections: true,
  },
  eleventyComputed: {
    permalink: (data) => `/targets/${data.pair.slug}/${data.pair.runId}/`,
    // The run's own date, not the build's.
    lastmod: (data) => data.pair.date,
    target: (data) => data.conformance.perTarget[data.pair.slug],
    point: (data) => data.conformance.perTarget[data.pair.slug]?.series.find((p) => p.runId === data.pair.runId),
    // The target's history up to and including this run, so the page can plot how
    // it got here rather than only stating where it landed. Truncated rather than
    // the full series: a page dated 1 July must not draw a line through August,
    // which would show a reader runs that hadn't happened when this one did.
    seriesTo: (data) => {
      const series = data.conformance.perTarget[data.pair.slug]?.series ?? [];
      const i = series.findIndex((p) => p.runId === data.pair.runId);
      return i < 0 ? series : series.slice(0, i + 1);
    },
    // Prev/next across the dates this target was actually measured, so navigation
    // only lands on pages that exist. The series is oldest-first, so the newer
    // run is the next index up.
    neighbours: (data) => {
      const series = data.conformance.perTarget[data.pair.slug]?.series ?? [];
      const i = series.findIndex((p) => p.runId === data.pair.runId);
      return {
        older: i > 0 ? series[i - 1] : null,
        newer: i >= 0 && i < series.length - 1 ? series[i + 1] : null,
      };
    },
    breadcrumbs: (data) => {
      const t = data.conformance.perTarget[data.pair.slug];
      return [
        { name: "Results", url: "/" },
        { name: "Targets", url: "/targets" },
        { name: t ? t.display : data.pair.slug, url: `/targets/${data.pair.slug}` },
        { name: data.pair.date, url: `/targets/${data.pair.slug}/${data.pair.runId}` },
      ];
    },
    meta: (data) => {
      const t = data.conformance.perTarget[data.pair.slug];
      const name = t ? t.display : data.pair.slug;
      return {
        title: `${name} on ${data.pair.date}`,
        description: `How ${name} scored against real DynamoDB in the conformance run of ${data.pair.date}, and the tests it failed.`,
        ogType: "article",
      };
    },
  },
};
