import { buildLatest } from "../../lib/dataset.mjs";

// The latest run in full: per target, per tier, coverage, capabilities and
// operation areas. The results API a consumer should read instead of scraping.
export default class {
  data() {
    return { permalink: "/data/latest.json", eleventyExcludeFromCollections: true };
  }

  render(data) {
    return JSON.stringify(buildLatest(data.conformance, data.site, data.summary), null, 2);
  }
}
