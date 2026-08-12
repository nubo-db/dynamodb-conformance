import { buildRuns } from "../../lib/dataset.mjs";

// The full history: every recorded run's per-target tier scores, coverage and
// movement, newest first. The runs API for stepping back through the timeline.
export default class {
  data() {
    return { permalink: "/data/runs.json", eleventyExcludeFromCollections: true };
  }

  render(data) {
    return JSON.stringify(buildRuns(data.conformance, data.site, data.summary), null, 2);
  }
}
