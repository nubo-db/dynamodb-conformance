import { buildIndex } from "../../lib/dataset.mjs";

// Discovery manifest for the published results data. Pretty-printed because it's
// meant to be opened and read, not just parsed.
export default class {
  data() {
    return { permalink: "/data/index.json", eleventyExcludeFromCollections: true };
  }

  render(data) {
    return JSON.stringify(buildIndex(data.conformance, data.site, data.summary), null, 2);
  }
}
