import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { gradeOf } from "../lib/scoring.mjs";

const HERE = dirname(fileURLToPath(import.meta.url));

// Strip a leading YAML front-matter block, leaving just the page body.
function body(md) {
  const m = md.match(/^---\n[\s\S]*?\n---\n?/);
  return (m ? md.slice(m[0].length) : md).trim();
}

// Pull a hand-authored prose page's body, absolutising its root-relative links
// so the corpus stands alone when read away from the site.
function page(file, siteUrl) {
  const raw = readFileSync(join(HERE, file), "utf8");
  return body(raw).replace(/\]\(\//g, `](${siteUrl}/`);
}

// The latest standings rendered as plain text, derived from the same model the
// tables use, so this corpus can never drift from the live figures.
function latestResults(conformance) {
  const latest = conformance?.latest;
  if (!latest) return "";
  const lines = latest.standings.map((r) => {
    const t = r.tiers || {};
    const tiers = `Tier 1 ${t.tier1?.divergence ?? "-"}, Tier 2 ${t.tier2?.divergence ?? "-"}, Tier 3 ${t.tier3?.divergence ?? "-"}`;
    const baseline = r.slug === "dynamodb" ? " (baseline)" : "";
    const grade = gradeOf(r.divergenceValue, r.coverageValue);
    return `- ${r.display}${baseline} - grade ${grade.letter ?? "-"}${grade.capped ? " (capped by coverage)" : ""}; diverges ${r.divergence} of the suite; covers ${r.coverage}; diverges per tier ${tiers}; version ${r.version}`;
  });
  return [
    `# Latest results`,
    "",
    `Run ${latest.id} (${latest.date}), ${latest.suiteSize} tests. Divergence is failed / total and coverage is implemented / total, over the whole suite and again within each tier; lower divergence is better and the two are never added together. The grade is a reading of the pair, never a blend: divergence sets the letter and low coverage can only cap it. DynamoDB is the baseline, diverging nowhere by definition.`,
    "",
    ...lines,
  ].join("\n");
}

export default class {
  data() {
    return { permalink: "/llms-full.txt", eleventyExcludeFromCollections: true };
  }

  render(data) {
    const { site, conformance } = data;
    const header = [
      `# Parity Suite`,
      "",
      `> ${site.description}`,
      "",
      `This file concatenates the About and Methodology pages, the agent guide, and the latest results as text, so the whole picture can be read in one fetch. It's regenerated at build time from the same results as the rest of the site, so it can't drift from the live figures. Data endpoints and the licence are listed at the end.`,
    ].join("\n");

    const data_footer = [
      `# Data`,
      "",
      `- Latest run (JSON): ${site.url}/data/latest.json`,
      `- All runs (JSON): ${site.url}/data/runs.json`,
      `- Data index (JSON): ${site.url}/data/index.json`,
      `- Runs feed (Atom): ${site.url}/feed.xml`,
      "",
      `Published under CC BY 4.0; credit ${site.dataAttribution}.`,
    ].join("\n");

    return [
      header,
      page("about.md", site.url),
      page("methodology.md", site.url),
      page("for-agents.md", site.url),
      latestResults(conformance),
      data_footer,
    ].join("\n\n---\n\n") + "\n";
  }
}
