// A smoke test for the built site.
//
// `npm test` stops at the lib/ boundary: nothing there loads eleventy.config.js,
// resolves a permalink, or renders a template. Most of what can go wrong in a
// page is therefore invisible to it. Two bugs shipped on this branch were plain
// in the built HTML and green in the test suite the whole time.
//
// The build here is hermetic. `fetch` is stubbed to reject, so every data file
// takes its committed-fallback path and the result depends on the repo alone,
// never on GitHub being reachable. That keeps this usable as a gate: a red run
// means the code broke, not that an API had a bad minute.
//
// Run with `npm run check:build`.

import { mkdtemp, readFile, rm, glob } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";

const failures = [];
const check = (ok, label, detail = "") => {
  if (ok) return console.log(`  ok    ${label}`);
  failures.push(`${label}${detail ? `: ${detail}` : ""}`);
  console.log(`  FAIL  ${label}${detail ? ` - ${detail}` : ""}`);
};

const out = await mkdtemp(join(tmpdir(), "paritysuite-build-"));

let pages = [];
try {
  // The CLI rather than the JS API: eleventy.config.js returns
  // `dir: { output: "_site" }`, which wins over the API's output argument, and
  // building into _site would clobber whatever someone is serving locally.
  // `--output` does override it. The preload is what removes the network.
  execFileSync("npx", ["@11ty/eleventy", "--output", out], {
    stdio: ["ignore", "inherit", "inherit"],
    env: { ...process.env, NODE_OPTIONS: `${process.env.NODE_OPTIONS ?? ""} --import ./scripts/no-network.mjs`.trim() },
  });

  for await (const f of glob("**/*.html", { cwd: out })) pages.push(f);
  // The text outputs too: llms.txt and llms-full.txt are built for machine
  // consumption, which made them the one surface exempt from the retired-
  // wording and NaN scans - and the one place a stale phrase survived a
  // conversion. The structural checks below filter by path, so text files
  // pass through them untouched.
  for await (const f of glob("**/*.txt", { cwd: out })) pages.push(f);
  const read = async (f) => ({ path: `/${f}`, html: await readFile(join(out, f), "utf8") });
  const docs = await Promise.all(pages.map(read));

  console.log(`\nBuilt ${docs.length} pages from the committed fallback.\n`);

  // Every check below reads from `docs`, so an empty collection would let all of
  // them pass by vacuous truth. Stop here instead of reporting a green run.
  if (!docs.length) {
    console.error("Collected no pages from the build output; the checks below would pass on nothing.\n");
    process.exit(1);
  }

  check(docs.length > 100, "builds a plausible number of pages", `got ${docs.length}`);
  check(docs.some((d) => d.path === "/index.html"), "builds a home page");
  check(docs.some((d) => /^\/targets\/[^/]+\/\d{4}-\d{2}-\d{2}\//.test(d.path)), "builds per-run target pages");

  // Every internal link has to resolve. This is the check that would have caught
  // 55 dead links when the synthesised baseline stopped getting dated pages but
  // two templates carried on linking to them.
  const built = new Set(docs.map((d) => d.path.replace(/index\.html$/, "").replace(/\/$/, "")));
  const dead = new Set();
  for (const d of docs) {
    for (const m of d.html.matchAll(/href="(\/[^"#?]*)"/g)) {
      const href = m[1].replace(/\/$/, "");
      if (built.has(href) || href.endsWith(".xml") || href.endsWith(".json") || href.endsWith(".txt")) continue;
      if (/\.(css|js|png|svg|ico|woff2?)$/.test(href)) continue;
      dead.add(`${href} (from ${d.path})`);
    }
  }
  check(dead.size === 0, "every internal link resolves to a built page", [...dead].slice(0, 5).join("; "));

  // The house style has no em dashes. Test titles come from the suite carrying
  // them, so they are normalised on the way out; this is what proves it.
  const dashed = docs.filter((d) => d.html.includes("—")).map((d) => d.path);
  check(dashed.length === 0, "no em dashes in any built page", dashed.slice(0, 3).join(", "));

  // The baseline is synthesised, never measured, so a page claiming it was
  // tested on a given date would contradict the pipeline it comes from.
  const baselineDated = docs.filter((d) => /^\/targets\/dynamodb\/\d{4}-\d{2}-\d{2}\//.test(d.path));
  check(baselineDated.length === 0, "the synthesised baseline gets no per-run pages", baselineDated.slice(0, 3).map((d) => d.path).join(", "));

  // A per-run page that renders a target's gaps must name the run it is for,
  // or the frozen figure it shows is indistinguishable from the current one.
  const dated = docs.filter((d) => /^\/targets\/[^/]+\/\d{4}-\d{2}-\d{2}\//.test(d.path));
  const undatedPages = dated.filter((d) => {
    const date = d.path.split("/")[3];
    return !d.html.includes(date) && !d.html.includes(date.replace(/-/g, ""));
  });
  check(undatedPages.length === 0, "every per-run page states its own run date", undatedPages.slice(0, 3).map((d) => d.path).join(", "));

  // The fallback keeps findings for the newest measurement only, so the newest
  // per-run pages have to itemise their failures with a source link. Without
  // this, the whole check would be exercising the degraded rendering that shows
  // when detail is absent, and would prove nothing about the page that ships.
  const itemised = dated.filter((d) => /tests\/[\w\-./]+\.test\.ts:\d+/.test(d.html));
  check(itemised.length > 0, "the newest per-run pages itemise their failures", `${itemised.length} of ${dated.length} dated pages carry a pinned source link`);

  // And a test-source link has to point at a commit, not a branch, or it stops
  // describing the code that was measured as soon as the file moves. Scoped to
  // links into `tests/`: the footer links NOTICE on main, quite correctly, and
  // matching every blob link flagged that as a failure.
  const unpinned = itemised.filter((d) => /\/blob\/(?:main|master)\/tests\//.test(d.html));
  check(unpinned.length === 0, "test-source links pin to a commit rather than a branch", unpinned.slice(0, 3).map((d) => d.path).join(", "));

  // Two plots per target page, and they must not be the same plot twice. The
  // filter was registered as `(series) => fn(series)`, which silently dropped
  // the options object, so the coverage call fell back to the divergence default
  // and the page rendered one metric under two headings. Every unit test passed,
  // because the library was right and the wiring was wrong. Geometry is compared
  // rather than headings: identical polylines is the symptom that matters.
  const targetPages = [];
  for (const path of pages) {
    if (!/^targets\/[^/]+\/index\.html$/.test(path)) continue;
    const html = await readFile(join(out, path), "utf8");
    const polylines = [...html.matchAll(/<polyline[^>]*points="([^"]+)"/g)].map((m) => m[1]);
    const axes = [...html.matchAll(/rotate\(-90\)[^>]*>([^<]+)<\/text>/g)].map((m) => m[1]);
    // A target with one run renders a note instead of plots, and the baseline
    // never renders them, so only pages that drew any are candidates.
    if (polylines.length === 0) continue;
    targetPages.push({ path, polylines, axes });
  }
  check(targetPages.length > 0, "target pages render history plots");
  const singlePlot = targetPages.filter((p) => p.polylines.length !== 2);
  check(singlePlot.length === 0, "every plotted target page draws exactly two plots", singlePlot.slice(0, 3).map((p) => `${p.path} drew ${p.polylines.length}`).join(", "));
  const duplicated = targetPages.filter((p) => p.polylines.length === 2 && p.polylines[0] === p.polylines[1]);
  check(duplicated.length === 0, "the two plots are different metrics, not the same one twice", duplicated.slice(0, 3).map((p) => p.path).join(", "));
  const mislabelled = targetPages.filter((p) => new Set(p.axes).size !== p.axes.length || p.axes.length !== 2);
  check(mislabelled.length === 0, "each plot names its own axis sense", mislabelled.slice(0, 3).map((p) => `${p.path}: ${p.axes.join(" / ")}`).join(", "));

  // Wording the conversion retired. A page still claiming a target can't lower
  // its divergence by attempting less, or framing the baseline as a flat 100%,
  // is the defect two review passes found by reading prose rather than code.
  const RETIRED = [
    "can't lower it by attempting less",
    "cannot lower it by attempting less",
    "at a flat 100%",
    "correctness over implemented operations, split into three tiers",
    // The grade introduction retired the bare-percentage headline and the
    // dual-encoded bar it needed a paragraph to explain.
    "the bar shows coverage coloured by divergence",
    "a short green bar is a target that is right about a narrow surface",
    // The baseline is framed by its divergence now, not the retired
    // correctness percentage.
    "the baseline, 100% by definition",
  ];
  const stale = [];
  for (const path of pages) {
    const html = await readFile(join(out, path), "utf8");
    for (const phrase of RETIRED) if (html.includes(phrase)) stale.push(`${path}: "${phrase}"`);
  }
  check(stale.length === 0, "no built page carries wording the conversion retired", stale.slice(0, 3).join(", "));

  // Every grade chip must show a letter from the published set (or the
  // unscored dash). A chip showing anything else - "NaN", "undefined", an
  // empty string - means the derivation broke inside a template, which no
  // unit test sees.
  const badChips = [];
  let chipCount = 0;
  for (const path of pages) {
    if (!path.endsWith(".html")) continue;
    const html = await readFile(join(out, path), "utf8");
    for (const m of html.matchAll(/class="grade-chip[^"]*"[^>]*>\s*<span aria-hidden="true">([^<]*)<\/span>/g)) {
      chipCount++;
      const letter = m[1].trim();
      if (!["A+", "A", "B", "C", "D", "F", "–"].includes(letter)) badChips.push(`${path}: "${letter}"`);
    }
  }
  check(chipCount > 0, "grade chips render on the built pages", `found ${chipCount}`);
  check(badChips.length === 0, "every grade chip shows a letter from the published set", badChips.slice(0, 3).join(", "));

  // The yardstick is not graded, and the published surfaces agreeing about that
  // is a claim the methodology makes in as many words. Checking the letter set
  // was not enough to catch the last breach: the run pages, the targets index
  // and both chips on a target's own page each graded the baseline A+ from its
  // raw figures while the README, the badges and the endpoints had stopped, and
  // every chip was a valid letter, so the build stayed green. This reads the
  // chip inside each rendered baseline row instead.
  const graded = [];
  for (const path of pages) {
    const html = await readFile(join(out, path), "utf8");
    // Each block from a link to the baseline's page up to the next one, which
    // is the row or card that link belongs to.
    // The slug boundary matters: /targets/dynamodb is a prefix of
    // /targets/dynamodb-local, whose C would otherwise read as the baseline's.
    for (const block of html.split(/(?=href="\/targets\/dynamodb["/])/).slice(1)) {
      const chip = block.slice(0, 1200).match(/grade-chip[^>]*>\s*<span[^>]*>([^<]+)</);
      if (chip && chip[1].trim() !== "–") graded.push(`${path}: ${chip[1].trim()}`);
    }
  }
  check(
    graded.length === 0,
    "no built page gives the baseline a letter",
    graded.slice(0, 3).join(", "),
  );

  // A figure that renders as NaN% or undefined is the shape of a zero-vs-null
  // slip, which is the recurring defect class of this conversion.
  const broken = [];
  for (const path of pages) {
    const html = await readFile(join(out, path), "utf8");
    if (/NaN%|>undefined<|undefined%/.test(html)) broken.push(path);
  }
  check(broken.length === 0, "no built page renders NaN or undefined as a figure", broken.slice(0, 3).join(", "));
} finally {
  await rm(out, { recursive: true, force: true });
}

if (failures.length) {
  console.error(`\n${failures.length} build check(s) failed:\n${failures.map((f) => `  - ${f}`).join("\n")}\n`);
  process.exit(1);
}
console.log("\nAll build checks passed.\n");
