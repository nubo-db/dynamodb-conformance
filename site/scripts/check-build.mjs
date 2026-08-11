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
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { execFileSync } from "node:child_process";

import { GRADE_BANDS, gradeOf, gradingCriteriaEffectiveLabel } from "../lib/scoring.mjs";
import { axesOf } from "dynamodb-conformance/scripts/lib/score.mjs";

const failures = [];
const check = (ok, label, detail = "") => {
  if (ok) return console.log(`  ok    ${label}`);
  failures.push(`${label}${detail ? `: ${detail}` : ""}`);
  console.log(`  FAIL  ${label}${detail ? ` - ${detail}` : ""}`);
};

// The two published artefacts the A+ premise is checked from, loaded the way
// the build loads them. This build is hermetic, so both take their committed
// fallback and this reads what the pages above were rendered from.
async function loadPublishedData() {
  const root = join(dirname(fileURLToPath(import.meta.url)), "..");
  const summary = JSON.parse(await readFile(join(root, "data", "summary-history.json"), "utf8"));
  const splits = JSON.parse(await readFile(join(root, "data", "splits-fallback.json"), "utf8"));
  return { summary: summary.latest, splits: splits.splits ?? [] };
}

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
    // The rolling coverage weight replaced three discrete steps. The old
    // wording claimed more than the criteria deliver: it said the two figures
    // were never traded against each other, which the weight does, and it
    // named cap boundaries that no longer exist.
    "never summed, averaged or otherwise traded",
    "nothing is averaged, weighted or traded between the two",
    "under 90% of the suite grades no better than",
    "under 90% caps at B",
    "the coverage caps are what answers",
    "That leniency is what the coverage caps are for",
  ];
  // One alternation rather than 15 includes() per page: the same assertion over
  // one pass of the corpus instead of fifteen.
  const retired = new RegExp(RETIRED.map((x) => x.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")).join("|"));
  const stale = [];
  for (const d of docs) {
    const hit = retired.exec(d.html);
    if (hit) stale.push(`${d.path}: "${hit[0]}"`);
  }
  check(stale.length === 0, "no built page carries wording the conversion retired", stale.slice(0, 3).join(", "));

  // Every grade chip must show a letter from the published set (or the
  // unscored dash). A chip showing anything else - "NaN", "undefined", an
  // empty string - means the derivation broke inside a template, which no
  // unit test sees.
  const badChips = [];
  let chipCount = 0;
  for (const d of docs) {
    if (!d.path.endsWith(".html")) continue;
    const { html } = d;
    for (const m of html.matchAll(/class="grade-chip[^"]*"[^>]*>\s*<span aria-hidden="true">([^<]*)<\/span>/g)) {
      chipCount++;
      const letter = m[1].trim();
      if (!["A+", "A", "B", "C", "D", "F", "–"].includes(letter)) badChips.push(`${d.path}: "${letter}"`);
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
  for (const d of docs) {
    const { html } = d;
    // Each block from a link to the baseline's page up to the next one, which
    // is the row or card that link belongs to.
    // The slug boundary matters: /targets/dynamodb is a prefix of
    // /targets/dynamodb-local, whose C would otherwise read as the baseline's.
    for (const block of html.split(/(?=href="\/targets\/dynamodb["/])/).slice(1)) {
      const chip = block.slice(0, 1200).match(/grade-chip[^>]*>\s*<span[^>]*>([^<]+)</);
      if (chip && chip[1].trim() !== "–") graded.push(`${d.path}: ${chip[1].trim()}`);
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
  for (const d of docs) {
    const { html } = d;
    if (/NaN%|>undefined<|undefined%/.test(html)) broken.push(d.path);
  }
  check(broken.length === 0, "no built page renders NaN or undefined as a figure", broken.slice(0, 3).join(", "));

  // Global data does not resolve to a bare name inside a WebC component - it
  // needs $data - and the failure is silent. The legend rendered the string
  // "[object Object]" where its six bands belong and the coverage sentence
  // rendered empty, on the block a reader uses to check a letter, through every
  // test and every other build check.
  const unresolved = docs.filter((d) => d.path.endsWith(".html") && d.html.includes("[object Object]"));
  check(
    unresolved.length === 0,
    "no built page renders an object where a value belongs",
    unresolved.slice(0, 3).map((d) => d.path).join(", "),
  );

  // A movement indicator that renders an arrow has to render its reading too.
  // Both times a field was added to the model this release, the templates read
  // it before the committed fallback carried it, and the page rendered an empty
  // span: valid HTML, no error, and a row showing a bare arrow with no figure.
  // The arrow is the marker because it is the sibling that cannot be absent.
  // Counted rather than pattern-matched around the arrow: the legend colours a
  // bare glyph with the same class and has no reading to state, so anchoring on
  // the class flagged the legend. Every real indicator carries the
  // screen-reader sentence, so the two counts have to agree.
  const armless = [];
  for (const d of docs) {
    if (!d.path.endsWith(".html")) continue;
    const announced = (d.html.match(/diverged \d+\.\d+ percentage points (?:less|more)/g) ?? []).length;
    const shown = (d.html.match(/\d+\.\d+pp (?:less|more)/g) ?? []).length;
    if (announced !== shown) armless.push(`${d.path} (${shown} shown, ${announced} announced)`);
  }
  check(
    armless.length === 0,
    "a movement indicator states its reading, not just its arrow",
    armless.slice(0, 3).join(", "),
  );

  // And the legend has to carry every band, not merely avoid rendering wrong:
  // an empty loop leaves valid HTML and no letters at all.
  const home = docs.find((d) => d.path === "/index.html");
  // The full published letter set, built from the criteria rather than from
  // gradeLegendOf: iterating GRADE_BANDS missed A+ and F, and iterating the
  // legend makes the check self-referential, so dropping a letter from the
  // legend would drop it from the expectation too. The letter is escaped
  // because "A+" reads as a quantifier in a RegExp.
  const expectedLetters = ["A+", ...GRADE_BANDS.map((b) => b.letter), "F"];
  const missingBands = expectedLetters
    .filter(
      (letter) =>
        !new RegExp(`grade-chip[^"]*"[^>]*>\\s*${letter.replace(/[.*+?^${}()|[\]\\]/g, "\\$&")}\\s*<`).test(
          home?.html ?? "",
        ),
    );
  check(
    missingBands.length === 0,
    "the grade legend renders a chip for every band",
    missingBands.length ? `missing ${missingBands.join(", ")}` : "",
  );

  // Headings carry generated ids, so a fragment link can point at any section
  // without someone hand-writing an anchor first. Two anchors are still written
  // by hand because their slugs are published URLs the heading text would not
  // produce; a hand-written one whose slug matches its heading is a duplicate.
  const dupIds = [];
  const deadFrags = [];
  const idsByPage = new Map();
  for (const d of docs) {
    if (!d.path.endsWith(".html")) continue;
    const ids = [...d.html.matchAll(/\sid="([^"]+)"/g)].map((m) => m[1]);
    idsByPage.set(d.path.replace(/index\.html$/, "").replace(/\/$/, ""), new Set(ids));
    const seen = new Set();
    for (const id of ids) {
      if (seen.has(id)) dupIds.push(`${d.path}: #${id}`);
      seen.add(id);
    }
  }
  check(dupIds.length === 0, "no page repeats an element id", dupIds.slice(0, 3).join(", "));

  // And a fragment link has to land on something. The dead-link check above
  // strips fragments, so a link to a section that never got an id passed it.
  for (const d of docs) {
    if (!d.path.endsWith(".html")) continue;
    for (const m of d.html.matchAll(/href="(\/[^"#]*)#([^"]+)"/g)) {
      const page = m[1].replace(/\/$/, "");
      const ids = idsByPage.get(page);
      if (ids && !ids.has(m[2])) deadFrags.push(`${d.path} -> ${m[1]}#${m[2]}`);
    }
  }
  check(deadFrags.length === 0, "every fragment link lands on an id that exists", [...new Set(deadFrags)].slice(0, 3).join(", "));

  // The criteria date is a predicate as well as a caption: the feed reads it to
  // decide which runs predate the criteria. A page stating a different one
  // would caption history differently from the way the feed treats it. The
  // methodology renders it, and nowhere else states a date of its own.
  const criteriaLabel = gradingCriteriaEffectiveLabel();
  const methodology = docs.find((d) => d.path === "/methodology/index.html");
  check(
    Boolean(methodology?.html.includes(criteriaLabel)),
    "the methodology renders the criteria date from the constant",
    `looking for "${criteriaLabel}"`,
  );

  // Any other long-form date on a page that also talks about the criteria is a
  // second copy waiting to drift. The methodology is the one that may carry it.
  const strayDates = [];
  for (const d of docs) {
    if (!d.path.endsWith(".html") || d.path === "/methodology/index.html") continue;
    for (const m of d.html.matchAll(/\b\d{1,2} (?:January|February|March|April|May|June|July|August|September|October|November|December) 20\d\d\b/g)) {
      if (m[0] !== criteriaLabel) continue;
      // The date itself is fine to print; stating it as the criteria's effective
      // date somewhere other than the one page that owns it is not.
      const around = d.html.slice(Math.max(0, m.index - 200), m.index + 200);
      if (/criteria|in effect from/i.test(around)) strayDates.push(`${d.path}: "${m[0]}"`);
    }
  }
  check(
    strayDates.length === 0,
    "no page other than the methodology dates the criteria itself",
    strayDates.slice(0, 3).join(", "),
  );

  // The A+ premise, checked against the data this build rendered.
  //
  // A target with no failures in its headline region can still fail elsewhere.
  // The board's claim is that when it does, the behaviour is one real DynamoDB
  // splits on by region. Counts cannot settle that: three fails matching three
  // splits and three fails on unrelated behaviours are the same number. So it
  // is checked by name, against the split registry the build already fetches.
  //
  // This lived in the tooling suite, which no publishing path depends on,
  // while the methodology called it a build-time guard. It runs here now, over
  // push, dispatch and cron alike. The tooling copy stays and reads the
  // committed tree, so a breach surfaces while someone is working as well as
  // when it ships.
  const { summary, splits } = await loadPublishedData();
  const unconfirmed = [];
  const uncheckable = [];
  let guarded = 0;

  for (const [slug, t] of Object.entries(summary?.targets ?? {})) {
    const worst = t.divergenceWorst;
    const best = t.divergenceBest;
    if (best !== 0) continue; // not a zero-divergence target; the A+ claim is not about it
    guarded++;

    const failingRegions = t.regions.filter((r) => r.failed > 0).map((r) => r.region);
    if (failingRegions.length === 0) continue; // perfect everywhere; nothing to confirm

    // Fails somewhere, and the artefact did not say which tests. The check
    // cannot be done, and passing here is how it silently becomes a count
    // comparison - so this is a failure, not a skip.
    if (!t.regionFailures) {
      uncheckable.push(`${slug} fails in ${failingRegions.length} region(s) but published no test identities`);
      continue;
    }

    for (const region of failingRegions) {
      const names = t.regionFailures[region] ?? [];
      const declared = t.regions.find((r) => r.region === region)?.failed ?? 0;
      if (names.length !== declared) {
        uncheckable.push(`${slug}/${region} declares ${declared} fail(s) but names ${names.length}`);
        continue;
      }
      for (const identity of names) {
        // `<file>::<fullName>`, the suite's own test identity. Matching on the
        // name alone accepted a same-named test from a different file.
        if (!splits.some((row) => `${row.test.file}::${row.test.fullName}` === identity)) {
          unconfirmed.push(`${slug}/${region}: "${identity}"`);
        }
      }
    }

    // Confirmed splits explain the drift, but enough of them would still move
    // the letter, and the row publishes the headline one. The old tolerance was
    // the A band: three splits in a thousand tests is 0.3% against 5%, so it
    // could not bind until the registry grew seventeenfold. Comparing the two
    // letters binds from the first split that would move one.
    //
    // Coverage comes from the suite's axesOf over the headline region's counts.
    // Withdrawal is region-invariant, so only the divergence moves between the
    // two readings.
    const headlineRegion = t.regions.find((r) => r.region === t.suiteHeadlineRegion);
    const coverage = headlineRegion ? axesOf(headlineRegion).coverage : null;
    const headlineLetter = gradeOf(best, coverage)?.letter ?? null;
    const worstLetter = gradeOf(worst, coverage)?.letter ?? null;
    // A null letter on either side means the grade could not be derived, which
    // would make the comparison pass by agreeing on nothing.
    if (headlineLetter === null || worstLetter === null) {
      uncheckable.push(`${slug} published no derivable grade to compare its worst region against`);
    } else if (headlineLetter !== worstLetter) {
      unconfirmed.push(
        `${slug} publishes ${headlineLetter} from its headline region but its worst region grades ${worstLetter}`,
      );
    }
  }

  check(
    unconfirmed.length === 0,
    "a zero-divergence target fails only on confirmed regional splits",
    unconfirmed.slice(0, 3).join(", "),
  );
  check(
    uncheckable.length === 0,
    "the published data carries the evidence the A+ premise is checked from",
    uncheckable.slice(0, 3).join(", "),
  );
  // Without this the guard goes quiet on a board where nothing holds zero
  // divergence - green, having checked nothing. If it ever fails, nothing on
  // the board exercises the A+ claim and the claim should come down.
  check(guarded > 0, "the A+ premise check found a target to check", `checked ${guarded}`);
} finally {
  await rm(out, { recursive: true, force: true });
}

if (failures.length) {
  console.error(`\n${failures.length} build check(s) failed:\n${failures.map((f) => `  - ${f}`).join("\n")}\n`);
  process.exit(1);
}
console.log("\nAll build checks passed.\n");
