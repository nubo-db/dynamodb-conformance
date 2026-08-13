// No hand-typed figure beside a derived one, on the pages that argue for it.
//
// The rule the whole site rests on is that a target's figures are derived and
// never typed. The prose pages are where that rule leaks: a sentence explaining
// what divergence means reaches for a real number to explain it with, the number
// is right on the day it is written, and nothing re-reads it afterwards. Five
// separate claims drifted that way before this existed - a split count, a
// regional residue, two grading worked examples and an operation's test count -
// each of them sitting a click away from the derived figure it contradicted.
//
// So: on the pages in GUARDED_PAGES, a literal number may not sit next to a word
// that names a quantity the build derives. The scan reads the markdown source
// rather than the built page, because in the output a rendered value and a typed
// one look identical, which is the whole problem.
//
// ── The exemption mechanism ──────────────────────────────────────────────────
//
// Some literals are legitimate and always will be: a movement that happened on a
// named date, the published grade bands, an unnamed target invented to carry an
// example. Widening the pattern to let those through would let the next drift
// through with them, so they are exempted one block at a time instead, by a
// marker comment on the line before the block:
//
//     <!-- literal-figures: historical, the 24 July 2026 withdrawal -->
//
// The kind must be one of KINDS below and a note must follow it, so an exemption
// states what it is claiming rather than merely silencing the scan. `historical`
// carries one further condition, checked rather than trusted: the block it
// exempts has to name a year, or it is not a dated example and the marker is the
// wrong one for it. The markers are HTML comments, so eleventy.config.js's
// comment-stripping transform keeps them out of the built page.
//
// Adding a marker is meant to feel like a decision. If a block needs one and the
// figure in it is live, derive the figure instead.

import { readFile } from "node:fs/promises";
import { join } from "node:path";

/**
 * The pages this rule covers, repo-relative.
 *
 * Every hand-authored prose page, not only the two whose claims drifted. The
 * agent guide is the page an agent parses and it shipped a half-stated A+ gate;
 * the ground truth page states the registry size beside its evidence. Guarding
 * where the damage happened to land last time is how the next one gets through.
 */
export const GUARDED_PAGES = [
  "site/src/about.md",
  "site/src/methodology.md",
  "site/src/for-agents.md",
  "site/src/ground-truth.md",
];

// The words that name something the build derives. A number beside one of these
// is a claim about live data, whoever typed it.
// `rows` and `behaviours` are here alongside the obvious ones because the
// registry's size is never written as a count of splits in prose - it is "it
// currently holds three rows", and "0.3 points across three behaviours". Both of
// those drifted; neither mentions a split.
const DERIVED_TERMS =
  /^(splits?|divergence|diverges?|diverged|diverging|coverage|regions?|tests?|rows?|behaviours?)\b/i;

// A figure: digits, optionally decimal, optionally a unit the board prints. The
// written-out numbers are here because "three splits" drifted exactly as far as
// "3 splits" would have, and only one of the two looks like a figure. "one" and
// "zero" are deliberately absent: both are far more often a determiner or a
// definitional statement ("zero divergence", "one figure per region") than a
// count, and including them buried the real hits.
//
// The word list runs past anything countWord renders. That filter spells counts
// up to ten and prints digits above it, so a guard that stopped at twelve would
// have left "thirteen splits" through - a words filter is only safe alongside a
// scan that reads words.
const WORD_NUMBERS =
  "two|three|four|five|six|seven|eight|nine|ten|eleven|twelve|thirteen|fourteen|fifteen|sixteen|" +
  "seventeen|eighteen|nineteen|twenty|thirty|forty|fifty|sixty|seventy|eighty|ninety|hundred";
const FIGURE = new RegExp(`^(?:\\d[\\d,]*(?:\\.\\d+)?(?:%|pp)?|${WORD_NUMBERS})\\b`, "i");
// A release identifier, not a quantity. "From 2.0.0, ground truth is per region"
// read as a figure beside "region" for the same reason "Tier 1" read as one
// beside "tests".
const VERSION = /^v?\d+\.\d+\.\d+/;

const MONTHS =
  /^(january|february|march|april|may|june|july|august|september|october|november|december)\b/i;
const YEAR = /^(?:19|20)\d\d\b/;
// A number that labels something rather than counting it. The tier names and the
// version channels are identifiers; treating "Tier 1" as a figure beside "tests"
// flagged the limitations section on every run.
const LABELLED = /^(tiers?|versions?|schema|criteria)$/i;

// How near counts as beside. Five words apart is the far end of "12.3% over 80.0%
// coverage" and the near end of two clauses that merely share a sentence.
const WINDOW = 5;

/** Exemption kinds, and what each one asserts. */
export const KINDS = {
  historical: "a figure from a named past run or release; the block must state the year",
  criteria: "a published grading constant, versioned in scripts/lib/grade.mjs",
  illustrative: "an invented figure on an unnamed target, carrying no claim about the board",
  structural: "a fact about the suite's own shape rather than any target's score",
};

const MARKER = /^<!--\s*literal-figures:\s*([a-z]+)\s*,\s*(.+?)\s*-->$/i;
// Anything reaching for the mechanism without hitting it. A marker that does not
// parse would otherwise be a comment: invisible in the build, silently exempting
// nothing, and read by whoever wrote it as having worked.
const MALFORMED = /literal-figures/i;

// What is not prose: front matter, template expressions (derived by definition,
// and `{% if splits.count != 1 %}` reads as a literal beside "splits"), inline
// code, fenced code, and link targets.
function stripNonProse(text) {
  return text
    .replace(/`{3}[\s\S]*?`{3}/g, " ")
    .replace(/`[^`]*`/g, " ")
    .replace(/\{[{%][\s\S]*?[}%]\}/g, " ")
    .replace(/\]\([^)]*\)/g, "] ")
    .replace(/[*_>#·|"“”]+/g, " ");
}

function stripFrontMatter(source) {
  const m = /^---\n[\s\S]*?\n---\n/.exec(source);
  // Replaced by its own newlines rather than removed, so reported line numbers
  // still point at the file a reader opens.
  return m ? m[0].replace(/[^\n]/g, "") + source.slice(m[0].length) : source;
}

/** Split a markdown body into blank-line-separated blocks, keeping line numbers. */
function blocksOf(source) {
  const blocks = [];
  let current = null;
  for (const [i, line] of source.split("\n").entries()) {
    if (line.trim() === "") {
      current = null;
      continue;
    }
    if (!current) blocks.push((current = { start: i + 1, lines: [] }));
    current.lines.push(line);
  }
  return blocks.map((b) => ({ start: b.start, text: b.lines.join("\n") }));
}

// Adjacency is judged within a sentence. Across a full stop the two are in
// different claims, and counting them together flagged "…in every region. Three
// unrelated fails…" as though the three described the regions.
const sentencesOf = (text) => text.split(/(?<=[.!?])\s+/);

// A date is not a figure: the year in "March 2026", and the day in "24 July".
const isDate = (words, i) => YEAR.test(words[i]) || MONTHS.test(words[i + 1] ?? "");

// Every figure within WINDOW words of a derived term, reported with the words
// between them so a failure reads as the phrase it came from.
function adjacencies(text) {
  const hits = [];
  for (const sentence of sentencesOf(text)) {
    const words = sentence.split(/\s+/).filter(Boolean);
    for (const [i, word] of words.entries()) {
      if (!FIGURE.test(word) || VERSION.test(word) || isDate(words, i) || LABELLED.test(words[i - 1] ?? "")) continue;
      const from = Math.max(0, i - WINDOW);
      const to = Math.min(words.length, i + WINDOW + 1);
      const near = words.slice(from, to).findIndex((w, j) => from + j !== i && DERIVED_TERMS.test(w));
      if (near === -1) continue;
      hits.push(words.slice(Math.min(i, from + near), Math.max(i, from + near) + 1).join(" "));
    }
  }
  return hits;
}

/**
 * Every literal figure sitting beside a derived term in one page's source.
 *
 * Returns `{ findings, exemptions }`. A finding is a breach; an exemption is a
 * marker that was honoured, returned so a caller can report what the scan was
 * told to ignore rather than reporting a clean page that is half-exempt.
 */
export function scanProse(path, source) {
  const findings = [];
  const exemptions = [];
  const blocks = blocksOf(stripFrontMatter(source));

  for (const block of blocks) {
    // A marker sits on the first line of the block it exempts, with no blank
    // line between them, so the two travel together when a paragraph moves.
    const [first, ...rest] = block.text.split("\n");
    const marker = MARKER.exec(first.trim());
    const body = marker ? rest.join("\n") : block.text;
    const at = `${path}:${block.start}`;

    if (!marker && MALFORMED.test(first)) {
      findings.push(`${at}: malformed literal-figures marker, expected "<!-- literal-figures: <kind>, <note> -->"`);
    }

    if (marker) {
      const kind = marker[1].toLowerCase();
      if (!(kind in KINDS)) {
        findings.push(`${at}: unknown literal-figures kind "${kind}", expected one of ${Object.keys(KINDS).join(", ")}`);
      } else if (!body.trim()) {
        findings.push(`${at}: a literal-figures marker with no paragraph under it exempts nothing`);
      } else if (kind === "historical" && !/\b(?:19|20)\d\d\b/.test(body)) {
        // Checked against the paragraph, never the marker's own note, which is
        // free text and would otherwise let any note carrying a year through.
        findings.push(`${at}: marked historical but the paragraph names no year, so the example is not dated`);
      } else {
        exemptions.push({ path, line: block.start, kind, note: marker[2] });
        continue;
      }
    }

    for (const hit of adjacencies(stripNonProse(body))) findings.push(`${at}: "${hit}"`);
  }

  return { findings, exemptions };
}

/** Scan every guarded page. `root` is the repository root. */
export async function checkProseLiterals(root, pages = GUARDED_PAGES) {
  const findings = [];
  const exemptions = [];
  for (const page of pages) {
    const result = scanProse(page, await readFile(join(root, page), "utf8"));
    findings.push(...result.findings);
    exemptions.push(...result.exemptions);
  }
  return { findings, exemptions };
}
