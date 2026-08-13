// The methodology's worked examples, derived from the board they describe.
//
// A grading rule explained in the abstract is hard to check, so the page works
// it through on real rows. Typed in, those rows go stale within a fortnight and
// the page ends up contradicting the standings a click above it - which it did,
// naming a target as diverging nowhere while the front page showed it diverging.
//
// So the examples are chosen from the data at build time rather than named in
// prose. Each helper returns the figures; the render functions turn them into
// the sentence the page prints, in the same place the phrasing already lives for
// the control strip and the coverage share.
//
// Every helper reads the headline region, which is the region a published row is
// scored on, and skips the baseline: real DynamoDB is not graded, so it can
// neither be capped nor buy a letter.

import {
  GROUND_TRUTH_SLUG,
  asPct,
  axesOf,
  configurationOf,
  display,
  effectiveOf,
  gradeOf,
  isVariant,
  projectOf,
} from "./scoring.mjs";

// Best letter first, so "moved up a band" is an index that fell.
const ORDER = ["A+", "A", "B", "C", "D", "F"];

// How a row is named in a sentence. A variant's display name is its README form
// ("Dynoxide (wasm)"), which is not the string the standings show it under - the
// board nests it as a configuration beneath its project - so a reader sent
// looking for it would not find it. Named from the two parts the board uses.
const nameOf = (slug) =>
  isVariant(slug) && configurationOf(slug)
    ? `${display(projectOf(slug))}'s ${configurationOf(slug)} build`
    : display(slug);

// A target's published row: the two figures the bands read, plus the raw counts
// a withdrawal is simulated on.
function rowOf(target) {
  const regions = target?.regions ?? [];
  const r = regions.find((x) => x.region === target.suiteHeadlineRegion) ?? regions[0];
  if (!r) return null;
  const { divergence, coverage } = axesOf(r);
  if (divergence == null || coverage == null) return null;
  return {
    slug: target.slug,
    name: nameOf(target.slug),
    divergence,
    coverage,
    failed: r.failed,
    skipped: r.skipped,
    count: r.count,
  };
}

/** Every graded row on a summary model, baseline excluded. */
export function gradedRows(model) {
  if (!model?.available) return [];
  return Object.values(model.targets ?? {})
    .filter((t) => t.slug !== GROUND_TRUTH_SLUG)
    .map(rowOf)
    .filter(Boolean)
    .sort((a, b) => a.divergence - b.divergence || a.slug.localeCompare(b.slug));
}

/**
 * The rows where coverage is holding the letter down, lowest coverage first.
 *
 * Ranked on coverage alone, because that is the only ordering a reader can
 * reproduce from the board. Ranking on bands lost first reads better and
 * discriminates nothing: the weight is a third of what a target declines, so
 * crossing two bands takes a coverage gap most rows never reach, and today all
 * four capped rows lose exactly one. The paragraph would then have named two of
 * four with the real tiebreak - coverage - unstated, which is the gap the
 * sentence exists to close.
 *
 * Ties on coverage fall to the slug, so a rebuild cannot reorder the examples.
 */
export function cappedRows(model) {
  return gradedRows(model)
    .map((row) => {
      const grade = gradeOf(row.divergence, row.coverage);
      return {
        ...row,
        letter: grade.letter,
        base: gradeOf(row.divergence, 100).letter,
        capped: grade.capped,
        effective: effectiveOf(Number(row.divergence.toFixed(1)), Number(row.coverage.toFixed(1))),
      };
    })
    .filter((row) => row.capped)
    .sort((a, b) => a.coverage - b.coverage || a.slug.localeCompare(b.slug));
}

// Every sentence below names a real target, and which target it names is decided
// by a rule rather than by whoever wrote the paragraph. That has to be on the
// page. A board maintained by the author of one of its engines cannot print "the
// cheapest letter is a competitor's" and leave the reader to work out that
// nobody chose it - and the same rule will put the author's own engine there on
// a different board, which only reads as fair if the rule was stated first.
const COUNT = ["no", "one", "two"];

/** The paragraph's worked examples: up to two capped rows, as one sentence. */
export function renderCappedExamples(model, limit = 2) {
  const all = cappedRows(model);
  const rows = all.slice(0, limit);
  if (!rows.length) return "";
  const sentences = rows.map(
    (row) =>
      `${row.name} diverges ${asPct(row.divergence)}, the ${row.base} band on its own, ` +
      `but implements ${asPct(row.coverage)}, which reads it up to an effective ` +
      `${row.effective.toFixed(1)} and grades it **${row.letter}**.`,
  );
  // Naming the rule only helps if the rule picks the rows out. "Lowest coverage"
  // does; where there is nothing to choose between, say that instead of implying
  // a ranking the reader would find no evidence of.
  const rule =
    all.length <= rows.length
      ? `the ${rows.length === 1 ? "only capped row" : `${COUNT[rows.length] ?? rows.length} capped rows`} on the board`
      : `the ${COUNT[rows.length] ?? rows.length} capped rows with the lowest coverage`;
  return `Picked by rule rather than by hand, ${rule}: ${sentences.join(" ")}`;
}

/**
 * The cheapest letter on the board, in tests withdrawn.
 *
 * Withdrawing a failing test drops divergence and coverage by the same amount,
 * so the effective figure falls by (1 - 1/divisor) of it and a target still
 * gains a little. How little is a measurement of today's board rather than a
 * property of the criteria, so it is counted here instead of stated.
 */
export function cheapestWithdrawal(model) {
  let cheapest = null;
  for (const row of gradedRows(model)) {
    const letterAfter = (k) =>
      gradeOf(((row.failed - k) / row.count) * 100, ((row.count - row.skipped - k) / row.count) * 100).letter;
    const now = letterAfter(0);
    for (let k = 1; k <= row.failed; k++) {
      if (ORDER.indexOf(letterAfter(k)) >= ORDER.indexOf(now)) continue;
      if (!cheapest || k < cheapest.tests) cheapest = { ...row, tests: k, from: now, to: letterAfter(k) };
      break;
    }
  }
  return cheapest;
}

/** That measurement as the sentence the page prints, or "" when nothing can buy one. */
export function renderCheapestWithdrawal(model) {
  const c = cheapestWithdrawal(model);
  if (!c) return "No letter on the current board can be bought this way at any price.";
  // Named as the row the count lands on, not as a target singled out. It is
  // decided by a single test today - the next row up needs one more - so it
  // changes hands on almost any movement, and a reader who saw a different name
  // last week needs the rule rather than a reason to wonder.
  return (
    `Whichever row currently sits closest to a band it has not earned is the one this costs least: ` +
    `on the current board that is ${c.name}, at ${c.tests} withdrawn tests.`
  );
}

/**
 * How far the regions disagree about any one target, in tests and in points.
 *
 * Both figures are maxima over the board, so the page states them as a ceiling
 * ("no row exceeds this") rather than naming the row they came from. That is a
 * stronger claim, it is checkable against any row rather than one, and it does
 * not need a selection rule: the widest spread is currently a tie between an
 * engine and its own variant, and a sentence naming one of the two would flip on
 * a rebuild for reasons no reader could see. The slug tiebreak below keeps the
 * model stable for anything that does want the row.
 */
export function regionalSpread(model) {
  if (!model?.available) return null;
  let widest = null;
  for (const t of Object.values(model.targets ?? {})) {
    if (t.slug === GROUND_TRUTH_SLUG) continue;
    const failed = (t.regions ?? []).map((r) => r.failed);
    if (!failed.length || t.divergenceBest == null || t.divergenceWorst == null) continue;
    const tests = Math.max(...failed) - Math.min(...failed);
    const better = !widest || tests > widest.tests || (tests === widest.tests && t.slug < widest.slug);
    if (!better) continue;
    widest = {
      slug: t.slug,
      name: nameOf(t.slug),
      tests,
      points: Number((t.divergenceWorst - t.divergenceBest).toFixed(1)),
      suite: (t.regions ?? [])[0]?.count ?? null,
    };
  }
  return widest;
}
