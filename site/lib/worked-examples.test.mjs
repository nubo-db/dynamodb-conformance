import { test } from "node:test";
import assert from "node:assert/strict";

import {
  cappedRows,
  cheapestWithdrawal,
  gradedRows,
  regionalSpread,
  renderCappedExamples,
  renderCheapestWithdrawal,
} from "./worked-examples.mjs";

// The examples the methodology prints are chosen from the live board, so on any
// given day these helpers describe whatever happens to be on it. Fixtures fix
// that: the selection rules, the arithmetic and the degradation are asserted
// against a board that does not move, and the page then applies them to one
// that does.

const SUITE = 1000;

// Best letter first. Restated here rather than imported so the bands-lost
// assertion below is checking the module's output against the published order
// rather than against the module's own copy of it.
const BANDS = ["A+", "A", "B", "C", "D", "F"];

// failed/skipped are the headline region's; `spread` adds that many extra fails
// to a second region, which is the residue the split registry has to explain.
const target = (slug, { failed, skipped, spread = 0 }) => ({
  slug,
  suiteHeadlineRegion: "eu-west-2",
  divergenceBest: (failed / SUITE) * 100,
  divergenceWorst: ((failed + spread) / SUITE) * 100,
  regions: [
    { region: "eu-west-2", passed: SUITE - failed - skipped, failed, skipped, indeterminate: 0, count: SUITE },
    {
      region: "us-east-1",
      passed: SUITE - failed - spread - skipped,
      failed: failed + spread,
      skipped,
      indeterminate: 0,
      count: SUITE,
    },
  ],
});

const board = (entries) => ({
  available: true,
  targets: Object.fromEntries(entries.map(([slug, spec]) => [slug, target(slug, spec)])),
});

// Coverage 77%, divergence 12.8%: a B on divergence alone, read up to an
// effective 20.5 and graded C. The shape of the live Dynalite row.
const CAPPED_HARD = ["dynalite", { failed: 128, skipped: 230 }];
// Coverage 90%, divergence 0.9%: an A read up to 4.2 - still an A, so this row
// is not capped and must not be offered as an example of one.
const UNCAPPED = ["ministack", { failed: 9, skipped: 100 }];
// Coverage 83.4%, divergence 0.9%: an A read up to 6.4 and graded B.
const CAPPED_SOFT = ["dynoxide-wasm", { failed: 9, skipped: 166, spread: 5 }];
// Coverage 95.3%, divergence 14.8%: a B read up to 16.4 and graded C. Capped
// like the other two and by the same one band, but nowhere near as hard - which
// is what makes it the row the selection has to leave out.
const CAPPED_WIDE = ["localstack", { failed: 148, skipped: 47 }];

test("the baseline is not a graded row", () => {
  const rows = gradedRows(board([CAPPED_HARD, ["dynamodb", { failed: 0, skipped: 0 }]]));
  assert.deepEqual(rows.map((r) => r.slug), ["dynalite"]);
});

test("an unavailable summary yields no examples rather than throwing", () => {
  for (const model of [null, undefined, { available: false }, { available: true }]) {
    assert.deepEqual(gradedRows(model), []);
    assert.equal(renderCappedExamples(model), "");
    assert.equal(regionalSpread(model?.available ? model : null), null);
  }
});

test("only rows coverage actually lowered are offered as capped examples", () => {
  const rows = cappedRows(board([CAPPED_HARD, UNCAPPED, CAPPED_SOFT]));
  assert.deepEqual(rows.map((r) => r.slug), ["dynalite", "dynoxide-wasm"]);
  // Lowest coverage first, and a coverage tie falls to the slug so a rebuild
  // cannot reorder the examples for reasons a reader could not see.
  assert.equal(rows[0].base, "B");
  assert.equal(rows[0].letter, "C");
  assert.equal(rows[0].effective, 20.5);
  assert.equal(rows[1].base, "A");
  assert.equal(rows[1].letter, "B");
});

test("the worked example states the figures the bands actually read", () => {
  const sentence = renderCappedExamples(board([CAPPED_HARD, CAPPED_SOFT]));
  assert.match(
    sentence,
    /Dynalite diverges 12\.8%, the B band on its own, but implements 77\.0%, which reads it up to an effective 20\.5 and grades it \*\*C\*\*\./,
  );
  // A variant is named the way the standings nest it, not by its README name.
  assert.match(sentence, /Dynoxide's WebAssembly \/ OPFS build diverges 0\.9%/);
});

test("the lead says there was nothing to choose between when there wasn't", () => {
  // Two capped rows and both shown: no row was left out, so claiming a ranking
  // would send a reader looking for the one that lost.
  assert.match(
    renderCappedExamples(board([CAPPED_HARD, CAPPED_SOFT])),
    /^Picked by rule rather than by hand, the two capped rows on the board:/,
  );
  assert.match(renderCappedExamples(board([CAPPED_HARD])), /^Picked by rule rather than by hand, the only capped row on the board:/);
  assert.equal(renderCappedExamples(board([UNCAPPED])), "");
});

// The board is maintained by the author of one of its engines, so a sentence
// naming a real target has to say who chose it - and the rule it states has to
// be the one that actually did. "Where coverage costs the most bands" was true
// of the code and useless on the page: the weight is a third of what a target
// declines, so crossing two bands takes a gap most rows never reach, and a
// reader checking it found every capped row tied at one and no way to derive the
// two named.
test("the stated rule is the one that picks the rows out", () => {
  const three = board([CAPPED_HARD, CAPPED_SOFT, CAPPED_WIDE]);
  // All three lose exactly one band, so bands cannot be what chose two of them.
  const bandsLost = cappedRows(three).map((r) => BANDS.indexOf(r.letter) - BANDS.indexOf(r.base));
  assert.deepEqual(bandsLost, [1, 1, 1]);

  const sentence = renderCappedExamples(three);
  assert.match(sentence, /^Picked by rule rather than by hand, the two capped rows with the lowest coverage:/);
  // And the two named are the two with the lowest coverage, not the two the
  // sentence happens to lead with.
  assert.match(sentence, /Dynalite diverges/);
  assert.match(sentence, /Dynoxide's WebAssembly \/ OPFS build diverges/);
  assert.equal(/LocalStack/.test(sentence), false);
});

test("the cheapest-letter sentence states its rule too", () => {
  assert.match(
    renderCheapestWithdrawal(board([CAPPED_HARD, CAPPED_SOFT])),
    /^Whichever row currently sits closest to a band it has not earned is the one this costs least: on the current board that is /,
  );
});

test("the cheapest letter is the fewest withdrawals that move any row up a band", () => {
  // 128 fails at 77% coverage grades C. Withdrawing k drops the effective figure
  // by two thirds of k/1000, so it takes 83 to cross 15.0 and reach B.
  const cheapest = cheapestWithdrawal(board([CAPPED_HARD, CAPPED_SOFT]));
  assert.equal(cheapest.slug, "dynalite");
  assert.equal(cheapest.from, "C");
  assert.equal(cheapest.to, "B");
  assert.equal(cheapest.tests, 83);
  assert.match(renderCheapestWithdrawal(board([CAPPED_HARD])), /that is Dynalite, at 83 withdrawn tests\.$/);
});

test("the cheapest letter is decided at the band edge, not by a continuous estimate", () => {
  // A row whose withdrawal lands the effective figure on exactly 5.0 has not
  // reached A: the band is `under 5`. Reading it off the arithmetic rather than
  // off gradeOf undercounts by one test, which on a board where two rows sit one
  // test apart is the difference between naming one target and another.
  // 5 fails at 86.4% coverage: one withdrawal gives an effective 5.0 and holds
  // at B, two give 4.9 and reach A.
  const edge = board([["extenddb", { failed: 5, skipped: 136 }]]);
  assert.equal(cheapestWithdrawal(edge).tests, 2);
});

test("a board where no letter can be bought says so rather than printing nothing", () => {
  // 9 fails at 90% coverage already grades A on the effective figure, and A+ is
  // shut off by the coverage half of its gate, so there is no band to reach.
  assert.equal(cheapestWithdrawal(board([UNCAPPED])), null);
  assert.match(renderCheapestWithdrawal(board([UNCAPPED])), /^No letter on the current board can be bought/);
});

test("the regional spread is the widest gap on the board, in tests and in points", () => {
  const spread = regionalSpread(board([CAPPED_HARD, CAPPED_SOFT]));
  assert.equal(spread.slug, "dynoxide-wasm");
  assert.equal(spread.tests, 5);
  assert.equal(spread.points, 0.5);
  assert.equal(spread.suite, SUITE);
});
