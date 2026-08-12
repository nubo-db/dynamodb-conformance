import { test } from "node:test";
import assert from "node:assert/strict";

import { checkAPlusPremise } from "./premise.mjs";

// No row has held zero divergence since 2026-08-12, so on the live board this
// rule runs on nothing and passes. That is honest, and check-build says so
// rather than reporting a green check - but it means the board cannot tell
// anyone whether the rule still works. These fixtures can.

const SPLIT = {
  id: "batch-get-item-empty-request-items-message",
  test: { file: "tests/tier3/error-messages/batchGetItem.test.ts", fullName: "BatchGetItem rejects empty RequestItems" },
};
const CONFIRMED = `${SPLIT.test.file}::${SPLIT.test.fullName}`;
const UNRELATED = "tests/tier1/putItem/basic.test.ts::PutItem writes an item";

// A target that is clean in its headline region and fails in one other.
//
// Coverage is 80%, which caps the row at B. That is deliberate: at full
// coverage a zero-divergence row grades A+ and any regional failure at all
// makes its worst region grade A, so the letter comparison fires before the
// registry matching is ever reached. Capping the row puts both readings in the
// same band and leaves the registry rule as the thing under test. The A+ case
// gets its own test at the bottom, because tripping is what it is meant to do.
const target = ({ failed, names, worst, coverage = 800 }) => ({
  targets: {
    alpha: {
      suiteHeadlineRegion: "eu-west-2",
      divergenceBest: 0,
      divergenceWorst: worst,
      regions: [
        { region: "eu-west-2", passed: coverage, failed: 0, skipped: 1000 - coverage, indeterminate: 0, count: 1000 },
        { region: "us-east-1", passed: coverage - failed, failed, skipped: 1000 - coverage, indeterminate: 0, count: 1000 },
      ],
      ...(names ? { regionFailures: { "us-east-1": names } } : {}),
    },
  },
});

test("a failure the registry records is confirmed", () => {
  const r = checkAPlusPremise(target({ failed: 1, names: [CONFIRMED], worst: 0.1 }), [SPLIT]);
  assert.equal(r.guarded, 1, "the row must actually have been checked");
  assert.deepEqual(r.unconfirmed, []);
  assert.deepEqual(r.uncheckable, []);
});

test("a failure the registry does not record is a breach", () => {
  // The case the guard exists for: zero divergence in the headline region,
  // failing elsewhere on a behaviour nobody has confirmed real AWS splits on.
  const r = checkAPlusPremise(target({ failed: 1, names: [UNRELATED], worst: 0.1 }), [SPLIT]);
  assert.equal(r.guarded, 1);
  assert.equal(r.unconfirmed.length, 1);
  assert.match(r.unconfirmed[0], /PutItem writes an item/);
});

test("a same-named test in a different file does not count as confirmed", () => {
  // Matching on the title alone accepted this. The identity is file-scoped
  // because fullName is only unique within a file.
  const elsewhere = `tests/tier3/error-messages/other.test.ts::${SPLIT.test.fullName}`;
  const r = checkAPlusPremise(target({ failed: 1, names: [elsewhere], worst: 0.1 }), [SPLIT]);
  assert.equal(r.unconfirmed.length, 1);
});

test("failing somewhere without naming the tests is uncheckable, not a pass", () => {
  // Passing on absent evidence is how a check by name silently becomes a check
  // by count, which is the thing this guard was written to stop being.
  const r = checkAPlusPremise(target({ failed: 3, names: null, worst: 0.3 }), [SPLIT]);
  assert.equal(r.guarded, 1);
  assert.equal(r.unconfirmed.length, 0);
  assert.equal(r.uncheckable.length, 1);
  assert.match(r.uncheckable[0], /published no test identities/);
});

test("naming fewer tests than the row declares is uncheckable", () => {
  const r = checkAPlusPremise(target({ failed: 3, names: [CONFIRMED], worst: 0.3 }), [SPLIT]);
  assert.match(r.uncheckable[0], /declares 3 fail\(s\) but names 1/);
});

test("enough confirmed splits to move the letter is still a breach", () => {
  // Confirmed splits explain the drift but do not license any amount of it:
  // the row publishes its headline letter, and this compares that against what
  // its worst region would grade. Every one of these 90 failures is recorded,
  // so the registry rule is satisfied and the letter rule is what binds.
  const names = Array.from({ length: 90 }, () => CONFIRMED);
  const r = checkAPlusPremise(target({ failed: 90, names, worst: 9 }), [SPLIT]);
  assert.equal(r.unconfirmed.length, 1);
  assert.match(r.unconfirmed[0], /publishes B from its headline region but its worst region grades C/);
});

test("an A+ row failing anywhere at all trips the letter comparison", () => {
  // Documented behaviour, not an accident. At full coverage a zero-divergence
  // row is A+ and its worst region can only be A or lower, so the first target
  // to earn A+ while failing any regional split will fail this check. That is
  // the claim the guard exists to question - read it as a prompt to revisit the
  // criteria in the open, not as a defect in the target that tripped it.
  const r = checkAPlusPremise(
    target({ failed: 1, names: [CONFIRMED], worst: 0.1, coverage: 1000 }),
    [SPLIT],
  );
  assert.equal(r.guarded, 1);
  assert.equal(r.unconfirmed.length, 1);
  assert.match(r.unconfirmed[0], /publishes A\+ from its headline region but its worst region grades A/);
});

test("a row that is perfect everywhere needs nothing confirmed", () => {
  const r = checkAPlusPremise(target({ failed: 0, names: null, worst: 0 }), [SPLIT]);
  assert.equal(r.guarded, 1);
  assert.deepEqual(r.unconfirmed, []);
  assert.deepEqual(r.uncheckable, []);
});

test("a row that diverges in its headline region is not in scope", () => {
  const summary = target({ failed: 1, names: [UNRELATED], worst: 0.5 });
  summary.targets.alpha.divergenceBest = 0.2;
  const r = checkAPlusPremise(summary, [SPLIT]);
  assert.equal(r.guarded, 0, "the premise is only about zero-divergence rows");
  assert.deepEqual(r.unconfirmed, []);
});

test("a registry row carrying no test identity confirms nothing", () => {
  const r = checkAPlusPremise(target({ failed: 1, names: [UNRELATED], worst: 0.1 }), [{ id: "shapeless" }]);
  assert.equal(r.unconfirmed.length, 1);
});

test("an empty board reports nothing checked rather than a clean pass", () => {
  const r = checkAPlusPremise({ targets: {} }, [SPLIT]);
  assert.equal(r.guarded, 0);
  assert.deepEqual(r.unconfirmed, []);
});
