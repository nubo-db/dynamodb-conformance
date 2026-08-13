import { test } from "node:test";
import assert from "node:assert/strict";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

import { buildSuiteShape, carriedEach, countWord, shapeOf } from "./suite-shape.mjs";

const MANIFEST = join(dirname(fileURLToPath(import.meta.url)), "..", "..", "registry", "suite-manifest.json");

const fixture = {
  count: 6,
  tests: [
    "tests/tier1/putItem/basic.test.ts::PutItem writes an item",
    "tests/tier1/putItem/validation.test.ts::PutItem rejects an empty key",
    "tests/tier1/putItem/conditions.test.ts::PutItem honours a condition",
    "tests/tier2/account/describe.test.ts::Account describes limits",
    "tests/tier2/resourcePolicy/basic.test.ts::ResourcePolicy attaches",
    "tests/tier3/error-messages/putItem.test.ts::PutItem error wording",
  ],
};

test("tests are counted by the operation directory under the tier", () => {
  const shape = shapeOf(fixture);
  assert.equal(shape.available, true);
  assert.equal(shape.size, 6);
  assert.equal(shape.byOperation.putItem, 3);
  assert.equal(shape.byOperation.account, 1);
  assert.equal(shape.byOperation["error-messages"], 1);
});

test("a missing or empty manifest degrades rather than failing the build", () => {
  for (const shape of [buildSuiteShape("/nowhere/suite-manifest.json"), shapeOf({ count: 0, tests: [] }), shapeOf({})]) {
    assert.deepEqual(shape, { available: false, size: null, byOperation: {} });
    assert.equal(carriedEach(shape, ["putItem"]), "");
  }
});

test("the committed manifest is what the pages actually read", () => {
  const shape = buildSuiteShape(MANIFEST);
  assert.equal(shape.available, true);
  assert.ok(shape.size > 0);
  // The three the methodology names by hand. They are identifiers rather than
  // figures, so they stay in the prose - but the sentence breaks if one is
  // renamed away, and this is where that surfaces.
  for (const op of ["putItem", "account", "resourcePolicy", "contributorInsights"]) {
    assert.ok(shape.byOperation[op] > 0, `the manifest carries no tests under ${op}`);
  }
});

test("equal counts read as 'each', unequal ones are spelled out", () => {
  const shape = shapeOf(fixture);
  assert.equal(carriedEach(shape, ["account", "resourcePolicy"]), "one each");
  assert.equal(carriedEach(shape, ["putItem", "account"]), "three and one");
  // An operation the manifest does not carry renders the clause away rather
  // than printing a gap where a count belongs.
  assert.equal(carriedEach(shape, ["account", "nosuchoperation"]), "");
  assert.equal(carriedEach(shape, []), "");
});

test("small counts read as words, larger ones as digits", () => {
  assert.equal(countWord(2), "two");
  assert.equal(countWord(10), "ten");
  assert.equal(countWord(129), "129");
});
