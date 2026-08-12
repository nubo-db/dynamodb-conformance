/**
 * How a test is named, everywhere it has to be named the same.
 *
 * The suite manifest, the publish guards, the lane merge and the real-AWS
 * reconciliation all have to agree on what "the same test" means, or they
 * check each other's populations by a key that only mostly matches. That
 * agreement lives here rather than in whichever script defined it first.
 */

/**
 * Every test identity in a Vitest JSON document, as `<file>::<fullName>`.
 *
 * Keyed on the file path as well as the name because `fullName` is only unique
 * within a file - two files may both have a `basic > rejects a missing key` -
 * and a collision here would silently mark an unobserved test as covered. File
 * paths are absolutised by the runner, so they are reduced to their repo
 * relative form first: the runs being compared come from different checkouts
 * on different machines.
 */
export function testIdentities(doc) {
  if (!Array.isArray(doc?.testResults)) {
    throw new Error('not a Vitest JSON result: missing testResults')
  }
  const ids = new Set()
  for (const tr of doc.testResults) {
    for (const ar of tr.assertionResults ?? []) {
      ids.add(`${relativeTestPath(tr.name)}::${ar.fullName}`)
    }
  }
  return ids
}

/** Reduce a runner-absolutised path to its `tests/...` form. */
export function relativeTestPath(name) {
  const at = name.indexOf('tests/')
  return at === -1 ? name : name.slice(at)
}
