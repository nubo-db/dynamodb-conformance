import { describe, it, expect } from 'vitest'
import { parseArgs, uncovered } from './ground-truth-coverage.mjs'
import { relativeTestPath, testIdentities } from './lib/identity.mjs'

// Minimal Vitest-shaped result: { '<file>': ['fullName', ...] }.
const doc = (files) => ({
  testResults: Object.entries(files).map(([name, names]) => ({
    name,
    assertionResults: names.map((fullName) => ({ fullName, status: 'passed' })),
  })),
})

const FULL = doc({
  '/runner/repo/tests/tier1/putItem/basic.test.ts': ['PutItem > writes an item'],
  '/runner/repo/tests/tier2/updateTable/gsi.test.ts': ['UpdateTable — add GSI > adds a hash-only GSI'],
  '/runner/repo/tests/tier2/kinesis/streamingDestination.test.ts': ['Kinesis > enables a destination'],
})

describe('relativeTestPath', () => {
  it('reduces a runner-absolutised path to its repo-relative form', () => {
    expect(relativeTestPath('/home/runner/work/x/y/tests/tier1/a.test.ts')).toBe(
      'tests/tier1/a.test.ts',
    )
  })

  it('leaves a path with no tests/ segment alone rather than mangling it', () => {
    expect(relativeTestPath('weird.test.ts')).toBe('weird.test.ts')
  })
})

describe('testIdentities', () => {
  it('keys on file and full name together', () => {
    expect(testIdentities(doc({ '/r/tests/tier1/a.test.ts': ['s > t'] }))).toEqual(
      new Set(['tests/tier1/a.test.ts::s > t']),
    )
  })

  it('does not conflate same-named tests living in different files', () => {
    const ids = testIdentities(
      doc({
        '/r/tests/tier1/a.test.ts': ['basic > rejects a missing key'],
        '/r/tests/tier1/b.test.ts': ['basic > rejects a missing key'],
      }),
    )
    // Two distinct identities, not one: collapsing them would let a test in b
    // be marked observed on the strength of a run that only covered a.
    expect(ids.size).toBe(2)
  })

  it('rejects a document that is not a Vitest result', () => {
    expect(() => testIdentities({ schema: 1 })).toThrow(/missing testResults/)
  })
})

describe('uncovered', () => {
  it('reports nothing when the lanes together cover the whole suite', () => {
    const gating = doc({ '/ci/tests/tier1/putItem/basic.test.ts': ['PutItem > writes an item'] })
    const gsi = doc({
      '/ci/tests/tier2/updateTable/gsi.test.ts': ['UpdateTable — add GSI > adds a hash-only GSI'],
    })
    const integrations = doc({
      '/ci/tests/tier2/kinesis/streamingDestination.test.ts': ['Kinesis > enables a destination'],
    })
    expect(uncovered(testIdentities(FULL), [gating, gsi, integrations])).toEqual([])
  })

  it('names the tests a missing lane leaves unobserved', () => {
    // The GSI lane did not run: its 1 test has no real-AWS observation, which
    // is exactly the silence this check exists to break.
    const gating = doc({ '/ci/tests/tier1/putItem/basic.test.ts': ['PutItem > writes an item'] })
    const integrations = doc({
      '/ci/tests/tier2/kinesis/streamingDestination.test.ts': ['Kinesis > enables a destination'],
    })
    expect(uncovered(testIdentities(FULL), [gating, integrations])).toEqual([
      'tests/tier2/updateTable/gsi.test.ts::UpdateTable — add GSI > adds a hash-only GSI',
    ])
  })

  it('counts a failed test as observed: a red answer is still an answer', () => {
    const observedButRed = {
      testResults: [
        {
          name: '/ci/tests/tier1/putItem/basic.test.ts',
          assertionResults: [{ fullName: 'PutItem > writes an item', status: 'failed' }],
        },
      ],
    }
    const reference = doc({
      '/r/tests/tier1/putItem/basic.test.ts': ['PutItem > writes an item'],
    })
    expect(uncovered(testIdentities(reference), [observedButRed])).toEqual([])
  })

  it('matches across checkouts, where absolute paths differ', () => {
    const reference = doc({ '/home/runner/work/a/b/tests/tier1/a.test.ts': ['s > t'] })
    const groundTruth = doc({ '/Users/dev/Projects/repo/tests/tier1/a.test.ts': ['s > t'] })
    expect(uncovered(testIdentities(reference), [groundTruth])).toEqual([])
  })

  it('reports the whole suite when no ground truth is supplied at all', () => {
    expect(uncovered(testIdentities(FULL), [])).toHaveLength(3)
  })
})

describe('parseArgs', () => {
  it('takes every argument as a ground-truth run', () => {
    expect(parseArgs(['gt.json', 'gsi.json'])).toEqual({ files: ['gt.json', 'gsi.json'] })
  })
})

describe('the suite comes from the manifest, not from a measured run', () => {
  it('a truncated emulator run cannot shrink the population and mask a gap', () => {
    // This used to reconcile against whichever emulator run was widest, so
    // dynalite dying after one test could make the check pass by comparing the
    // ground truth against a one-test suite. The manifest has no such failure
    // mode: it is the suite's own enumeration, not a measurement of it.
    expect(uncovered(testIdentities(FULL), [])).toHaveLength(3)
  })
})
