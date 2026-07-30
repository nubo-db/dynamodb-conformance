import { describe, it, expect } from 'vitest'
import { failureSet, lineageReport, similarity } from './lineage.mjs'

const doc = (files) => ({
  testResults: Object.entries(files).map(([name, assertions]) => ({
    name,
    assertionResults: assertions.map(([fullName, status]) => ({ fullName, status })),
  })),
})

const set = (...names) => new Set(names)

describe('failureSet', () => {
  it('collects only the failures, keyed by file and test', () => {
    const f = failureSet(
      doc({
        '/ci/checkout/tests/tier1/a.test.ts': [
          ['passes', 'passed'],
          ['breaks', 'failed'],
          ['declines', 'skipped'],
        ],
      }),
    )
    expect([...f]).toEqual(['tests/tier1/a.test.ts::breaks'])
  })

  it('normalises the path so two runs from different checkouts compare', () => {
    // One target's results come from CI, another from a laptop; the absolute
    // prefix differs and would make every pair look unrelated.
    const ci = failureSet(doc({ '/home/runner/work/repo/tests/tier1/a.test.ts': [['x', 'failed']] }))
    const local = failureSet(doc({ '/Users/someone/repo/tests/tier1/a.test.ts': [['x', 'failed']] }))
    expect([...ci]).toEqual([...local])
  })

  it('is empty for a document that failed nothing', () => {
    expect(failureSet(doc({ 'tests/a.test.ts': [['x', 'passed']] })).size).toBe(0)
  })
})

describe('similarity', () => {
  it('is 100% for identical failure sets', () => {
    expect(similarity(set('a', 'b'), set('a', 'b'))).toBe(100)
  })

  it('is 0% when nothing is shared', () => {
    expect(similarity(set('a'), set('b'))).toBe(0)
  })

  it('charges for the failures they do not share', () => {
    // Raw overlap would call this 100% of the smaller set; Jaccard says 50%,
    // which is the point - independent engines differ in what they get wrong,
    // and that difference is the signal.
    expect(similarity(set('a'), set('a', 'b'))).toBe(50)
  })

  it('says nothing about a target that failed nothing', () => {
    // A perfect target has no divergences to inherit or share, so the measure
    // is undefined rather than zero.
    expect(similarity(set(), set('a'))).toBeNull()
  })
})

describe('lineageReport', () => {
  // Three unrelated targets that overlap a little (the suite's hard cases are
  // hard for everyone), plus one pair that fails almost identically.
  const common = ['h1', 'h2', 'h3']
  const targets = [
    { slug: 'alpha', failures: set(...common, 'a1', 'a2', 'a3', 'a4') },
    { slug: 'beta', failures: set(...common, 'b1', 'b2', 'b3', 'b4') },
    { slug: 'gamma', failures: set(...common, 'g1', 'g2', 'g3', 'g4') },
    { slug: 'delta', failures: set(...common, 'g1', 'g2', 'g3', 'g4', 'd1') },
  ]

  it('ranks pairs and flags the one that shares an implementation', () => {
    const { pairs } = lineageReport(targets)
    expect(pairs[0]).toMatchObject({ a: 'gamma', b: 'delta', flagged: true })
    // The merely-hard-for-everyone overlaps are not flagged.
    expect(pairs.filter((p) => p.flagged).map((p) => `${p.a}/${p.b}`)).toEqual(['gamma/delta'])
  })

  it('derives its threshold from the spread rather than a fixed number', () => {
    const { baseline, deviation, threshold } = lineageReport(targets)
    expect(threshold).toBeGreaterThan(baseline)
    expect(threshold).toBeCloseTo(baseline + 3 * deviation, 6)
  })

  it('excludes two builds of one project, which are related by construction', () => {
    const sameProject = (a, b) => a.startsWith('gamma') && b.startsWith('gamma')
    const withVariant = [
      ...targets.slice(0, 3),
      { slug: 'gamma-wasm', failures: set(...common, 'g1', 'g2', 'g3', 'g4') },
    ]
    const { pairs } = lineageReport(withVariant, { sameProject })
    // The pair is absent entirely: leaving it in would drag the baseline up and
    // mask a genuine find elsewhere.
    expect(pairs.some((p) => p.a === 'gamma' && p.b === 'gamma-wasm')).toBe(false)
  })

  it('leaves out a target that failed nothing', () => {
    const { pairs } = lineageReport([...targets, { slug: 'perfect', failures: set() }])
    expect(pairs.some((p) => p.a === 'perfect' || p.b === 'perfect')).toBe(false)
  })

  it('reports nothing rather than throwing when no target failed anything', () => {
    const { pairs, threshold } = lineageReport([{ slug: 'a', failures: set() }])
    expect(pairs).toEqual([])
    expect(threshold).toBeNull()
  })
})
