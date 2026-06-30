import { describe, it, expect } from 'vitest'
import { GROUND_TRUTH_SLUG, isPublishedTarget, passRate, scoreResults, tierOf } from './score.mjs'

// Build a minimal Vitest-shaped result: one test file per named tier directory,
// each carrying the given passed/failed/skipped assertion counts.
function result(tiers) {
  const fill = (status, n) => Array.from({ length: n }, () => ({ status }))
  const testResults = Object.entries(tiers).map(([tier, { p = 0, f = 0, s = 0 }]) => ({
    name: `/repo/tests/${tier}/x.test.ts`,
    assertionResults: [...fill('passed', p), ...fill('failed', f), ...fill('skipped', s)],
  }))
  return { testResults }
}

describe('tierOf', () => {
  it('maps tier directories and falls back to other', () => {
    expect(tierOf('/repo/tests/tier1/a.test.ts')).toBe('tier1')
    expect(tierOf('/repo/tests/tier2/a.test.ts')).toBe('tier2')
    expect(tierOf('/repo/tests/tier3/a.test.ts')).toBe('tier3')
    expect(tierOf('/repo/tests/misc/a.test.ts')).toBe('other')
  })
})

describe('scoreResults', () => {
  it('returns null for a file that is not a Vitest result', () => {
    expect(scoreResults({ schema: 1, describes: {} })).toBeNull()
    expect(scoreResults({})).toBeNull()
    expect(scoreResults(null)).toBeNull()
  })

  it('sums passed/failed/skipped across the three tiers', () => {
    const scored = scoreResults(
      result({ tier1: { p: 3 }, tier2: { p: 2, f: 1 }, tier3: { p: 1, s: 4 } }),
    )
    expect(scored).toMatchObject({ passed: 6, failed: 1, skipped: 4, count: 11 })
  })

  it('excludes the "other" tier from the counts', () => {
    const scored = scoreResults(result({ tier1: { p: 2 }, other: { p: 5, f: 5 } }))
    expect(scored).toMatchObject({ passed: 2, failed: 0, skipped: 0, count: 2 })
  })

  it('returns zeroed counts (not null) for a real result with no scored tests', () => {
    expect(scoreResults({ testResults: [] })).toMatchObject({
      passed: 0,
      failed: 0,
      skipped: 0,
      count: 0,
    })
  })
})

describe('passRate', () => {
  it('is passed / (passed + failed) as a percentage', () => {
    expect(passRate(99, 1)).toBeCloseTo(99, 5)
    expect(passRate(2, 1)).toBeCloseTo(66.6667, 3)
  })

  it('returns null when nothing ran', () => {
    expect(passRate(0, 0)).toBeNull()
  })
})

describe('GROUND_TRUTH_SLUG', () => {
  it('is dynamodb', () => {
    expect(GROUND_TRUTH_SLUG).toBe('dynamodb')
  })
})

describe('isPublishedTarget', () => {
  it('excludes the reserved local scratch slug', () => {
    expect(isPublishedTarget('local')).toBe(false)
  })

  it('keeps real targets, matching the slug exactly rather than by prefix', () => {
    // dynamodb-local contains "local" but is a real target; an exact-match
    // reservation must not catch it.
    expect(isPublishedTarget('dynamodb-local')).toBe(true)
    expect(isPublishedTarget('dynoxide')).toBe(true)
    expect(isPublishedTarget(GROUND_TRUTH_SLUG)).toBe(true)
  })
})
