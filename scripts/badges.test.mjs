import { describe, it, expect } from 'vitest'
import { existsSync, readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { buildBadge, colour, rateFor } from './badges.mjs'

const RESULTS_DIR = 'results'

// Minimal Vitest-shaped result with the given tier 1 passed/failed counts.
function result(passed, failed) {
  const fill = (status, n) => Array.from({ length: n }, () => ({ status }))
  return {
    testResults: [
      {
        name: '/repo/tests/tier1/x.test.ts',
        assertionResults: [...fill('passed', passed), ...fill('failed', failed)],
      },
    ],
  }
}

describe('colour', () => {
  it.each([
    [100, 'brightgreen'],
    [99, 'brightgreen'],
    [98.9, 'green'],
    [95, 'green'],
    [94.9, 'yellowgreen'],
    [90, 'yellowgreen'],
    [89.9, 'yellow'],
    [75, 'yellow'],
    [74.9, 'orange'],
    [50, 'orange'],
    [49.9, 'red'],
    [0, 'red'],
  ])('%s%% -> %s', (pct, expected) => {
    expect(colour(pct)).toBe(expected)
  })
})

describe('rateFor', () => {
  it('pins the ground-truth target to 100', () => {
    expect(rateFor('dynamodb', {})).toBe(100)
  })

  it('returns null for a non-result file', () => {
    expect(rateFor('tag-manifest', { schema: 1 })).toBeNull()
  })

  it('returns null for the reserved local scratch slug', () => {
    // A real, well-formed run output - excluded by slug, not by structure.
    expect(rateFor('local', result(5, 0))).toBeNull()
  })

  it('scores a real target as passed / (passed + failed)', () => {
    expect(rateFor('dynoxide', result(2, 1))).toBeCloseTo(66.6667, 3)
  })
})

describe('buildBadge', () => {
  it('returns null when there is nothing to show', () => {
    expect(buildBadge('tag-manifest', { schema: 1 })).toBeNull()
  })

  it('returns null for the reserved local scratch slug', () => {
    expect(buildBadge('local', result(5, 0))).toBeNull()
  })

  it('emits the shields endpoint shape for the ground truth', () => {
    expect(buildBadge('dynamodb', {})).toEqual({
      schemaVersion: 1,
      label: 'conformance',
      message: '100.0%',
      color: 'brightgreen',
    })
  })

  it('colours off the displayed value, not the raw rate', () => {
    // 197/199 = 98.99%, which displays as "99.0%" and must colour brightgreen
    // to match the number shown rather than the sub-99 raw rate.
    const badge = buildBadge('dynoxide', result(197, 2))
    expect(badge.message).toBe('99.0%')
    expect(badge.color).toBe('brightgreen')
  })
})

describe('committed badges are fresh', () => {
  const resultFiles = readdirSync(RESULTS_DIR).filter(
    (f) => f.endsWith('.json') && !f.endsWith('.badge.json'),
  )

  it.each(resultFiles)('%s matches a fresh build', (file) => {
    const slug = file.replace(/\.json$/, '')
    const raw = JSON.parse(readFileSync(join(RESULTS_DIR, file), 'utf8'))
    const expected = buildBadge(slug, raw)
    const badgePath = join(RESULTS_DIR, `${slug}.badge.json`)

    if (expected === null) {
      expect(existsSync(badgePath), `${slug} should not have a badge`).toBe(false)
      return
    }

    expect(
      existsSync(badgePath),
      `${slug}.badge.json missing — run \`npm run results:badges\``,
    ).toBe(true)
    const committed = JSON.parse(readFileSync(badgePath, 'utf8'))
    expect(committed, `${slug}.badge.json is stale — run \`npm run results:badges\``).toEqual(
      expected,
    )
  })
})
