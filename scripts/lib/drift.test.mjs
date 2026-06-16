import { describe, it, expect } from 'vitest'
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { diffCaptures, diffProbe, diffRegions, isClean } from './drift.mjs'

const here = dirname(fileURLToPath(import.meta.url))
const snapshot = JSON.parse(
  readFileSync(join(here, '../../captures/2026-06-09-validation-rewording.json'), 'utf8'),
)

const probe = (over) => ({
  id: 'p1',
  name: 'ValidationException',
  message: 'a message',
  n: 1,
  fields: ['tableName'],
  ...over,
})

const block = (...probes) => ({ probes, nullRoundTrip: { put: 'accepted' } })

describe('diffProbe', () => {
  it('returns null when nothing moved', () => {
    expect(diffProbe(probe(), probe())).toBeNull()
  })

  it('flags a changed message with both texts', () => {
    const d = diffProbe(probe({ message: 'old' }), probe({ message: 'new' }))
    expect(d.changed).toEqual(['message'])
    expect(d.baseline.message).toBe('old')
    expect(d.observed.message).toBe('new')
  })

  it('marks a prose-only change as passing-but-different', () => {
    // same type, count and field; only the wording moved
    const d = diffProbe(probe({ message: 'old prose' }), probe({ message: 'new prose' }))
    expect(d.passingButDifferent).toBe(true)
  })

  it('does NOT mark a field or type change as passing-but-different', () => {
    const d = diffProbe(probe({ fields: ['tableName'] }), probe({ message: 'new', fields: ['TableName'] }))
    expect(d.changed).toContain('fields')
    expect(d.passingButDifferent).toBe(false)
  })

  it('flags a changed N count', () => {
    expect(diffProbe(probe({ n: 1 }), probe({ n: 2 })).changed).toContain('n')
  })

  it('flags a changed field list (order-sensitive)', () => {
    const d = diffProbe(probe({ fields: ['a', 'b'] }), probe({ fields: ['b', 'a'] }))
    expect(d.changed).toContain('fields')
  })
})

describe('diffCaptures', () => {
  it('is clean when blocks are identical', () => {
    const b = block(probe())
    expect(isClean(diffCaptures(b, b))).toBe(true)
  })

  it('reports an added and a removed probe', () => {
    const base = block(probe({ id: 'only-base' }))
    const obs = block(probe({ id: 'only-obs' }))
    const ids = diffCaptures(base, obs).probes
    expect(ids.find((p) => p.id === 'only-base').changed).toEqual(['removed'])
    expect(ids.find((p) => p.id === 'only-obs').changed).toEqual(['added'])
  })

  it('flags a nullRoundTrip divergence', () => {
    const base = { probes: [], nullRoundTrip: { put: 'accepted', returnedItem: { x: { NULL: true } } } }
    const obs = { probes: [], nullRoundTrip: { put: 'rejected', name: 'ValidationException' } }
    const d = diffCaptures(base, obs)
    expect(d.nullRoundTrip).not.toBeNull()
    expect(d.nullRoundTrip.observed.put).toBe('rejected')
  })
})

describe('fixture regression: the June 2026 four-region snapshot', () => {
  const euw2 = snapshot.regions['eu-west-2']

  it('finds no drift comparing eu-west-2 to itself', () => {
    expect(isClean(diffCaptures(euw2, euw2))).toBe(true)
  })

  it('reproduces the 22 eu-west-2-vs-us-east-1 divergences (old vs new wording)', () => {
    const d = diffCaptures(euw2, snapshot.regions['us-east-1'])
    expect(d.probes).toHaveLength(22)
    // the empty-TableName case moved both prose and the field token (camelCase -> PascalCase)
    const empty = d.probes.find((p) => p.id === 's_put_table_empty')
    expect(empty.changed).toEqual(expect.arrayContaining(['message', 'fields']))
    expect(empty.baseline.message).toContain("at 'TableName'")
    expect(empty.observed.message).toContain("at 'tableName'")
    // { NULL: false } acceptance differs between the lead and laggard regions
    expect(d.nullRoundTrip).not.toBeNull()
    // at least one probe drifted in prose only (the case a tolerant assertion misses)
    expect(d.probes.some((p) => p.passingButDifferent)).toBe(true)
  })

  it('shows eu-central-1 tracking eu-west-2 far more closely than the laggards', () => {
    const cr = diffRegions(snapshot, 'eu-west-2')
    expect(cr.baselineRegion).toBe('eu-west-2')
    expect(cr.regions['eu-central-1'].probes.length).toBeLessThan(
      cr.regions['us-east-1'].probes.length,
    )
    expect(cr.regions['us-east-1'].probes).toHaveLength(22)
    expect(cr.regions['ap-southeast-2'].probes).toHaveLength(22)
  })
})
