import { describe, expect, it } from 'vitest'
import { COVERAGE_CAPS, GRADE_BANDS, GRADING_VERSION, gradeOf } from './grade.mjs'

describe('gradeOf', () => {
  it('grades exactly zero divergence A+, with the caps doing the coverage work', () => {
    expect(gradeOf(0, 100)).toEqual({
      letter: 'A+',
      qualifier: 'no divergence',
      band: 'pass',
      capped: false,
    })
    expect(gradeOf(0, 90)).toMatchObject({ letter: 'A+' })
    // A+ needs exactly zero, not a figure that would display as 0.0%: one
    // fail in a large suite rounds to 0.0% without being zero.
    expect(gradeOf(0.04, 100).letter).toBe('A')
  })

  it('letters follow the published divergence bands', () => {
    expect(gradeOf(1.8, 100).letter).toBe('A')
    expect(gradeOf(4.9, 100).letter).toBe('A')
    expect(gradeOf(5, 100).letter).toBe('B')
    expect(gradeOf(14.9, 100).letter).toBe('B')
    expect(gradeOf(15, 100).letter).toBe('C')
    expect(gradeOf(24.9, 100).letter).toBe('C')
    expect(gradeOf(25, 100).letter).toBe('D')
    expect(gradeOf(34.9, 100).letter).toBe('D')
    expect(gradeOf(35, 100).letter).toBe('F')
  })

  it('zero divergence under 90% coverage is capped, not gated', () => {
    // No coverage floor on A+ itself: the sub-90 cap ceilings the letter at
    // B, which is why a floor on the gate would have been redundant.
    expect(gradeOf(0, 89.9)).toMatchObject({ letter: 'B', capped: true })
  })

  it('coverage alone never grades F: the caps stop at D', () => {
    expect(gradeOf(0, 1)).toMatchObject({ letter: 'D', capped: true })
  })

  it('coverage caps ceiling the letter without touching the figures', () => {
    // Dynoxide's wasm build on the current board: perfect answers over 78.7%
    // of the suite reads B, not A+ - the cap is the whole point.
    expect(gradeOf(0, 78.7)).toEqual({
      letter: 'B',
      qualifier: 'no divergence',
      band: 'partial',
      capped: true,
    })
    expect(gradeOf(1, 65)).toMatchObject({ letter: 'C', capped: true })
    expect(gradeOf(1, 45)).toMatchObject({ letter: 'D', capped: true })
  })

  it('cap boundaries are exclusive at exactly 50, 70 and 90 covered', () => {
    // "Under" means under: sitting exactly on a cap boundary takes the
    // lighter cap, the same exclusivity the divergence bands use.
    expect(gradeOf(1, 90)).toMatchObject({ letter: 'A', capped: false })
    expect(gradeOf(1, 89.9)).toMatchObject({ letter: 'B', capped: true })
    expect(gradeOf(1, 70)).toMatchObject({ letter: 'B', capped: true })
    expect(gradeOf(1, 69.9)).toMatchObject({ letter: 'C', capped: true })
    expect(gradeOf(1, 50)).toMatchObject({ letter: 'C', capped: true })
    expect(gradeOf(1, 49.9)).toMatchObject({ letter: 'D', capped: true })
  })

  it('a cap never improves a letter already below it', () => {
    expect(gradeOf(30, 45)).toMatchObject({ letter: 'D', capped: false })
    expect(gradeOf(40, 45)).toMatchObject({ letter: 'F', capped: false })
  })

  it('qualifiers name the divergence band in plain language', () => {
    expect(gradeOf(1.8, 100).qualifier).toBe('low divergence')
    expect(gradeOf(12.3, 100).qualifier).toBe('moderate divergence')
    expect(gradeOf(16.1, 100).qualifier).toBe('high divergence')
    expect(gradeOf(28, 100).qualifier).toBe('very high divergence')
    expect(gradeOf(40, 100).qualifier).toBe('severe divergence')
  })

  it('nothing scored grades nothing', () => {
    expect(gradeOf(null, null)).toEqual({
      letter: null,
      qualifier: 'not scored',
      band: 'none',
      capped: false,
    })
    expect(gradeOf(null, 100).letter).toBe(null)
    expect(gradeOf(0, null).letter).toBe(null)
  })

  it('letters agree with the printed one-decimal figures at every boundary sliver', () => {
    // 4.96% prints "5.0%": the letter must be B, as a printed 5.0 demands,
    // not the A that raw-value banding would give. Same for the caps: 89.98%
    // coverage prints "90.0%" and must escape the sub-90 cap.
    expect(gradeOf(4.96, 100).letter).toBe('B')
    expect(gradeOf(4.94, 100).letter).toBe('A')
    expect(gradeOf(1, 89.98)).toMatchObject({ letter: 'A', capped: false })
    expect(gradeOf(0, 89.98).letter).toBe('A+')
    // A divergence that merely rounds to 0.0% is still not zero: the letter
    // is A, never A+ - the top grade reads the raw count alone.
    expect(gradeOf(0.04, 100).letter).toBe('A')
    expect(gradeOf(0.04, 100).qualifier).toBe('low divergence')
  })

  it('a malformed input grades nothing, never F', () => {
    // NaN falls through every band bound (NaN comparisons are all false), so
    // without the finite guard a data fault would publish the worst letter.
    expect(gradeOf(NaN, 100).letter).toBe(null)
    expect(gradeOf(0, NaN).letter).toBe(null)
    expect(gradeOf(Infinity, 100).letter).toBe(null)
  })

  it('band colours restate the published colour boundaries', () => {
    expect(gradeOf(0, 100).band).toBe('pass')
    expect(gradeOf(4.9, 100).band).toBe('pass')
    expect(gradeOf(5, 100).band).toBe('partial')
    expect(gradeOf(24.9, 100).band).toBe('partial')
    expect(gradeOf(25, 100).band).toBe('fail')
  })

  it('pins the criteria the published grades were computed under', () => {
    // A threshold change moves grades on targets that changed nothing, so the
    // criteria are versioned: retuning bands or caps must bump the version and
    // date the change in the methodology.
    expect(GRADING_VERSION).toBe(1)
    expect(GRADE_BANDS).toEqual([
      { letter: 'A', under: 5 },
      { letter: 'B', under: 15 },
      { letter: 'C', under: 25 },
      { letter: 'D', under: 35 },
    ])
    expect(COVERAGE_CAPS).toEqual([
      { under: 50, cap: 'D' },
      { under: 70, cap: 'C' },
      { under: 90, cap: 'B' },
    ])
  })
})
