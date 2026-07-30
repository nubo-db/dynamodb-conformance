import { describe, expect, it } from 'vitest'
import {
  BASELINE_GRADE,
  BASELINE_LABEL,
  COVERAGE_CAPS,
  GRADE_BANDS,
  GRADING_VERSION,
  gradeOf,
} from './grade.mjs'

describe('gradeOf', () => {
  it('grades exactly zero divergence A+, with the caps doing the coverage work', () => {
    expect(gradeOf(0, 100)).toEqual({
      letter: 'A+',
      qualifier: 'no divergence',
      band: 'pass',
      capped: false,
      capAt: null,
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
      capAt: 'B',
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

  it('capAt names the ceiling a letter sits at, in all three cap states', () => {
    // The three states a cap can be in, and only the first of them used to be
    // visible on a row. `capped` answers "did reaching the ceiling move the
    // letter"; `capAt` answers "is this letter sitting on a ceiling at all",
    // which is the question a reader comparing two rows is actually asking.

    // 1. The cap bit: Dynoxide's wasm build, perfect over 78.7% of the suite.
    expect(gradeOf(0, 78.7)).toMatchObject({ letter: 'B', capped: true, capAt: 'B' })

    // 2. The cap is holding without ever having bitten: Dynalite's live row.
    // Divergence alone gives B and coverage caps at B, so the letter is at its
    // ceiling and `capped` is false. This is the case the row went silent on -
    // it read identically to a target that earned B with room above it.
    expect(gradeOf(12.3, 80)).toMatchObject({ letter: 'B', capped: false, capAt: 'B' })

    // 3. The cap is irrelevant: divergence alone already put the row below it,
    // so naming B as the ceiling would imply a constraint doing no work.
    expect(gradeOf(30, 80)).toMatchObject({ letter: 'D', capped: false, capAt: null })

    // No cap in force at all.
    expect(gradeOf(12.3, 100)).toMatchObject({ letter: 'B', capped: false, capAt: null })
  })

  it('LocalStack and Dynalite are separated by the cap, not just the letter', () => {
    // The pair that made the ceiling worth publishing. Dynalite reads B and
    // LocalStack C, so the letters put Dynalite ahead - and it implements 19
    // points less of the suite. Both facts are true and the board reports them
    // apart on purpose, but a reader scanning letters needs the row to say that
    // Dynalite's B cannot go higher while LocalStack's C can.
    const dynalite = gradeOf(12.3, 80)
    const localstack = gradeOf(15.6, 99.2)
    expect(dynalite.letter).toBe('B')
    expect(localstack.letter).toBe('C')
    expect(dynalite.capAt).toBe('B')
    expect(localstack.capAt).toBe(null)
  })

  it('the baseline carries no letter, on every surface that reads one', () => {
    // Real DynamoDB is what a grade measures distance from, so it is not graded
    // against itself. The constant is shared rather than restated per surface:
    // the results table, the badges, the endpoints and the agent corpus each
    // used to derive their own, and the table was still publishing A+ after the
    // site had stopped.
    expect(BASELINE_LABEL).toBe('baseline')
    expect(BASELINE_GRADE).toEqual({
      letter: null,
      qualifier: 'baseline',
      band: 'none',
      capped: false,
      capAt: null,
    })
    // It reuses the null-letter shape a consumer already handles for a target
    // that scored nothing, distinguished by the qualifier rather than by a
    // letter they would have to special-case.
    expect(BASELINE_GRADE.letter).toBe(gradeOf(null, null).letter)
    expect(BASELINE_GRADE.qualifier).not.toBe(gradeOf(null, null).qualifier)
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
      capAt: null,
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
