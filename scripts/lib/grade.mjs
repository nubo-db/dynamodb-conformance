// The letter grade published beside the two axes, shared by the results table,
// the badges and the site so the three can never disagree.
//
// The grade is a reading aid, not a third metric. Divergence sets the letter
// and coverage can only cap it: the two figures are never summed, averaged or
// otherwise traded against each other, so the board's rule that a missing
// operation and a wrong one carry different risks survives the letter intact.
// Both figures stay published beside every grade, and the grade is
// recomputable from them with the thresholds below.
//
// The 5% and 25% boundaries carry over the numbers the board has published as
// its colour bands since it began. The numbers carry over; the denominator does
// not, and that is worth stating rather than eliding. Those bands sat on
// correctness (95% and 75%, over the operations a target implements) and these
// sit on divergence (over the whole suite), so the same digits cut a different
// line everywhere but full coverage: at 80% coverage, 5% divergence is 93.75%
// correctness, which the old amber band caught. That is the leniency the
// coverage caps below exist to answer. The splits at 15% and 35% are new, and
// so are the caps.
//
// A+ is exactly zero divergence, and the coverage caps apply to it like any
// other letter.
//
// Criteria changes are versioned. A retuned threshold moves published grades
// on targets that changed nothing - the documented failure mode of every
// graded system - so any change here bumps GRADING_VERSION and is dated in
// the methodology's grading criteria section.

export const GRADING_VERSION = 1

// Divergence bands, upper bound exclusive, in points of the whole suite.
// The letter for a divergence below `under`, best first. F catches the rest.
export const GRADE_BANDS = [
  { letter: 'A', under: 5 },
  { letter: 'B', under: 15 },
  { letter: 'C', under: 25 },
  { letter: 'D', under: 35 },
]

// A+ is exactly zero divergence: not one failing test, not a figure that
// rounds to 0.0%. Zero is a natural boundary rather than a tunable one -
// conformance on everything the target implements - and pinning it to the
// count rather than the rounded figure matters as the suite grows, because a
// single fail in a large enough suite displays as 0.0% without being zero.
// A+ carries no coverage condition of its own: the caps below apply to it
// like any other letter, so a perfect answer over a narrow surface is capped
// rather than gated. (A coverage floor on the gate would be redundant - any
// coverage low enough to deny A+ already caps the letter to B or below.)

// Coverage caps, lowest first: a target implementing under `under` percent of
// the suite can grade no better than `cap`, however little it diverges. A cap
// never moves a number - it ceilings the letter, so a capped grade is read
// with the coverage figure that earned the cap, published right beside it.
// The caps stop at D: coverage alone never grades a target F, because F means
// wrong on more than a third of the suite, and an operation a target never
// attempts is absent, not wrong.
//
// A cap that bit and a cap that is merely holding are different facts, and only
// the first used to be visible. Dynalite diverges 12.3% (band B) over 80.0%
// coverage (cap B), so its letter sits on a ceiling it never had to be lowered
// to, and its row was indistinguishable from one that earned B outright.
// `capAt` publishes the ceiling a letter is sitting at; `capped` still says
// whether reaching it moved the letter. Both are null/false for a row that
// diverges its way past the cap on its own - a target at 30% divergence over
// 80% coverage grades D, and naming B as its ceiling would imply a constraint
// that is doing nothing.
export const COVERAGE_CAPS = [
  { under: 50, cap: 'D' },
  { under: 70, cap: 'C' },
  { under: 90, cap: 'B' },
]

// Plain-language reading of the divergence behind a grade, shown where the
// bare percentage used to stand alone: "0.0% diverges" reads as a zero, and
// "no divergence" reads as what it is.
const QUALIFIERS = [
  { under: 5, text: 'low divergence' },
  { under: 15, text: 'moderate divergence' },
  { under: 25, text: 'high divergence' },
  { under: 35, text: 'very high divergence' },
]

// Grade order for capping, best first.
const ORDER = ['A+', 'A', 'B', 'C', 'D', 'F']

// The colour band a letter falls in, matching the board's published colour
// boundaries: the A range is the green band, B and C span the amber one, D
// and F the red. A capped letter wears its capped colour - the cap is the
// grade, not a footnote to a better one.
const BAND_OF = { 'A+': 'pass', A: 'pass', B: 'partial', C: 'partial', D: 'fail', F: 'fail' }

const letterFor = (divergence) =>
  GRADE_BANDS.find((b) => divergence < b.under)?.letter ?? 'F'

const capFor = (coverage) => COVERAGE_CAPS.find((c) => coverage < c.under)?.cap ?? null

const worse = (a, b) => (ORDER.indexOf(a) >= ORDER.indexOf(b) ? a : b)

/**
 * Grade a target from its two published figures. Null in, null out: a target
 * that implemented nothing has no divergence to grade, and inventing a letter
 * for it would rank an empty target - the same reason its divergence is null.
 */
export function gradeOf(divergenceValue, coverageValue) {
  // Null in, null out - and NaN counts as null. Without the finite check a
  // malformed input would fall through every band bound (NaN comparisons are
  // all false) and grade F: the worst published letter, manufactured from a
  // data fault rather than a measurement.
  if (!Number.isFinite(divergenceValue) || !Number.isFinite(coverageValue)) {
    return { letter: null, qualifier: 'not scored', band: 'none', capped: false, capAt: null }
  }

  // The bands and caps read the figures at the one decimal place the board
  // publishes, via the same toFixed the display uses, so a letter can never
  // disagree with the number printed beside it: 898 implemented of 998 is
  // 89.98% raw, prints "90.0%", and must escape the sub-90 cap exactly as a
  // true 90.0 does. Grading the raw value instead opens a sliver at every
  // boundary where the printed figure satisfies a band the letter denies.
  const divergence = Number(divergenceValue.toFixed(1))
  const coverage = Number(coverageValue.toFixed(1))

  // A+ alone reads the raw value: it means zero failing tests, and a
  // divergence that merely rounds to 0.0% is not zero. Everything else is
  // graded as printed.
  const base = divergenceValue === 0 ? 'A+' : letterFor(divergence)

  const cap = capFor(coverage)
  const letter = cap ? worse(base, cap) : base

  const qualifier =
    divergenceValue === 0
      ? 'no divergence'
      : (QUALIFIERS.find((q) => divergence < q.under)?.text ?? 'severe divergence')

  // The ceiling the letter is sitting at, when coverage is what put it there.
  // `letter === cap` is the test for "the cap is doing work": it covers both the
  // cap lowering the letter and the letter arriving at the cap on its own, and
  // excludes a row that diverged past the cap without its help.
  const capAt = cap && letter === cap ? cap : null

  return { letter, qualifier, band: BAND_OF[letter], capped: letter !== base, capAt }
}

// The label the baseline wears where every other row wears a letter. Real
// DynamoDB is what a grade measures distance from, so grading it against itself
// seats the yardstick in a band an engine had to earn its way into and the top
// of the table stops meaning what it says. Its two figures still publish: they
// are the definition the rest of the column is read against. Every surface
// reads this one constant - the results table, the badges and the data
// endpoints - so none of them can grade the baseline while another declines to.
export const BASELINE_LABEL = 'baseline'

// The same decision in the shape `gradeOf` returns, for consumers that read a
// grade object per row. It reuses the null-letter case they already handle for
// an unscored target, with a qualifier that says which of the two it is.
export const BASELINE_GRADE = Object.freeze({
  letter: null,
  qualifier: BASELINE_LABEL,
  band: 'none',
  capped: false,
  capAt: null,
})
