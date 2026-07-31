// The letter grade published beside the two axes, shared by the results table,
// the badges and the site so the three can never disagree.
//
// The grade is a reading aid, not a third metric. Divergence sets the letter
// and coverage can only lower it, never raise it. Both figures stay published
// beside every grade, and the grade is recomputable from them with the criteria
// below.
//
// The 5% and 25% boundaries carry over the numbers the board has published as
// its colour bands since it began. The numbers carry over; the denominator does
// not. Those bands sat on correctness - 95% and 75%, over the operations a
// target implements - and these sit on divergence, over the whole suite, so the
// same digits cut a different line at anything below full coverage: at 80%
// coverage, 5% divergence is 93.75% correctness, which the old amber band
// caught. The coverage weight below answers that leniency. The splits at 15%
// and 35% are new lines, and so is the weight.
//
// The cap rolls rather than stepping. Withdrawing a failing test lowers
// divergence and coverage by the same amount, so a stepped cap left gaps
// between its steps where a target could decline what it failed, cross a band,
// and never reach the step that would have held it.
//
// A weight prices every withdrawal, but it does not stop one: withdrawal moves
// the effective figure by (1 - weight) of what was withdrawn, so only a weight
// of 1 removes the gain, and that is a skip counted as heavily as a fail. The
// prose says the price rose rather than claiming the gap closed.
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

// A+ is zero divergence over the whole suite: nothing failed and nothing
// declined. Both halves read raw values, because a single fail in a large enough
// suite displays as 0.0% without being zero, and a target one test short of the
// suite does not print 100.0% coverage.
export const A_PLUS = Object.freeze({ divergence: 0, coverage: 100, exact: true })

// What a declined test costs against the letter, relative to a failed one:
//
//   effective = divergence + (100 - coverage) / COVERAGE_DIVISOR
//
// so a target implementing everything is graded on divergence alone. Since
// effective is never below divergence, coverage can only lower a letter.
//
// A divisor rather than a decimal weight, because this is published for
// consumers to regrade with: 0.333... is not representable, and a rounded
// decimal lands on the other side of a band boundary from the division.
//
// Three is a judgement. At 1 a skip is a fail; much higher and it does nothing.
// A third puts a target declining 30% of the suite two bands behind one that
// implements it all, and leaves a target declining 2% where divergence puts it.
export const COVERAGE_DIVISOR = 3

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

// The colour band for a letter on its own, for surfaces that show the criteria
// rather than a graded row - the board's legend, which needs a tinted chip per
// letter without a target to grade. Exported so the legend derives its colours
// from the same table the rows do.
export const bandOf = (letter) => BAND_OF[letter] ?? 'none'

const letterFor = (divergence) =>
  GRADE_BANDS.find((b) => divergence < b.under)?.letter ?? 'F'

// The figure the bands read: divergence plus the weighted share the target
// declines, rounded to the one decimal the board publishes so a reader working
// it out from the two figures on a card lands on the same letter.
export const effectiveOf = (divergence, coverage) =>
  Number((divergence + (100 - coverage) / COVERAGE_DIVISOR).toFixed(1))

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

  // The bands read the figures at the one decimal place the board publishes,
  // via the same toFixed the display uses, so a letter can never disagree with
  // the numbers printed beside it. Grading the raw values instead opens a sliver
  // at every boundary where the printed figures satisfy a band the letter denies.
  const divergence = Number(divergenceValue.toFixed(1))
  const coverage = Number(coverageValue.toFixed(1))

  // A+ alone reads the raw values, on both axes: zero failing tests and nothing
  // declined. A divergence that merely rounds to 0.0% is not zero, and a target
  // one test short of the suite does not print 100.0% coverage.
  const perfect =
    divergenceValue === A_PLUS.divergence && coverageValue === A_PLUS.coverage

  // Divergence alone gives the letter the target would hold if it implemented
  // everything; the effective figure gives the one it actually holds. The second
  // is never better than the first, because effective >= divergence always.
  const base = divergenceValue === A_PLUS.divergence ? 'A+' : letterFor(divergence)
  const letter = perfect ? 'A+' : letterFor(effectiveOf(divergence, coverage))

  const qualifier =
    divergenceValue === A_PLUS.divergence
      ? 'no divergence'
      : (QUALIFIERS.find((q) => divergence < q.under)?.text ?? 'severe divergence')

  // Published only where coverage actually lowered the letter. A row graded on
  // its divergence alone reports no ceiling, so a consumer ranking on letters
  // can tell the two apart.
  const capped = letter !== base

  return { letter, qualifier, band: BAND_OF[letter], capped, capAt: capped ? letter : null }
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
