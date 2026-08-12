// Detect engines that share an implementation, from the results alone.
//
// Two independent engines get different things wrong. Two engines where one is
// built on the other get the *same* things wrong, because the divergence is
// inherited rather than arrived at. So the overlap of two targets' failure sets
// is evidence about their lineage, and it is evidence the suite already holds.
//
// Why measure it rather than declare it. A hand-typed "LocalStack is built on
// DynamoDB Local" is a claim about someone else's product that nothing checks,
// it goes stale silently when they re-implement, and it sits awkwardly beside a
// board where every other figure is derived from a run. A measured
// relationship is one the suite can defend with its own data, and it keeps
// working when a project changes underneath.
//
// Raw overlap is not the measure. The suite's hard cases are hard for
// everybody, so any two weak targets share a lot of failures without being
// related at all - two unrelated engines here overlap on 83% of the smaller
// set. Jaccard (shared / combined) corrects for that by charging for the
// failures they do NOT share, which is where independent engines differ.
//
// The threshold is derived, not chosen. Pairwise similarity across the board is
// mostly low with a couple of outliers, so the flag is a robust outlier rule -
// median plus a multiple of the median absolute deviation - which recalibrates
// as targets come and go instead of encoding today's numbers as a constant.

import { classifyResults } from './classify.mjs'
import { relativeTestPath } from './identity.mjs'

/**
 * Every test a target got wrong, as `file::fullName`.
 *
 * Through the classifier, not `assertionResults[].status`. An indeterminate
 * records `status: "failed"` too, and two targets timing out on the same slow
 * test is a fact about the run rather than a shared implementation - which is
 * the only thing this report is looking for.
 */
export function failureSet(doc) {
  const out = new Set()
  if (!Array.isArray(doc?.testResults)) return out
  for (const v of classifyResults(doc, null)) {
    if (v.verdict !== 'fail') continue
    out.add(`${relativeTestPath(String(v.file ?? ''))}::${v.fullName ?? v.title}`)
  }
  return out
}

/**
 * Jaccard similarity of two failure sets: shared / combined, as a percentage.
 * Null when either target failed nothing, where the measure says nothing.
 */
export function similarity(a, b) {
  if (a.size === 0 || b.size === 0) return null
  let shared = 0
  for (const x of a) if (b.has(x)) shared++
  const combined = a.size + b.size - shared
  return combined === 0 ? null : (shared / combined) * 100
}

const median = (xs) => {
  if (xs.length === 0) return null
  const s = [...xs].sort((a, b) => a - b)
  const m = Math.floor(s.length / 2)
  return s.length % 2 ? s[m] : (s[m - 1] + s[m]) / 2
}

/**
 * Every pair's similarity, ranked, with the outliers flagged.
 *
 * `targets` is `[{ slug, failures }]`. Pairs belonging to the same project are
 * excluded: two builds of one engine are related by construction, that is
 * already declared in the target registry, and leaving them in would drag the
 * baseline up and mask a real find.
 */
export function lineageReport(targets, { sameProject = () => false, deviations = 3 } = {}) {
  const pairs = []
  for (let i = 0; i < targets.length; i++) {
    for (let j = i + 1; j < targets.length; j++) {
      const [a, b] = [targets[i], targets[j]]
      if (sameProject(a.slug, b.slug)) continue
      const score = similarity(a.failures, b.failures)
      if (score === null) continue
      let shared = 0
      for (const x of a.failures) if (b.failures.has(x)) shared++
      pairs.push({
        a: a.slug,
        b: b.slug,
        similarity: score,
        shared,
        combined: a.failures.size + b.failures.size - shared,
      })
    }
  }
  pairs.sort((x, y) => y.similarity - x.similarity)

  const scores = pairs.map((p) => p.similarity)
  const mid = median(scores)
  // Median absolute deviation: a spread measure the outliers themselves cannot
  // inflate, unlike a standard deviation.
  const mad = mid === null ? null : median(scores.map((s) => Math.abs(s - mid)))
  const threshold = mid === null || mad === null ? null : mid + deviations * mad

  return {
    pairs: pairs.map((p) => ({
      ...p,
      flagged: threshold !== null && p.similarity > threshold,
    })),
    baseline: mid,
    deviation: mad,
    threshold,
  }
}
