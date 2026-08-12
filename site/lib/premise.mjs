// The A+ premise: what a zero-divergence row is allowed to be hiding.
//
// A target with no failures in its headline region can still fail elsewhere.
// The board's claim is that when it does, the behaviour is one real DynamoDB
// splits on by region. Counts cannot settle that - three fails matching three
// splits and three fails on unrelated behaviours are the same number - so it is
// checked by name, against the split registry.
//
// This lives here, apart from the build check that calls it, because it is the
// one rule qualifying the top of the scale and for most of a year there has
// been no row for it to run on. Left inline it was exercised only by whatever
// the board happened to hold, which since 2026-08-12 is nothing: a guard that
// can only be tested by waiting for a target to earn A+ is a guard nobody can
// tell is working. As a function it takes fixtures.

import { axesOf, gradeOf } from "./scoring.mjs";

/**
 * Check every zero-divergence row in a published summary against the registry.
 *
 * Returns `{ guarded, unconfirmed, uncheckable }`. `guarded` counts the rows
 * the premise applies to, so a caller can tell "nothing to check" from
 * "checked and clean" - the distinction that makes a vacuous pass honest.
 *
 * `unconfirmed` is a breach: a failure the registry does not explain, or a
 * headline letter its worst region does not support. `uncheckable` is the
 * artefact not carrying the evidence, which is also a failure - passing on
 * absent evidence is how a check by name silently becomes a check by count.
 */
export function checkAPlusPremise(summary, splits = []) {
  const unconfirmed = [];
  const uncheckable = [];
  let guarded = 0;

  // A registry row with no test identity confirms nothing: it cannot be matched
  // against a named failure, so it is left out rather than allowed to widen the
  // set by accident.
  const confirmed = new Set(
    splits.filter((row) => row?.test?.file).map((row) => `${row.test.file}::${row.test.fullName}`),
  );

  for (const [slug, t] of Object.entries(summary?.targets ?? {})) {
    const worst = t.divergenceWorst;
    const best = t.divergenceBest;
    if (best !== 0) continue; // not a zero-divergence target; the claim is not about it
    guarded++;

    const failingRegions = (t.regions ?? []).filter((r) => r.failed > 0).map((r) => r.region);

    if (failingRegions.length > 0 && !t.regionFailures) {
      uncheckable.push(`${slug} fails in ${failingRegions.length} region(s) but published no test identities`);
    } else {
      for (const region of failingRegions) {
        const names = t.regionFailures[region] ?? [];
        const declared = t.regions.find((r) => r.region === region)?.failed ?? 0;
        if (names.length !== declared) {
          uncheckable.push(`${slug}/${region} declares ${declared} fail(s) but names ${names.length}`);
          continue;
        }
        for (const identity of names) {
          // `<file>::<fullName>`, the suite's own test identity. Matching on
          // the name alone accepted a same-named test from a different file.
          if (!confirmed.has(identity)) unconfirmed.push(`${slug}/${region}: "${identity}"`);
        }
      }
    }

    // Confirmed splits explain the drift, but enough of them would still move
    // the letter, and the row publishes the headline one. The old tolerance was
    // the A band: three splits in a thousand tests is 0.3% against 5%, so it
    // could not bind until the registry grew seventeenfold. Comparing the two
    // letters binds from the first split that would move one.
    //
    // A note on the day this first fires. The comparison is the published
    // letter against the worst region's, so at full coverage it reads A+ versus
    // A: the first target ever to earn A+ while any confirmed split exists will
    // fail this, and the trigger is the ordinary A+ case rather than an
    // anomaly. That is deliberate - an A+ that holds only in the headline
    // region is the claim this guard exists to question - but read it as a
    // prompt to revisit the criteria in the open, not as a defect in the target
    // that tripped it.
    //
    // Coverage comes from the suite's axesOf over the headline region's counts.
    // Withdrawal is region-invariant, so only the divergence moves between the
    // two readings.
    const headlineRegion = (t.regions ?? []).find((r) => r.region === t.suiteHeadlineRegion);
    const coverage = headlineRegion ? axesOf(headlineRegion).coverage : null;
    const headlineLetter = gradeOf(best, coverage)?.letter ?? null;
    const worstLetter = gradeOf(worst, coverage)?.letter ?? null;
    // A null letter on either side means the grade could not be derived, which
    // would make the comparison pass by agreeing on nothing.
    if (headlineLetter === null || worstLetter === null) {
      uncheckable.push(`${slug} published no derivable grade to compare its worst region against`);
    } else if (headlineLetter !== worstLetter) {
      unconfirmed.push(
        `${slug} publishes ${headlineLetter} from its headline region but its worst region grades ${worstLetter}`,
      );
    }
  }

  return { guarded, unconfirmed, uncheckable };
}
