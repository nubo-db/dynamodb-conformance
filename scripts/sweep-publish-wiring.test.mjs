import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'

// The sweep commits registry/regions.json to main, and the published board is
// derived from that record: the observed set is the denominator of every
// target's Regions column, and admitting or dropping a region can move a
// target's headline region and so its grade. All three paths sit in
// conformance.yml's paths-ignore, so the sweep's push starts no conformance
// run and results-table.yml - which only publishes off one - never fires. If
// the sweep records health without rebuilding the board, nothing else will,
// and the freshness tests in the cheap gate hold main red until an unrelated
// merge happens to refresh it.
//
// The freshness tests catch that only once a bad sweep has already landed.
// These assertions are what make the wiring itself loud, in the same spirit as
// registry-wiring.test.mjs: prose identity joined across artefacts, asserted
// here rather than discovered in production a week later.

const workflow = readFileSync('.github/workflows/sweep.yml', 'utf8')

// The commit step, isolated so an occurrence of any of these strings elsewhere
// in the file cannot satisfy the ordering assertions below.
const step = (() => {
  const start = workflow.indexOf('- name: Commit the region health record')
  expect(start, 'sweep.yml no longer has a "Commit the region health record" step').toBeGreaterThan(
    -1,
  )
  const next = workflow.indexOf('\n  cleanup:', start)
  return workflow.slice(start, next === -1 ? undefined : next)
})()

// Comments stripped before any ordering is asserted. This step explains itself
// at length, and prose that quotes a command it is describing would otherwise
// satisfy - or, worse, silently invert - an ordering assertion about the code.
const code = step
  .split('\n')
  .filter((line) => !line.trim().startsWith('#'))
  .join('\n')

// Presence is asserted on every lookup, so an ordering check cannot pass
// vacuously. `expect(at(a)).toBeLessThan(at(b))` reads as a claim about order,
// but a deleted `a` indexes to -1 and satisfies it against any b - so the
// assertion guarding a line goes green precisely when that line is removed,
// which is the regression it exists to catch.
const at = (needle) => {
  const index = code.indexOf(needle)
  expect(index, `sweep.yml's commit step no longer contains \`${needle}\``).toBeGreaterThan(-1)
  return index
}

describe('the sweep publishes the board it derives from the health record', () => {
  it('regenerates the table and the badges', () => {
    expect(at('node scripts/summarise.mjs --write')).toBeGreaterThan(-1)
    expect(at('node scripts/badges.mjs')).toBeGreaterThan(-1)
  })

  it('regenerates after staging the record and before committing', () => {
    expect(at('git add registry/regions.json')).toBeLessThan(at('node scripts/summarise.mjs'))
    expect(at('node scripts/badges.mjs')).toBeLessThan(at('git commit'))
  })

  it('stages the board before the commit, not after it', () => {
    // Presence alone is not the invariant. Staging that lands after the commit
    // leaves the board staged-but-never-committed, which pushes nothing and
    // reads, in the diff, exactly like staging that works.
    expect(at('git add README.md results')).toBeLessThan(at('git commit'))
  })

  it('regenerates before deciding there is nothing to commit', () => {
    // The early exit must not gate regeneration, or the run that most needs a
    // rebuild - a hand re-run after a degraded run, whose health record comes
    // out byte-identical - is the one run that skips it.
    expect(at('node scripts/summarise.mjs --write')).toBeLessThan(
      at('if git diff --staged --quiet; then'),
    )
  })

  it('commits the record even when regeneration fails', () => {
    // The step runs under `bash -e` and both scripts refuse to publish rather
    // than publish something wrong, so an unguarded invocation between the
    // staging and the commit would end the step with the record uncommitted -
    // losing a week of health for a board the next run could have rebuilt.
    const guarded = /if node scripts\/summarise\.mjs --write && node scripts\/badges\.mjs; then/.test(
      code,
    )
    expect(guarded, 'regeneration must be guarded so a throw cannot skip the commit').toBe(true)
    expect(at('::warning title=Board')).toBeGreaterThan(-1)
    // A half-written board (publish() splices README before writing the
    // summary) must be discarded rather than committed beside the record.
    expect(at('git checkout -- README.md results')).toBeGreaterThan(-1)
  })

  it('clears a conflicted rebase before each push retry', () => {
    // Every later `git pull --rebase` refuses while a rebase is in progress,
    // so without this the second and third attempts are dead on arrival.
    expect(at('git rebase --abort')).toBeLessThan(at('git pull --rebase'))
  })
})
