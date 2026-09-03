import { readFileSync } from 'node:fs'
import { describe, expect, it } from 'vitest'
import { load as loadYaml } from 'js-yaml'
import {
  assertPlainVersion,
  assertUnambiguousRef,
  confirmTagKind,
  releaseTagsByVersion,
  resolveMeasuredRef,
} from './resolve-measured-ref.mjs'

const TAGS = ['v2.0.0', 'v2.1.0', 'v3.0.0', 'v3.1.0']

describe('releaseTagsByVersion', () => {
  it('orders by version, highest first', () => {
    expect(releaseTagsByVersion(TAGS)).toEqual(['v3.1.0', 'v3.0.0', 'v2.1.0', 'v2.0.0'])
  })

  it('compares components numerically, so v3.10.0 beats v3.9.0', () => {
    // Lexical ordering puts v3.10.0 before v3.9.0 and would pick the wrong
    // release the first time a minor reaches double digits.
    expect(releaseTagsByVersion(['v3.9.0', 'v3.10.0'])[0]).toBe('v3.10.0')
    expect(releaseTagsByVersion(['v3.1.9', 'v3.1.10'])[0]).toBe('v3.1.10')
  })

  it('ignores anything that is not exactly vMAJOR.MINOR.PATCH', () => {
    // A tag this cannot read is a tag it must not choose.
    expect(releaseTagsByVersion(['v3.1.0', 'latest', 'v4', 'release-5.0.0', 'v9.9.9-rc1'])).toEqual([
      'v3.1.0',
    ])
  })

  it('returns nothing when no tag is a release tag', () => {
    expect(releaseTagsByVersion(['nightly', 'v1.2'])).toEqual([])
    expect(releaseTagsByVersion([])).toEqual([])
  })
})

describe('resolveMeasuredRef', () => {
  it('resolves a schedule to the latest release tag', () => {
    expect(resolveMeasuredRef({ event: 'schedule', tags: TAGS })).toEqual({
      ref: 'v3.1.0',
      kind: 'tag',
    })
  })

  it('resolves a push to the pushed sha, which is never publishable', () => {
    const r = resolveMeasuredRef({ event: 'push', sha: 'abc123', tags: TAGS })
    expect(r.ref).toBe('abc123')
    expect(r.kind).toBe('sha')
  })

  it('takes an explicit ref verbatim, whatever the event', () => {
    expect(resolveMeasuredRef({ event: 'workflow_dispatch', inputRef: 'v3.0.0', tags: TAGS })).toEqual(
      { ref: 'v3.0.0', kind: 'tag' },
    )
    // Even on a push: an explicit ref is a deliberate instruction.
    expect(resolveMeasuredRef({ event: 'push', sha: 'abc123', inputRef: 'v2.1.0', tags: TAGS }).ref).toBe(
      'v2.1.0',
    )
  })

  it('marks an explicit non-tag ref unpublishable, so a re-measure of main cannot publish', () => {
    expect(resolveMeasuredRef({ event: 'workflow_dispatch', inputRef: 'main', tags: TAGS })).toEqual({
      ref: 'main',
      kind: 'other',
    })
  })

  it('prefers version order over the order tags were written', () => {
    // v2.0.0 and v2.1.0 were backfilled after v3.0.0 in this repo, so a
    // creation-ordered rule would answer v2.1.0 here.
    expect(resolveMeasuredRef({ event: 'schedule', tags: ['v3.0.0', 'v2.0.0', 'v2.1.0'] }).ref).toBe(
      'v3.0.0',
    )
  })

  it('falls back to main when there are no release tags, and refuses to call it publishable', () => {
    // A fresh clone, or a repo before its first cut. Failing here would brick
    // the workflow; claiming a tag would publish something no release earned.
    expect(resolveMeasuredRef({ event: 'schedule', tags: [] })).toEqual({
      ref: 'main',
      kind: 'other',
    })
  })

  it('refuses a push that names no sha rather than guessing', () => {
    expect(() => resolveMeasuredRef({ event: 'push', tags: TAGS })).toThrow(/sha/)
  })

  it('measures the PR head on a pull_request, never the released tag', () => {
    // The event list is closed. An open one sent every PR's jobs to the latest
    // release, so a PR adding a conformance test ran the old tests and passed.
    const r = resolveMeasuredRef({ event: 'pull_request', sha: 'prhead1', tags: TAGS })
    expect(r).toEqual({ ref: 'prhead1', kind: 'sha' })
  })

  it('measures its own sha for any event that is not a schedule or a dispatch', () => {
    for (const event of ['pull_request', 'push', 'merge_group', 'repository_dispatch', 'release']) {
      const r = resolveMeasuredRef({ event, sha: 'abc123', tags: TAGS })
      expect(r.kind, `${event} resolved to ${r.ref}`).toBe('sha')
      expect(r.ref, `${event} resolved to ${r.ref}`).toBe('abc123')
    }
  })
})

describe('confirmTagKind', () => {
  const COMMIT = '9129f0fbfb6fb5ff01aadf5f9f957fa0bf1871ad'

  it('confirms a tag that exists and points at the measured commit', () => {
    expect(confirmTagKind('v3.1.0', COMMIT, { git: () => COMMIT })).toBe('tag')
  })

  it('refuses a branch that merely looks like a tag', () => {
    // A branch named v9.9.9 pattern-matches as a release. Publishing is gated
    // on this field, so the claim is settled by git rather than by the string.
    const git = () => {
      throw new Error('fatal: needed a single revision')
    }
    expect(confirmTagKind('v9.9.9', COMMIT, { git })).toBe('other')
  })

  it('refuses a tag that resolves to a different commit than was measured', () => {
    // A tag deleted and re-cut elsewhere after the measurement ran.
    expect(confirmTagKind('v3.1.0', COMMIT, { git: () => 'deadbeef' })).toBe('other')
  })

  it('does not call git for a ref that is not tag-shaped', () => {
    let called = false
    const kind = confirmTagKind('abc123', COMMIT, {
      git: () => {
        called = true
        return COMMIT
      },
    })
    expect(kind).toBe('other')
    expect(called).toBe(false)
  })
})

describe('the committed workflow consumes the resolved ref', () => {
  const workflow = loadYaml(readFileSync('.github/workflows/conformance.yml', 'utf8'))
  const jobs = Object.entries(workflow.jobs)

  // The checkout that reads this repo, as opposed to the second checkouts that
  // clone a target engine from its own repository.
  const suiteCheckouts = (job) =>
    (job.steps ?? []).filter(
      (s) => typeof s.uses === 'string' && s.uses.startsWith('actions/checkout') && !s.with?.repository,
    )

  // `changes` resolves the ref, so it reads the triggering ref by definition.
  // `capture-cross-region` commits back to main and has to stand on main to do
  // it, so it is deliberately left there.
  const RESOLVES_OR_COMMITS = new Set(['changes', 'capture-cross-region'])

  it('every suite checkout outside the resolver and the committer takes the resolved ref', () => {
    const wrong = []
    for (const [id, job] of jobs) {
      if (RESOLVES_OR_COMMITS.has(id)) continue
      for (const step of suiteCheckouts(job)) {
        const ref = step.with?.ref
        if (typeof ref !== 'string' || !ref.includes('needs.changes.outputs.ref')) {
          wrong.push(`${id} (ref: ${ref ?? 'unset'})`)
        }
      }
    }
    expect(wrong, `these check out the triggering ref instead of the measured one: ${wrong.join(', ')}`)
      .toEqual([])
  })

  it('keeps the committing job on main, so it never commits from a detached tag', () => {
    const capture = workflow.jobs['capture-cross-region']
    expect(suiteCheckouts(capture)[0].with.ref).toBe('main')
  })

  it('leaves the target-engine checkouts alone, since they clone another repo', () => {
    const external = jobs.flatMap(([, job]) =>
      (job.steps ?? []).filter((s) => typeof s.uses === 'string' && s.uses.startsWith('actions/checkout') && s.with?.repository),
    )
    expect(external.length).toBeGreaterThan(0)
    for (const step of external) {
      expect(step.with.ref).not.toContain('needs.changes.outputs.ref')
    }
  })

  it('publishes the resolved ref, its kind, commit and suite version as job outputs', () => {
    const outputs = workflow.jobs.changes.outputs
    for (const key of ['ref', 'kind', 'commit', 'version']) {
      expect(outputs, `changes job does not output ${key}`).toHaveProperty(key)
    }
  })
})

describe('the sweep measures the same suite the board publishes', () => {
  const sweep = loadYaml(readFileSync('.github/workflows/sweep.yml', 'utf8'))

  it('resolves the measured ref once and publishes it as an output', () => {
    const outputs = sweep.jobs.regions.outputs
    for (const key of ['ref', 'commit', 'version']) {
      expect(outputs, `regions job does not output ${key}`).toHaveProperty(key)
    }
  })

  it('runs the suite at the measured ref', () => {
    const checkout = (sweep.jobs.sweep.steps ?? []).find((s) =>
      (s.uses ?? '').startsWith('actions/checkout'),
    )
    expect(checkout.with?.ref).toContain('needs.regions.outputs.ref')
  })

  it('keeps the committing job on main, which is why it cannot read the suite from its own tree', () => {
    const checkout = (sweep.jobs.detect.steps ?? []).find((s) =>
      (s.uses ?? '').startsWith('actions/checkout'),
    )
    expect(checkout.with?.ref).toBe('main')
  })

  it('detects splits against the registry the sweep measured, at the commit not the ref', () => {
    const step = (sweep.jobs.detect.steps ?? []).find((s) =>
      (s.run ?? '').includes('sweep-detect.mjs'),
    )
    expect(step.run).toContain('--registry')
    expect(step.env?.MEASURED_REF).toContain('needs.regions.outputs.commit')
  })
})

describe('assertUnambiguousRef', () => {
  const gitWith = (present) => (spec) => {
    if (present.includes(spec)) return 'abc123'
    throw new Error(`unknown revision ${spec}`)
  }

  it('accepts a tag with no branch of the same name', () => {
    expect(assertUnambiguousRef('v3.2.0', { git: gitWith(['refs/tags/v3.2.0']) })).toBe('v3.2.0')
  })

  it('refuses a name that is both a tag and a branch', () => {
    // actions/checkout resolves a bare name by looking for a branch first,
    // while this module asks git for refs/tags - so the measuring jobs would
    // read the branch and the publish gate would confirm the tag.
    expect(() =>
      assertUnambiguousRef('v3.2.0', { git: gitWith(['refs/tags/v3.2.0', 'refs/heads/v3.2.0']) }),
    ).toThrow(/a branch of that name exists/)
  })

  it('ignores anything that is not release-shaped', () => {
    // A sha or a branch name is never resolved through refs/tags, so there is
    // no ambiguity to refuse.
    expect(assertUnambiguousRef('main', { git: gitWith(['refs/heads/main']) })).toBe('main')
  })
})

describe('assertPlainVersion', () => {
  it('accepts a version the tag and JSON conventions can both carry', () => {
    for (const v of ['3.2.0', '3.10.12', '1.2.3-rc1+build']) {
      expect(assertPlainVersion(v)).toBe(v)
    }
  })

  it('refuses a version that could close a JSON string and forge a field', () => {
    // Read from package.json at whatever ref was dispatched, so it is not ours
    // to trust. `kind` is the only gate on publishing.
    expect(() => assertPlainVersion('3.1.0", "kind": "tag')).toThrow(/may contain only/)
    expect(() => assertPlainVersion('3.1.0\n')).toThrow(/may contain only/)
    expect(() => assertPlainVersion(undefined)).toThrow(/may contain only/)
  })
})
