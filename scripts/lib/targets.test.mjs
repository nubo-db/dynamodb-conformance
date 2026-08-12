import { describe, it, expect } from 'vitest'
import { readFileSync, readdirSync } from 'node:fs'
import { join } from 'node:path'
import {
  CHANNELS,
  TARGETS,
  configurationOf,
  display,
  distributionOf,
  isVariant,
  label,
  projectOf,
  repoUrl,
} from './targets.mjs'
import { GROUND_TRUTH_SLUG, isPublishedTarget, isTargetResultFile } from './score.mjs'

// The registry is hand-maintained, unlike every figure the board publishes, so
// what can be checked about it is checked here: that its relationships are
// coherent, and that a distribution claim always carries the page proving it.

describe('the target registry', () => {
  it('gives every target a display name and a home', () => {
    for (const [slug, t] of Object.entries(TARGETS)) {
      expect(t.display, `${slug} has no display name`).toBeTruthy()
      expect(t.url, `${slug} has no url`).toMatch(/^https:\/\//)
    }
  })

  it('groups every target under a project', () => {
    for (const slug of Object.keys(TARGETS)) {
      expect(projectOf(slug), `${slug} has no project`).toBeTruthy()
    }
  })

  it('gives each project exactly one reference configuration', () => {
    // The parent row carries the reference configuration's figures, so a
    // project with none would have no row and one with two would have the
    // wrong figures on whichever won the tie.
    const byProject = new Map()
    for (const [slug, t] of Object.entries(TARGETS)) {
      const list = byProject.get(t.project) ?? []
      list.push({ slug, reference: Boolean(t.reference) })
      byProject.set(t.project, list)
    }
    const problems = []
    for (const [project, members] of byProject) {
      const refs = members.filter((m) => m.reference)
      if (refs.length !== 1) {
        problems.push(`${project}: ${refs.length} reference configurations`)
      }
    }
    expect(problems, problems.join('\n')).toEqual([])
  })

  it('names the configuration of every project that has more than one', () => {
    // With two builds in play, an unlabelled row cannot say which was measured.
    const counts = {}
    for (const t of Object.values(TARGETS)) counts[t.project] = (counts[t.project] ?? 0) + 1
    const problems = Object.entries(TARGETS)
      .filter(([, t]) => counts[t.project] > 1 && !t.configuration)
      .map(([slug]) => slug)
    expect(problems, `unlabelled members of a multi-build project: ${problems}`).toEqual([])
  })

  it('marks exactly the non-reference configurations as variants', () => {
    expect(isVariant('dynoxide-wasm')).toBe(true)
    expect(isVariant('dynoxide')).toBe(false)
    expect(configurationOf('dynoxide-wasm')).toBe('WebAssembly / OPFS')
  })

  it('has a row for every target the results directory publishes', () => {
    // A results file with no registry entry renders under a hyphen-stripped
    // slug with no link, which is how a new target reaches the board unnamed.
    // Through the shared predicate, which is the criterion the pipeline
    // applies: results/ also holds derived artefacts (the tag manifest, the
    // summary) and the gitignored scratch slug, none of which are targets.
    const published = readdirSync('results')
      .filter(isTargetResultFile)
      .map((f) => f.replace(/\.json$/, ''))
    const missing = published.filter((slug) => !TARGETS[slug])
    expect(missing, `results files with no registry entry: ${missing}`).toEqual([])
  })

  it('keeps the ground truth in the registry, since it heads the table', () => {
    expect(TARGETS[GROUND_TRUTH_SLUG]).toBeTruthy()
  })
})

describe('distribution claims', () => {
  it('sources every channel it claims', () => {
    // Nothing in the suite can observe that a project ships a Docker image, so
    // the claim travels with the page documenting it and the board links there.
    // An unsourced channel is an assertion the reader cannot check.
    const problems = []
    for (const slug of Object.keys(TARGETS)) {
      for (const d of TARGETS[slug].distribution ?? []) {
        if (!d.url || !/^https:\/\//.test(d.url)) {
          problems.push(`${slug}/${d.channel}: no source url`)
        }
      }
    }
    expect(problems, problems.join('\n')).toEqual([])
  })

  it('draws channels from the declared vocabulary', () => {
    const problems = []
    for (const slug of Object.keys(TARGETS)) {
      for (const d of TARGETS[slug].distribution ?? []) {
        if (!CHANNELS[d.channel]) problems.push(`${slug}: unknown channel "${d.channel}"`)
      }
    }
    expect(problems, problems.join('\n')).toEqual([])
  })

  it('labels each channel for display', () => {
    for (const slug of Object.keys(TARGETS)) {
      for (const d of distributionOf(slug)) expect(d.label, `${slug}/${d.channel}`).toBeTruthy()
    }
  })

  it('declares no lineage: a relationship between engines is measured, not typed', () => {
    // scripts/lib/lineage.mjs derives it from shared failures. A typed
    // "built on" would be a claim about someone else's product that nothing
    // checks and that goes stale silently when they re-implement.
    const declared = Object.entries(TARGETS).filter(
      ([, t]) => t.dependsOn || t.builtOn || t.lineage,
    )
    expect(declared.map(([slug]) => slug)).toEqual([])
  })
})

describe('display helpers', () => {
  it('falls back to a readable name for an unregistered slug', () => {
    expect(display('some-new-engine')).toBe('some new engine')
    expect(repoUrl('some-new-engine')).toBeNull()
  })

  it('links a registered target and leaves an unregistered one bare', () => {
    expect(label('dynoxide')).toBe('[Dynoxide](https://github.com/nubo-db/dynoxide)')
    expect(label('some-new-engine')).toBe('some new engine')
  })
})
