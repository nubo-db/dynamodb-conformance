import { describe, it, expect } from 'vitest'
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { TAG_NAMES, PLANE_TAGS } from '../src/tags.js'
import { buildManifest } from './tag-manifest.mjs'

// Guards that the feature/capability tags stay trustworthy as the suite grows.
// A `--tags-filter` is only honest if every test that belongs to an axis is
// actually tagged; an untagged file would silently slip through an exclusion.
//
// Why static parsing rather than booting vitest: every top-level describe
// carries an inline `{ tags: [...] }` literal (no const indirection, no
// per-test overrides), and tags inherit from a top-level describe to all of its
// nested suites and tests. So validating every top-level describe — plus
// forbidding stray top-level it()/test() that would escape a describe and carry
// no tags — is equivalent to validating every test's resolved tags, with no AWS
// and no test run. strictTags in vitest.config.ts is the run-time backstop that
// rejects any undeclared tag whenever the suite actually executes.

const TEST_DIR = 'tests'
const PLANES = new Set(PLANE_TAGS)

const testFiles = readdirSync(TEST_DIR, { recursive: true })
  .map((f) => f.toString())
  .filter((f) => f.endsWith('.test.ts'))
  .map((f) => join(TEST_DIR, f))

// Every top-level (column-0) describe with its declared tags, or null if untagged.
function topLevelDescribes(src) {
  const found = []
  for (const line of src.split('\n')) {
    if (!line.startsWith('describe(')) continue
    const titleMatch = line.match(/^describe\((['"])(.*?)\1,/)
    const title = titleMatch ? titleMatch[2] : line.slice(0, 60)
    const tagsMatch = line.match(/\{\s*tags:\s*\[([^\]]*)\]\s*\}/)
    const tags = tagsMatch
      ? [...tagsMatch[1].matchAll(/['"]([^'"]+)['"]/g)].map((m) => m[1])
      : null
    found.push({ title, tags })
  }
  return found
}

describe('tag coverage guard', () => {
  it('discovers the test suite', () => {
    expect(testFiles.length).toBeGreaterThan(50)
  })

  it('every top-level describe is tagged, all tags are declared, and exactly one is a plane', () => {
    const problems = []
    for (const file of testFiles) {
      const src = readFileSync(file, 'utf8')
      for (const { title, tags } of topLevelDescribes(src)) {
        const where = `${file} « ${title} »`
        if (tags === null) {
          problems.push(`${where}: no tags`)
          continue
        }
        if (tags.length === 0) {
          problems.push(`${where}: empty tags array`)
          continue
        }
        const undeclared = tags.filter((t) => !TAG_NAMES.has(t))
        if (undeclared.length > 0) {
          problems.push(`${where}: undeclared tag(s): ${undeclared.join(', ')}`)
        }
        const planes = tags.filter((t) => PLANES.has(t))
        if (planes.length !== 1) {
          problems.push(`${where}: ${planes.length} plane tags (need exactly one of ${PLANE_TAGS.join('/')})`)
        }
      }
    }
    expect(problems, `tag problems:\n${problems.join('\n')}`).toEqual([])
  })

  it('has no top-level it()/test() that would escape describe-level tagging', () => {
    const offenders = []
    for (const file of testFiles) {
      const src = readFileSync(file, 'utf8')
      if (src.split('\n').some((l) => /^(it|test)[.(]/.test(l))) offenders.push(file)
    }
    expect(offenders, `top-level tests outside any describe:\n${offenders.join('\n')}`).toEqual([])
  })

  it('the README tag table matches the declared vocabulary', () => {
    const readme = readFileSync('README.md', 'utf8')
    const block = readme.split('<!-- tags:start -->')[1]?.split('<!-- tags:end -->')[0]
    expect(block, 'README tag block (<!-- tags:start --> / <!-- tags:end -->) not found').toBeTruthy()
    // First-column backtick token of each table row is the tag name.
    const documented = new Set([...block.matchAll(/^\|\s*`([a-z-]+)`\s*\|/gm)].map((m) => m[1]))
    const missingFromReadme = [...TAG_NAMES].filter((t) => !documented.has(t))
    const staleInReadme = [...documented].filter((t) => !TAG_NAMES.has(t))
    expect(
      { missingFromReadme, staleInReadme },
      'README tag table drifted from src/tags.ts',
    ).toEqual({ missingFromReadme: [], staleInReadme: [] })
  })

  it('the published results/tag-manifest.json is up to date with the test sources', () => {
    const committed = JSON.parse(readFileSync('results/tag-manifest.json', 'utf8'))
    // The manifest is consumed by paritysuite.org; a stale committed copy would
    // silently mis-group the published per-capability results. Regenerate with
    // `npm run results:tags`.
    expect(committed, 'results/tag-manifest.json is stale — run `npm run results:tags`').toEqual(buildManifest())
  })
})
