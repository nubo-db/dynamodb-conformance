import { describe, it, expect } from 'vitest'
import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { stripLiterals } from './lib/tag-content.mjs'

// Guards that every test file declares the shared tables it uses.
//
// Provisioning is demand-driven: src/setup.ts creates the tables the running
// file declared via `declareTables(...)`, and nothing else. A file that uses a
// shared def without declaring it still passes a full run, because some other
// selected file will have declared the same table - and then fails with
// ResourceNotFoundException on the narrower run that does not, which is scored
// as a behavioural disagreement rather than the scaffolding error it is.
//
// The reverse direction matters for the index axes. A file declaring a table it
// no longer uses keeps creating it, so an exclusion that should have skipped
// that table silently still provisions it.
//
// Static parsing rather than a runtime check, for the same reason the tag guard
// is static: the declaration is the whole truth, reading it needs no AWS, and
// the failure it prevents only reproduces under a filtered run.

const TEST_DIR = 'tests'
const HELPERS = 'src/helpers.ts'

/** The shared table defs exported from src/helpers.ts. */
function sharedDefNames() {
  const src = readFileSync(HELPERS, 'utf8')
  return new Set(
    [...src.matchAll(/^export const (\w+TableDef)\s*:/gm)].map((m) => m[1]),
  )
}

/**
 * Shared defs carrying GSIs and LSIs together.
 *
 * Derived rather than named, because the list grows: this was hard-coded to
 * `compositeIndexedTableDef` and missed `partiqlIndexTableDef` when it arrived,
 * so the guard below passed on exactly the file it exists to catch.
 */
function dualIndexDefNames() {
  const src = readFileSync(HELPERS, 'utf8')
  const names = new Set()
  const heads = /^export const (\w+TableDef)\s*:[^=]*=\s*\{$/gm
  for (const m of src.matchAll(heads)) {
    // A def's own terminator is the only `}` at column 0 after its head; every
    // nested object inside it is indented.
    const rest = src.slice(m.index + m[0].length)
    const end = rest.search(/^\}/m)
    const body = end === -1 ? rest : rest.slice(0, end)
    if (/^\s*gsis:/m.test(body) && /^\s*lsis:/m.test(body)) names.add(m[1])
  }
  return names
}

/** The defs named in a file's `declareTables(...)` call. */
function declaredIn(stripped) {
  const call = stripped.match(/declareTables\(([^)]*)\)/)
  if (!call) return null
  return new Set(
    call[1]
      .split(',')
      .map((s) => s.trim())
      .filter(Boolean),
  )
}

/** Shared defs a file actually references, ignoring the declaration itself. */
function usedIn(stripped, shared) {
  const withoutDeclaration = stripped.replace(/declareTables\([^)]*\)/, '')
  const used = new Set()
  for (const name of shared) {
    if (new RegExp(`\\b${name}\\b`).test(withoutDeclaration)) used.add(name)
  }
  return used
}

const testFiles = readdirSync(TEST_DIR, { recursive: true })
  .map((f) => f.toString())
  .filter((f) => f.endsWith('.test.ts'))
  .map((f) => join(TEST_DIR, f))

describe('the single-fork invariant the registry rests on', () => {
  // The creation memo and the sweep guard are module state, so they are only
  // shared across files while the suite runs as one non-isolated worker.
  // Raising either setting makes the memo reset per file: every file would
  // re-sweep and delete tables still in use. It fails silently, so assert it.
  const config = readFileSync('vitest.config.ts', 'utf8')

  it('runs a single worker', () => {
    expect(config).toMatch(/^\s*maxWorkers:\s*1\s*,/m)
  })

  it('does not isolate modules between files', () => {
    expect(config).toMatch(/^\s*isolate:\s*false\s*,/m)
  })
})

describe('shared table declaration guard', () => {
  const shared = sharedDefNames()

  it('finds the shared table defs', () => {
    expect(shared.size).toBeGreaterThan(3)
    expect(shared).toContain('compositeTableDef')
  })

  it('discovers the test suite', () => {
    expect(testFiles.length).toBeGreaterThan(50)
  })

  it('every file declares the shared tables it uses', () => {
    const problems = []
    for (const file of testFiles) {
      const stripped = stripLiterals(readFileSync(file, 'utf8'))
      const used = usedIn(stripped, shared)
      if (used.size === 0) continue

      const declared = declaredIn(stripped)
      if (declared === null) {
        problems.push(
          `${file}: uses ${[...used].join(', ')} but never calls declareTables()`,
        )
        continue
      }
      const missing = [...used].filter((n) => !declared.has(n))
      if (missing.length) {
        problems.push(`${file}: uses ${missing.join(', ')} without declaring it`)
      }
    }
    expect(
      problems,
      `shared tables used without being declared:\n${problems.join('\n')}`,
    ).toEqual([])
  })

  it('no file declares a shared table it does not use', () => {
    const problems = []
    for (const file of testFiles) {
      const stripped = stripLiterals(readFileSync(file, 'utf8'))
      const declared = declaredIn(stripped)
      if (!declared || declared.size === 0) continue

      const used = usedIn(stripped, shared)
      const unused = [...declared].filter((n) => shared.has(n) && !used.has(n))
      if (unused.length) {
        problems.push(`${file}: declares ${unused.join(', ')} but never uses it`)
      }
    }
    expect(
      problems,
      `shared tables declared but unused (they would still be created):\n${problems.join('\n')}`,
    ).toEqual([])
  })

  it('a file declaring a table with both index kinds carries both index tags', () => {
    // A def carrying LSIs and GSIs together means a run excluding either axis
    // must exclude the whole file - otherwise the excluded index kind is
    // created anyway and an engine lacking it fails in setup, which is the
    // failure the exclusion exists to prevent.
    const dualIndex = dualIndexDefNames()
    expect(
      dualIndex.size,
      'no shared def carries both index kinds, so this guard is checking nothing',
    ).toBeGreaterThan(0)

    const problems = []
    for (const file of testFiles) {
      const src = readFileSync(file, 'utf8')
      const stripped = stripLiterals(src)
      const declared = declaredIn(stripped)
      const dual = [...(declared ?? [])].filter((n) => dualIndex.has(n))
      if (dual.length === 0) continue

      for (const line of src.split('\n')) {
        if (!line.startsWith('describe(')) continue
        const tags = line.match(/\{\s*tags:\s*\[([^\]]*)\]\s*\}/)
        const names = tags
          ? [...tags[1].matchAll(/['"]([^'"]+)['"]/g)].map((m) => m[1])
          : []
        const missing = ['gsi', 'lsi'].filter((t) => !names.includes(t))
        if (missing.length) {
          const title = line.match(/^describe\((['"])(.*?)\1/)?.[2] ?? line.slice(0, 60)
          problems.push(
            `${file} « ${title} » declares ${dual.join(', ')}: missing ${missing.join(', ')}`,
          )
        }
      }
    }
    expect(
      problems,
      `describes using the index-bearing table without both index tags:\n${problems.join('\n')}`,
    ).toEqual([])
  })
})
