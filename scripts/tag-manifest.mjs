// Generate the tag manifest: every test file's top-level describes mapped to
// the tags applied to them, parsed straight from the test sources — the same
// inline tags that strictTags and the coverage guard enforce, so this is a
// faithful extraction of the single source of truth, not a second taxonomy.
//
// Published to results/tag-manifest.json so paritysuite.org can group results
// by capability (its results JSON carries file path + top-level describe title
// per test, which is the manifest's join key) without re-deriving the tagging.
//
// Run: `npm run results:tags` (regenerates the committed manifest). The
// coverage guard fails if the committed file drifts from the sources.

import { readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { join } from 'node:path'

const TEST_DIR = 'tests'

// Top-level (column-0) describes and their inline tags, parsed the same way as
// scripts/tag-coverage.test.mjs (tags are inlined literals, no const indirection).
function topLevelDescribes(src) {
  const found = []
  for (const line of src.split('\n')) {
    if (!line.startsWith('describe(')) continue
    const titleMatch = line.match(/^describe\((['"])(.*?)\1,/)
    if (!titleMatch) continue
    const tagsMatch = line.match(/\{\s*tags:\s*\[([^\]]*)\]\s*\}/)
    const tags = tagsMatch
      ? [...tagsMatch[1].matchAll(/['"]([^'"]+)['"]/g)].map((m) => m[1])
      : []
    found.push({ title: titleMatch[2], tags })
  }
  return found
}

export function buildManifest(testDir = TEST_DIR) {
  const files = readdirSync(testDir, { recursive: true })
    .map((f) => f.toString())
    .filter((f) => f.endsWith('.test.ts'))
    .map((f) => join(testDir, f).replace(/\\/g, '/'))
    .sort()

  const describes = {}
  for (const file of files) {
    const entry = {}
    for (const { title, tags } of topLevelDescribes(readFileSync(file, 'utf8'))) {
      entry[title] = tags
    }
    describes[file] = entry
  }
  return { schema: 1, describes }
}

// CLI: write the committed manifest.
if (import.meta.url === `file://${process.argv[1]}`) {
  const manifest = buildManifest()
  writeFileSync('results/tag-manifest.json', `${JSON.stringify(manifest, null, 2)}\n`)
  const fileCount = Object.keys(manifest.describes).length
  const describeCount = Object.values(manifest.describes).reduce((s, d) => s + Object.keys(d).length, 0)
  console.log(`wrote results/tag-manifest.json: ${fileCount} files, ${describeCount} describes`)
}
