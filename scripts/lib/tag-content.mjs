// Parse a test file into its describe/test blocks with the tags applied to
// each, and check that a test exercising a tagged capability actually carries
// that capability's tag.
//
// Why this exists: a `--tags-filter` is only honest if every test that belongs
// to an axis is tagged. The coverage guard checks that a describe carries *some*
// tag, which is the "is tagged" half. It cannot see the other half - whether a
// test sending `AttributesToGet` carries `legacy` - and that gap is how seven
// legacy tests sat inside a `!legacy` run for two months.
//
// Why static parsing rather than vitest's collection: collection resolves the
// tags a test *has*, and says nothing about what the test *sends*. The check
// needs both sides, and only the source has the second one.
//
// Kept free of any `src/` import so `scripts/tag-manifest.mjs` can consume it
// under plain node (`npm run results:tags`); `src/tags.ts` is TypeScript and
// would need a transform. The marker table's tag names are asserted against the
// canonical vocabulary in this module's sibling test instead, which runs under
// vitest and can import it.

/** Opens a describe/it/test block. Covers `it.each(...)` and friends. */
const BLOCK_OPEN = /^(\s*)(describe|it|test)(\.[A-Za-z]+)?\s*\(/

/** The first string literal on the line, which is a block's title. */
const TITLE = /\(\s*(['"`])(.*?)\1/

/** An inline `{ tags: [...] }` options literal. */
const TAGS = /\{\s*tags:\s*\[([^\]]*)\]\s*\}/

/**
 * Source markers that imply a capability, and the tag each one requires.
 *
 * The legacy parameters are matched in object-key position, anywhere on the
 * line, so an inline `{ AttributesToGet: [pk] }` counts as much as a key on its
 * own line. They also appear inside asserted message strings ("...Index Key
 * lsi1sk Expected: S Actual: N", "Must specify the AttributesToGet or
 * ProjectionExpression..."), which assert about a message rather than send the
 * parameter. String contents are blanked before matching, so a quoted
 * occurrence never reaches these patterns; that is what separates the two, not
 * where the name sits on the line.
 */
export const CAPABILITY_MARKERS = [
  {
    tag: 'legacy',
    what: 'a deprecated request parameter',
    pattern:
      /\b(AttributesToGet|AttributeUpdates|Expected|QueryFilter|ScanFilter|KeyConditions|ConditionalOperator)\s*:/,
  },
  {
    tag: 'partiql',
    what: 'a PartiQL command',
    pattern: /\b(ExecuteStatementCommand|BatchExecuteStatementCommand|ExecuteTransactionCommand)\b/,
  },
  {
    // The category filenames miss entirely: a test that never names an index
    // but writes an index-key attribute, and is only rejected because that
    // attribute is an index key. Either axis satisfies it - the shared indexed
    // table carries both, so which one a case is filed under is a coin toss,
    // and demanding a specific one would just encode that arbitrariness.
    tag: 'gsi',
    anyOf: ['gsi', 'lsi'],
    what: 'a secondary index key attribute',
    pattern: /\b(lsi1sk|lsi2sk|bidx)\s*:/,
  },
  {
    tag: 'search-vectors',
    what: 'a SearchVectors command',
    pattern: /\b(SearchVectorsCommand)\b/,
  },
  {
    // Building or reshaping a table that carries a vector index. Mirrors the
    // secondary-index marker below: the table is created before any assertion
    // runs, so an exclusion filter has to catch the construction itself.
    tag: 'vector',
    what: 'a table carrying a vector index',
    pattern: /\b(VectorIndexes|VectorIndexUpdates)\s*:/,
  },
  {
    // Building a table that carries an index. This is the case an exclusion has
    // to catch, because the table is created before any assertion runs.
    //
    // Deliberately not `IndexName`: naming an index is not depending on one.
    // tests/tier3/validation-ordering/index.test.ts queries a name that exists
    // nowhere, on an index-free table, and an engine without index support
    // should still reject it - tagging it would drop a test that run can
    // legitimately execute. Same for GlobalSecondaryIndexUpdates, which is a
    // delete as often as an add.
    tag: 'gsi',
    anyOf: ['gsi', 'lsi'],
    what: 'a table carrying a secondary index',
    pattern: /\b(GlobalSecondaryIndexes|LocalSecondaryIndexes)\s*:/,
  },
]

/**
 * Blank the contents of string literals and comments, preserving line count and
 * column offsets so line numbers and indentation still line up.
 *
 * Both consumers need this. Brace counting would be thrown off by a lone brace
 * inside a message string, and marker matching would fire on a parameter name
 * that only appears in an assertion.
 */
export function stripLiterals(src) {
  const out = []
  let state = 'code'
  let quote = ''
  for (let i = 0; i < src.length; i++) {
    const c = src[i]
    const next = src[i + 1]
    if (state === 'code') {
      if (c === '/' && next === '/') { state = 'line-comment'; out.push('  '); i++; continue }
      if (c === '/' && next === '*') { state = 'block-comment'; out.push('  '); i++; continue }
      if (c === "'" || c === '"' || c === '`') { state = 'string'; quote = c; out.push(c); continue }
      out.push(c)
      continue
    }
    if (state === 'line-comment') {
      if (c === '\n') { state = 'code'; out.push('\n'); continue }
      out.push(' ')
      continue
    }
    if (state === 'block-comment') {
      if (c === '*' && next === '/') { state = 'code'; out.push('  '); i++; continue }
      out.push(c === '\n' ? '\n' : ' ')
      continue
    }
    // state === 'string'
    if (c === '\\') { out.push('  '); i++; continue }
    if (c === quote) { state = 'code'; out.push(c); continue }
    // An unterminated literal would run away; a newline inside a non-template
    // quote ends it, matching how the parser would fail anyway.
    if (c === '\n' && quote !== '`') { state = 'code'; out.push('\n'); continue }
    out.push(c === '\n' ? '\n' : ' ')
  }
  return out.join('')
}

function netBraces(line) {
  let n = 0
  for (const c of line) {
    if (c === '{') n++
    else if (c === '}') n--
  }
  return n
}

function tagsOn(line) {
  const m = line.match(TAGS)
  if (!m) return []
  return [...m[1].matchAll(/['"]([^'"]+)['"]/g)].map((x) => x[1])
}

/**
 * Parse a test source into nested blocks.
 *
 * Each block is `{ kind, title, tags, start, end, children }` with 1-indexed
 * line numbers. `kind` is 'describe' or 'test'. Tags are the ones applied
 * directly to that block, not the resolved set - callers walk the chain.
 */
export function parseBlocks(src) {
  const raw = src.split('\n')
  const stripped = stripLiterals(src).split('\n')
  const roots = []
  const stack = []
  let depth = 0

  raw.forEach((line, i) => {
    const open = line.match(BLOCK_OPEN)
    const before = depth
    depth += netBraces(stripped[i] ?? '')

    if (open) {
      const titleMatch = line.match(TITLE)
      const block = {
        kind: open[2] === 'describe' ? 'describe' : 'test',
        title: titleMatch ? titleMatch[2] : line.trim().slice(0, 60),
        tags: tagsOn(line),
        start: i + 1,
        end: null,
        depthAtOpen: before,
        children: [],
      }
      const parent = stack[stack.length - 1]
      if (parent) parent.children.push(block)
      else roots.push(block)
      stack.push(block)
      return
    }

    while (stack.length && depth <= stack[stack.length - 1].depthAtOpen) {
      stack.pop().end = i + 1
    }
  })

  // An unclosed block runs to the end of the file rather than staying null.
  for (const b of stack) b.end = raw.length
  return roots
}

/** Walk the block tree, yielding each block with its ancestor chain. */
function* walk(blocks, chain = []) {
  for (const b of blocks) {
    yield { block: b, chain: [...chain, b] }
    yield* walk(b.children, [...chain, b])
  }
}

/** Every tag applied anywhere along a chain of nested blocks. */
export function resolveTags(chain) {
  return new Set(chain.flatMap((b) => b.tags))
}

/**
 * The innermost block containing a line, and its ancestor chain.
 *
 * "Innermost" is the latest-opening block that still contains the line. A block
 * opening and closing on one line is not closed until the following line, so its
 * recorded end overlaps the next block's start; picking the latest opener is
 * what keeps that overlap from misattributing a marker to the previous test.
 */
function innermostAt(roots, line) {
  let found = null
  for (const { block, chain } of walk(roots)) {
    if (line < block.start || line > block.end) continue
    if (!found || block.start > found.block.start) found = { block, chain }
  }
  return found
}

/**
 * Capability markers that appear without the tag they imply.
 *
 * A marker inside a test needs the tag on that test or on any describe around
 * it. A marker in shared setup - a `beforeAll`, or anything else between tests -
 * makes every test in that describe depend on the capability, so it escalates
 * to the describe. A marker at module scope (outside every describe) applies to
 * the whole file, so every top-level describe must carry the tag; import lines
 * are skipped, since naming a command in an import is not sending it.
 *
 * Returns one entry per offending line:
 *   { line, tag, marker, scope, title }
 */
export function capabilityLeaks(src, { markers = CAPABILITY_MARKERS } = {}) {
  const roots = parseBlocks(src)
  const stripped = stripLiterals(src).split('\n')
  const leaks = []

  stripped.forEach((line, i) => {
    if (/^\s*import\b/.test(line)) return
    for (const marker of markers) {
      const hit = line.match(marker.pattern)
      if (!hit) continue
      const accepted = marker.anyOf ?? [marker.tag]
      const found = {
        line: i + 1,
        tag: accepted.join(' or '),
        marker: hit[1] ?? hit[0].trim(),
      }
      const at = innermostAt(roots, i + 1)

      if (!at) {
        for (const d of roots.filter((r) => !accepted.some((t) => r.tags.includes(t)))) {
          leaks.push({ ...found, scope: 'file', title: d.title })
        }
        continue
      }

      const resolved = resolveTags(at.chain)
      if (accepted.some((t) => resolved.has(t))) continue
      leaks.push({ ...found, scope: at.block.kind, title: at.block.title })
    }
  })

  return leaks
}

/** One line per leak, for a guard's failure message. */
export function describeLeak(file, leak) {
  const where = leak.scope === 'file' ? `describe « ${leak.title} »` : `« ${leak.title} »`
  return `${file}:${leak.line} ${where} sends ${leak.marker} without the '${leak.tag}' tag`
}
