#!/usr/bin/env node

/**
 * Make a results file safe to commit to a public repo.
 *
 * Two things get removed.
 *
 * Account IDs: all 12-digit IDs in ARN patterns become '000000000000'. Covers
 * ARNs for any AWS service (TableArn, LatestStreamArn, IndexArn, backup,
 * Kinesis, KMS, S3 access points) and bare account IDs in IAM "AWS" principals
 * inside resource-policy documents.
 *
 * Machine paths: Vitest absolutises every test file against wherever the run
 * happened, so a results file records that machine's directory layout. On CI
 * that is a throwaway path; run anywhere else it is somebody's home directory.
 * Nothing reads the prefix - every consumer slices from `tests/` - so it is
 * reduced to the repo-relative form on the way in.
 *
 * Usage:
 *   node scripts/sanitize-arns.mjs results/*.json ground-truth/*.json
 *   node scripts/sanitize-arns.mjs  # defaults to results/ and ground-truth/
 */

import { readFileSync, writeFileSync, readdirSync, existsSync } from 'node:fs'
import { join } from 'node:path'

const REPLACERS = [
  { re: /(arn:aws[a-z-]*:[a-z0-9-]+:[^:]*:)\d{12}(:)/g, repl: '$1000000000000$2' },
  // Tolerates backslash-escaped quotes — policy docs land in result JSON escaped.
  { re: /(\\?"AWS\\?"\s*:\s*\\?")\d{12}(\\?")/g, repl: '$1000000000000$2' },
  // Any absolute path into the checkout becomes repo-relative.
  //
  // Deliberately not anchored to the `name` field: Vitest embeds the same
  // prefix in every stack frame inside failureMessages, and those frames reach
  // into node_modules and src as well as tests. Anchoring on the repo-relative
  // roots rather than on the prefix means it does not matter what the checkout
  // was called. Line and column numbers are kept, so a trace reads as it did.
  { re: /\/(?:[^/"\s]+\/)+(tests|src|scripts|site|node_modules)\//g, repl: '$1/' },
]

const files = process.argv.slice(2)

if (files.length === 0) {
  for (const dir of ['results', 'ground-truth']) {
    if (existsSync(dir)) {
      const entries = readdirSync(dir).filter(f => f.endsWith('.json'))
      files.push(...entries.map(f => join(dir, f)))
    }
  }
}

if (files.length === 0) {
  console.log('No JSON files found to sanitize.')
  process.exit(0)
}

let totalReplacements = 0

for (const file of files) {
  const content = readFileSync(file, 'utf8')
  let sanitized = content
  let replacements = 0
  for (const { re, repl } of REPLACERS) {
    re.lastIndex = 0
    replacements += (sanitized.match(re) || []).length
    sanitized = sanitized.replace(re, repl)
  }

  if (replacements > 0) {
    writeFileSync(file, sanitized)
    console.log(`${file}: ${replacements} replacement(s)`)
    totalReplacements += replacements
  }
}

if (totalReplacements === 0) {
  console.log('No account IDs or machine paths found — files are clean.')
} else {
  console.log(`\nTotal: ${totalReplacements} replacement(s) across ${files.length} files.`)
}
