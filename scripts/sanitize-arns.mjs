#!/usr/bin/env node

/**
 * Sanitize AWS account IDs from JSON result files.
 *
 * Replaces all 12-digit account IDs in ARN patterns with '000000000000'.
 * Covers ARNs for any AWS service (TableArn, LatestStreamArn, IndexArn, backup,
 * Kinesis, KMS, S3 access points) and bare account IDs in IAM "AWS" principals
 * inside resource-policy documents.
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
    console.log(`${file}: ${replacements} account ID(s) sanitized`)
    totalReplacements += replacements
  }
}

if (totalReplacements === 0) {
  console.log('No account IDs found — files are clean.')
} else {
  console.log(`\nTotal: ${totalReplacements} account ID(s) sanitized across ${files.length} files.`)
}
