#!/usr/bin/env node

/**
 * Sanitize AWS account IDs from JSON result files.
 *
 * Replaces all 12-digit account IDs in ARN patterns with '000000000000'.
 * Covers: TableArn, LatestStreamArn, IndexArn, and any other ARN field.
 *
 * Usage:
 *   node scripts/sanitize-arns.mjs results/*.json ground-truth/*.json
 *   node scripts/sanitize-arns.mjs  # defaults to results/ and ground-truth/
 */

import { readFileSync, writeFileSync, readdirSync, existsSync } from 'node:fs'
import { join } from 'node:path'

const ARN_ACCOUNT_REGEX = /(arn:aws:dynamodb:[^:]+:)\d{12}(:)/g
const REPLACEMENT = '$1000000000000$2'

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
  const sanitized = content.replace(ARN_ACCOUNT_REGEX, REPLACEMENT)
  const replacements = (content.match(ARN_ACCOUNT_REGEX) || []).length

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
