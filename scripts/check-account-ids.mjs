#!/usr/bin/env node

/**
 * Check committed JSON files for leaked AWS account IDs.
 * Exit code 1 if any real account IDs are found.
 *
 * Usage:
 *   node scripts/check-account-ids.mjs
 *   node scripts/check-account-ids.mjs results/*.json
 */

import { readFileSync, readdirSync, existsSync } from 'node:fs'
import { join } from 'node:path'

const PLACEHOLDER = '000000000000'
const ARN_ACCOUNT_REGEX = /arn:aws:dynamodb:[^:]+:(\d{12}):/g

const files = process.argv.slice(2)

if (files.length === 0) {
  for (const dir of ['results', 'ground-truth']) {
    if (existsSync(dir)) {
      const entries = readdirSync(dir).filter(f => f.endsWith('.json'))
      files.push(...entries.map(f => join(dir, f)))
    }
  }
}

let found = false

for (const file of files) {
  const content = readFileSync(file, 'utf8')
  let match
  while ((match = ARN_ACCOUNT_REGEX.exec(content)) !== null) {
    const accountId = match[1]
    if (accountId !== PLACEHOLDER) {
      console.error(`LEAKED: ${file} contains real account ID: ${accountId}`)
      found = true
    }
  }
}

if (found) {
  console.error('\nRun: node scripts/sanitize-arns.mjs')
  process.exit(1)
} else {
  console.log('No leaked account IDs found.')
}
