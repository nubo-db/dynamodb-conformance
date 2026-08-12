#!/usr/bin/env node

/**
 * Check committed JSON files for anything that should not be published: a real
 * AWS account ID, or a machine path from wherever the run happened.
 *
 * Exit code 1 if either is found.
 *
 * The path check exists because a results file records the directory layout of
 * whatever machine produced it. Nothing downstream reads the prefix, so the
 * invariant is simply that every recorded path is repo-relative.
 *
 * Usage:
 *   node scripts/check-account-ids.mjs
 *   node scripts/check-account-ids.mjs results/*.json
 */

import { readFileSync, readdirSync, existsSync } from 'node:fs'
import { join } from 'node:path'

const PLACEHOLDER = '000000000000'
// Account IDs appear in ARNs for any AWS service (DynamoDB, Kinesis, KMS, S3
// access points, backups) and bare inside IAM "AWS" principals in
// resource-policy documents. Cover both so the control-plane operations can't
// leak a real account ID through a non-DynamoDB ARN or a PutResourcePolicy body.
// The "AWS" principal pattern tolerates backslash-escaped quotes, because a
// resource-policy document lands in result JSON as an escaped string
// (\"AWS\":\"123456789012\").
const ACCOUNT_PATTERNS = [
  /arn:aws[a-z-]*:[a-z0-9-]+:[^:]*:(\d{12}):/g,
  /\\?"AWS\\?"\s*:\s*\\?"(\d{12})\\?"/g,
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

let found = false

// A path that is not repo-relative carries the directory the run happened in.
// Reported once per file with a sample: a single run leaks the same prefix on
// every test it executed, and in every stack frame of every failure.
//
// The second pattern is the backstop: the first only knows about paths that
// reach a `tests/` directory, so a home directory anywhere in the file is
// reported whether or not it looks like a test path.
const MACHINE_PATHS = [
  /(?:\/[^/"\s]+)+\/tests\//g,
  /\/(?:Users|home)\/[^/"\s]+/g,
]

for (const file of files) {
  const content = readFileSync(file, 'utf8')
  for (const pattern of ACCOUNT_PATTERNS) {
    pattern.lastIndex = 0
    let match
    while ((match = pattern.exec(content)) !== null) {
      const accountId = match[1]
      if (accountId !== PLACEHOLDER) {
        console.error(`LEAKED: ${file} contains real account ID: ${accountId}`)
        found = true
      }
    }
  }

  for (const pattern of MACHINE_PATHS) {
    pattern.lastIndex = 0
    const paths = [...content.matchAll(pattern)].map((m) => m[0])
    if (paths.length > 0) {
      console.error(
        `LEAKED: ${file} carries ${paths.length} path(s) from the machine that ran it, e.g. ${paths[0]}`,
      )
      found = true
      break
    }
  }
}

if (found) {
  console.error('\nRun: node scripts/sanitize-arns.mjs')
  process.exit(1)
} else {
  console.log('No leaked account IDs or machine paths found.')
}
