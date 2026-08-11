#!/usr/bin/env node

/**
 * Reduce a real-AWS lane document to what the board actually reads.
 *
 * The integrations and GSI lanes exercise live S3, Kinesis and KMS, so their
 * Vitest `failureMessages` carry bucket names, stream and role ARNs, request
 * IDs and whatever else the SDK put in the error. Those documents are committed
 * to a public repo, and sanitize-arns.mjs only rewrites the 12-digit account ID
 * inside an ARN or an IAM principal - everything around it survives.
 *
 * Nothing downstream needs the message. summarise.mjs folds these documents in
 * on test identity, and ground-truth-coverage.mjs reconciles on the same key, so
 * the fields that matter are the file name, the test title and the status.
 *
 * Usage: node scripts/strip-lane-detail.mjs results/dynamodb.gsi.json ...
 */

import { readFileSync, writeFileSync, existsSync } from 'node:fs'

let stripped = 0
for (const file of process.argv.slice(2)) {
  if (!existsSync(file)) continue
  const doc = JSON.parse(readFileSync(file, 'utf8'))
  for (const tr of doc.testResults ?? []) {
    if (tr.message) { tr.message = ''; stripped++ }
    for (const ar of tr.assertionResults ?? []) {
      if (ar.failureMessages?.length) { ar.failureMessages = []; stripped++ }
    }
  }
  writeFileSync(file, `${JSON.stringify(doc, null, 2)}\n`)
  console.error(`stripped detail from ${file}`)
}
console.error(`cleared ${stripped} message field(s)`)
