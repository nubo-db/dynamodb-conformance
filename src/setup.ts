import {
  createTable,
  cleanupAllTables,
  hashTableDef,
  hashNTableDef,
  hashBTableDef,
  gsiBTableDef,
  compositeTableDef,
  compositeNTableDef,
  compositeBTableDef,
} from './helpers.js'

// Provision the shared tables once per run.
//
// vitest 4 runs setupFiles' beforeAll for every test file (vitest 3's singleFork
// ran it once), so without this guard the suite deletes and recreates the
// shared tables ~once per file - ~100 times a run. That is slow, and on real AWS
// the churn of just-created tables surfaces as InternalServerException on the
// data operations that hit them. The single fork (maxWorkers: 1) means every
// file shares one process, so a process.env flag set after the first successful
// provision is seen by every later file. Final teardown runs once in
// src/global-teardown.ts.
beforeAll(async () => {
  if (process.env.CONFORMANCE_PROVISIONED === '1') return
  await cleanupAllTables()
  await Promise.all([
    createTable(hashTableDef),
    createTable(hashNTableDef),
    createTable(hashBTableDef),
    createTable(gsiBTableDef),
    createTable(compositeTableDef),
    createTable(compositeNTableDef),
    createTable(compositeBTableDef),
  ])
  // Set only after success, so a failed first attempt is retried by the next file.
  process.env.CONFORMANCE_PROVISIONED = '1'
}, 180_000)
