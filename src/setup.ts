import {
  createTable,
  cleanupAllTables,
  hashTableDef,
  hashNTableDef,
  compositeTableDef,
  compositeNTableDef,
  compositeBTableDef,
} from './helpers.js'

// Create shared tables once before all tests
beforeAll(async () => {
  await cleanupAllTables()
  await Promise.all([
    createTable(hashTableDef),
    createTable(hashNTableDef),
    createTable(compositeTableDef),
    createTable(compositeNTableDef),
    createTable(compositeBTableDef),
  ])
}, 120_000)

// Cleanup after all tests
afterAll(async () => {
  await cleanupAllTables()
}, 60_000)
