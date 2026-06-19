import { cleanupAllTables } from './helpers.js'

// Runs once after the whole run (vitest globalSetup teardown), removing the
// shared tables that src/setup.ts now provisions once per run. cleanupAllTables
// deletes by the `_conformance_` prefix, so it clears the shared tables (and any
// ad-hoc tables a test left behind) regardless of which worker created them.
export async function teardown(): Promise<void> {
  await cleanupAllTables()
}
