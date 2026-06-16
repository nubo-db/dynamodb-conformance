import { defineConfig } from 'vitest/config'

// Tooling tests (scripts/**/*.test.mjs) are pure-logic unit tests for the helper
// scripts. They must not load the conformance setup - src/setup.ts provisions
// real AWS tables in a global beforeAll - nor the tests/ suite, so they run
// under their own config: no setupFiles, a scripts-only include, and no target.
export default defineConfig({
  test: {
    globals: true,
    include: ['scripts/**/*.test.mjs'],
  },
})
