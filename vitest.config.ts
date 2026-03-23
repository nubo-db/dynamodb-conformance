import { defineConfig } from 'vitest/config'

export default defineConfig({
  test: {
    globals: true,
    testTimeout: 30_000,
    hookTimeout: 120_000,
    // singleFork is a CORRECTNESS requirement, not just performance.
    // Table definitions are resolved at module load time and shared by reference
    // across test files. Parallel execution would cause table contention.
    pool: 'forks',
    poolOptions: {
      forks: { singleFork: true },
    },
    setupFiles: ['./src/setup.ts'],
    reporters: ['verbose'],
    include: ['tests/**/*.test.ts'],
  },
})
