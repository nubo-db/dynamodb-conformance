import { defineConfig } from 'vitest/config'
import { TAGS } from './src/tags.js'

const resultTarget =
  process.env.CONFORMANCE_TARGET ??
  (process.env.DYNAMODB_ENDPOINT ? 'local' : 'dynamodb')

export default defineConfig({
  test: {
    globals: true,
    testTimeout: 30_000,
    hookTimeout: 180_000,
    // Bounded retry, opt-in via CONFORMANCE_RETRY (set only on the real-AWS
    // gating job). Off everywhere else: emulators are deterministic and instant,
    // so retrying them would only mask real emulator bugs as flakes. Genuine AWS
    // drift fails deterministically and survives the retry (stays red); a
    // transient throttle or slow-settling read passes on the re-run. This covers
    // test bodies only - vitest does not retry beforeAll, so shared-table
    // provisioning flakes stay owned by hookTimeout, not this. Re-runs reuse the
    // singleFork worker against the shared tables, which is safe because each
    // test cleans up after itself; don't raise the bound blindly.
    retry: Number(process.env.CONFORMANCE_RETRY ?? 0),
    // singleFork is a CORRECTNESS requirement, not just performance.
    // Table definitions are resolved at module load time and shared by reference
    // across test files. Parallel execution would cause table contention.
    pool: 'forks',
    poolOptions: {
      forks: { singleFork: true },
    },
    setupFiles: ['./src/setup.ts'],
    // Canonical feature/capability tag vocabulary, declared in src/tags.ts.
    // strictTags stays on (the default), so applying a tag not declared here
    // fails collection rather than silently mis-filtering. Tags are an axis
    // orthogonal to the tier directories; see src/tags.ts.
    tags: [...TAGS],
    reporters: ['verbose', 'json'],
    outputFile: `results/${resultTarget}.json`,
    include: ['tests/**/*.test.ts'],
  },
})
