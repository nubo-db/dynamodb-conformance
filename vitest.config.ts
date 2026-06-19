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
    // CORRECTNESS requirement, not just performance. Table definitions are
    // resolved at module load time and shared by reference across test files, so
    // the suite must run as a single, non-isolated worker: parallel or
    // module-isolated execution re-evaluates the table names per file and causes
    // table contention and ResourceNotFoundException. vitest 4 removed
    // `poolOptions` and replaced `forks.singleFork: true` with the top-level
    // `maxWorkers: 1` + `isolate: false` below (see the v4 pool-rework migration);
    // the old nested form was silently ignored, which reintroduced exactly that.
    pool: 'forks',
    maxWorkers: 1,
    isolate: false,
    setupFiles: ['./src/setup.ts'],
    // Removes the shared tables once after the whole run; src/setup.ts now
    // provisions them once per run (guarded), not once per file.
    globalSetup: ['./src/global-teardown.ts'],
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
