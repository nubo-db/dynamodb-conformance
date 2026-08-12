import { defineConfig } from 'vitest/config'
import { TAGS } from './src/tags.js'
import { resultTargetFrom } from './scripts/lib/result-target.mjs'

// Naming a target is what opts a run into writing a published results file;
// an unconfigured run goes to the gitignored scratch slug. See
// scripts/lib/result-target.mjs for why the default is not the ground truth.
const resultTarget = resultTargetFrom()

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
    // The failure message IS the evidence, so it must survive serialisation.
    // Chai truncates a diff at 40 characters by default, which turns a real
    // answer into "1 validation error detected: Value at…". Every failure the
    // sweep records is that shape, so the weekly cross-region run could prove
    // that four regions disagreed on a behaviour while recording nothing about
    // what any of them said - and a split candidate cannot be admitted to the
    // registry from a verdict alone, only from the answer. That is the wrong
    // way round: a test recorded its answer only once it was already a split.
    //
    // Bounded rather than unlimited. 1000 clears the longest real validation
    // message by a wide margin (the two-error aggregation form is under 300),
    // while still capping the pathological ones - a rejected BatchWriteItem
    // echoes all 26 request items back and runs to several kilobytes.
    chaiConfig: { truncateThreshold: 1000 },
    reporters: ['verbose', 'json'],
    outputFile: `results/${resultTarget}.json`,
    include: ['tests/**/*.test.ts'],
  },
})
