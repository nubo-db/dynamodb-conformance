import { describe, it, expect } from 'vitest'
import { createHash } from 'node:crypto'
import { mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { buildBadge, writeBadges } from './badges.mjs'
import { testIdentities } from './ground-truth-coverage.mjs'
import { GROUND_TRUTH_SLUG, axesOf, isTargetResultFile, loadScoringContext, scoreTarget, verdictsForRegion } from './lib/score.mjs'
import { classifyResults } from './lib/classify.mjs'
import { splitFor } from './lib/registry.mjs'
import { BASELINE_LABEL, gradeOf } from './lib/grade.mjs'
import { suiteIdentities, suiteSizeOf } from './suite-manifest.mjs'
import {
  DISPLAY,
  REPO,
  SUMMARY_PATH,
  SUMMARY_SCHEMA_VERSION,
  assertOneDenominator,
  buildSummary,
  display,
  label,
  mergeLanes,
  readTargets,
  regionStanding,
  renderTable,
  repoUrl,
  tableCaption,
  tableRows,
  writeSummaryFile,
} from './summarise.mjs'

const DAY = '2026-07-06'
const health = (regions) => ({ regions })
const entry = (consecutiveUnresolved, lastResolved = DAY) => ({
  lastResolved,
  consecutiveUnresolved,
})

// Two healthy regions, one admitted split between them. The committed
// assertion encodes us-east-1's answer (pinned), so a target passing it
// matches us-east-1 and not eu-west-2.
const HEALTHY = health({ 'eu-west-2': entry(0), 'us-east-1': entry(0) })
const REGISTRY = {
  splits: [
    {
      id: 'example-split',
      test: { file: 'tests/tier3/split.test.ts', fullName: 'suite splits' },
      pinned: 'us-east-1',
      regions: {
        'us-east-1': { outcome: 'accepted' },
        'eu-west-2': { outcome: 'rejected' },
      },
    },
  ],
}

// Minimal Vitest-shaped result: { '<file>': [['fullName', 'status'], ...] }.
function rawDoc(files, startTime = Date.UTC(2026, 6, 6)) {
  return {
    startTime,
    testResults: Object.entries(files).map(([name, assertions]) => ({
      name,
      assertionResults: assertions.map(([fullName, status]) => ({
        title: fullName,
        fullName,
        status,
        meta: {},
      })),
    })),
  }
}

const target = (slug, raw, overrides = {}) => ({
  slug,
  raw,
  sidecar: null,
  version: '1.0.0',
  runDate: DAY,
  ...overrides,
})

// Two region-invariant passes plus the split test with the given status.
const suiteDoc = (splitStatus) =>
  rawDoc({
    '/repo/tests/tier1/a.test.ts': [
      ['a', 'passed'],
      ['b', 'passed'],
    ],
    '/repo/tests/tier3/split.test.ts': [['suite splits', splitStatus]],
  })

describe('regionStanding', () => {
  it('keeps healthy regions observed with nothing unresolved or dropped', () => {
    expect(regionStanding(HEALTHY)).toEqual({
      observed: ['eu-west-2', 'us-east-1'],
      unresolved: [],
      dropped: [],
    })
  })

  it('a region that missed one sweep stays observed but is named unresolved', () => {
    const standing = regionStanding(health({ 'eu-west-2': entry(0), 'us-east-1': entry(1) }))
    expect(standing.observed).toEqual(['eu-west-2', 'us-east-1'])
    expect(standing.unresolved).toEqual(['us-east-1'])
    expect(standing.dropped).toEqual([])
  })

  it('two consecutive misses drop a region out of the observed set', () => {
    const standing = regionStanding(health({ 'eu-west-2': entry(0), 'us-east-1': entry(2) }))
    expect(standing.observed).toEqual(['eu-west-2'])
    expect(standing.dropped).toEqual(['us-east-1'])
  })

  it('a region that has never resolved is not observed', () => {
    const standing = regionStanding(
      health({ 'eu-west-2': entry(0), 'ap-southeast-2': entry(0, null) }),
    )
    expect(standing.observed).toEqual(['eu-west-2'])
    expect(standing.dropped).toEqual(['ap-southeast-2'])
  })

  it('every region dropping at once is loud, not a silently empty set', () => {
    expect(() => regionStanding(health({ 'eu-west-2': entry(2) }))).toThrow(
      /no observed regions/,
    )
  })
})

describe('buildSummary', () => {
  it('scores each target in every observed region and headlines the max (per-region columns)', () => {
    const summary = buildSummary([target('alpha', suiteDoc('passed'))], {
      registry: REGISTRY,
      health: HEALTHY,
    })
    const t = summary.targets.alpha

    // One entry per observed region, and the headline is their max - here
    // us-east-1, whose recorded answer the passing committed assertion encodes.
    expect(Object.keys(t.regions)).toEqual(['eu-west-2', 'us-east-1'])
    expect(t.regions['us-east-1'].rate).toBe(100)
    expect(t.regions['eu-west-2'].rate).toBe(66.7)
    expect(t.headline).toEqual({ region: 'us-east-1', rate: 100 })
    expect(summary.schemaVersion).toBe(SUMMARY_SCHEMA_VERSION)
  })

  it('carries the ground-truth run date and pins its rate at 100 (self-agreement)', () => {
    const summary = buildSummary(
      [target(GROUND_TRUTH_SLUG, suiteDoc('passed')), target('alpha', suiteDoc('passed'))],
      { registry: REGISTRY, health: HEALTHY },
    )
    expect(summary.groundTruth).toMatchObject({
      slug: GROUND_TRUTH_SLUG,
      rate: 100,
      runDate: DAY,
    })
    // The ground truth is never listed as a target of itself.
    expect(Object.keys(summary.targets)).toEqual(['alpha'])
  })

  it('an unresolved region appears explicitly and is still scored against (AE6)', () => {
    const summary = buildSummary([target('alpha', suiteDoc('passed'))], {
      registry: REGISTRY,
      health: health({ 'eu-west-2': entry(0), 'us-east-1': entry(1) }),
    })
    expect(summary.regions.unresolved).toEqual(['us-east-1'])
    // Its registry rows are retained: the target's headline still draws on it.
    expect(summary.targets.alpha.headline.region).toBe('us-east-1')
    expect(renderTable(summary)).toContain('`us-east-1` did not resolve the latest sweep')
  })

  it('a dropped region is excluded from the headline max and labelled dropped (AE5)', () => {
    const summary = buildSummary([target('alpha', suiteDoc('passed'))], {
      registry: REGISTRY,
      health: health({ 'eu-west-2': entry(0), 'us-east-1': entry(2) }),
    })
    expect(summary.regions.dropped).toEqual(['us-east-1'])
    // us-east-1 would give this target 100%, but a dropped region cannot
    // contribute: the headline falls back to the best remaining region.
    expect(summary.targets.alpha.headline).toEqual({ region: 'eu-west-2', rate: 66.7 })
    expect(summary.targets.alpha.regions['us-east-1']).toBeUndefined()
    expect(renderTable(summary)).toContain(
      '`us-east-1` has been dropped from the observed set',
    )
  })

  it('a run-level indeterminate empties the rate rather than failing the target', () => {
    const sidecar = { runLevel: [{ reason: 'table-active-timeout', phase: 'provisioning' }] }
    const summary = buildSummary([target('alpha', suiteDoc('failed'), { sidecar })], {
      registry: REGISTRY,
      health: HEALTHY,
    })
    const t = summary.targets.alpha
    expect(t.headline.rate).toBeNull()
    expect(t.regions['eu-west-2']).toMatchObject({ rate: null, indeterminate: 3, failed: 0 })
  })

  it('skips files that are not a target run (e.g. the tag manifest)', () => {
    const summary = buildSummary(
      [target('tag-manifest', { schema: 1, describes: {} }), target('alpha', suiteDoc('passed'))],
      { registry: REGISTRY, health: HEALTHY },
    )
    expect(Object.keys(summary.targets)).toEqual(['alpha'])
  })

  // The evidence the site build checks the A+ premise from. Publish the names
  // and it can check identity; publish nothing and a count is all it has.
  describe('the failing test identities behind a zero-divergence row', () => {
    it('names the tests a zero-divergence target fails outside its headline region', () => {
      // Passes the split in us-east-1 (which accepts) and so fails it in
      // eu-west-2 (which rejects): zero divergence in its headline, one fail
      // elsewhere, and that fail is the registry's split.
      const summary = buildSummary([target('alpha', suiteDoc('passed'))], {
        registry: REGISTRY,
        health: HEALTHY,
      })
      const t = summary.targets.alpha
      expect(t.headline.region).toBe('us-east-1')
      expect(t.regionFailures).toEqual({
        'eu-west-2': ['tests/tier3/split.test.ts::suite splits'],
      })
    })

    it('names every fail the row declares, so the build can check the count adds up', () => {
      const summary = buildSummary([target('alpha', suiteDoc('passed'))], {
        registry: REGISTRY,
        health: HEALTHY,
      })
      const t = summary.targets.alpha
      for (const [region, names] of Object.entries(t.regionFailures)) {
        expect(names.length, `${region} names as many tests as it declares failed`).toBe(
          t.regions[region].failed,
        )
      }
    })

    it('names each fail by file and title, the identity splitFor matches on', () => {
      // A title is unique only within its file, so a bare name would let a
      // same-named test in another file satisfy the build's split check.
      const summary = buildSummary([target('alpha', suiteDoc('passed'))], {
        registry: REGISTRY,
        health: HEALTHY,
      })
      for (const names of Object.values(summary.targets.alpha.regionFailures)) {
        for (const id of names) expect(id).toMatch(/^tests\/.+\.test\.ts::.+/)
      }
    })

    it('omits the field for a target that diverges in its headline region', () => {
      // The A+ claim is not about this row, so publishing the evidence for it
      // would grow the artefact for every target on the board to no purpose.
      const summary = buildSummary([target('alpha', suiteDoc('failed'))], {
        registry: REGISTRY,
        health: HEALTHY,
      })
      expect(summary.targets.alpha.regionFailures).toBeUndefined()
    })

    it('omits regions where a zero-divergence target fails nothing', () => {
      const summary = buildSummary([target('alpha', suiteDoc('passed'))], {
        registry: REGISTRY,
        health: HEALTHY,
      })
      expect(Object.keys(summary.targets.alpha.regionFailures)).not.toContain('us-east-1')
    })

    it('the committed board publishes evidence for every zero-divergence row', () => {
      // A change that stopped emitting the names would leave the build check
      // with nothing to check. It reports that rather than passing, but this
      // fails first, against the tree, while someone is working.
      const context = loadScoringContext()
      const summary = buildSummary(
        readTargets(
          readdirSync('results')
            .filter(isTargetResultFile)
            .map((f) => join('results', f)),
        ),
        context,
      )
      let zeroDivergence = 0
      for (const [slug, t] of Object.entries(summary.targets)) {
        const headline = t.regions[t.headline.region]
        if (!headline || headline.count === 0 || axesOf(headline).divergence !== 0) continue
        zeroDivergence++
        const failing = Object.entries(t.regions).filter(([, r]) => r.failed > 0)
        if (failing.length === 0) continue
        expect(t.regionFailures, `${slug} fails somewhere and published no identities`).toBeTruthy()
        for (const [region, r] of failing) {
          expect(t.regionFailures[region]?.length, `${slug}/${region}`).toBe(r.failed)
        }
      }
      expect(
        zeroDivergence,
        'no target on the board holds zero divergence, so this asserted nothing',
      ).toBeGreaterThan(0)
    })
  })
})

// ── The real-AWS lanes behind the baseline row ──────────────────────────────

describe('the ground truth as three lanes', () => {
  const laneDir = () => mkdtempSync(join(tmpdir(), 'lanes-'))

  it('a lane that shipped an indeterminate sidecar is not merged', () => {
    // Otherwise the lane's failures read as observed answers, `unobserved`
    // empties, the row derives, and the board publishes real DynamoDB
    // diverging from itself.
    const dir = laneDir()
    const file = join(dir, 'dynamodb.json')
    writeFileSync(file, JSON.stringify(suiteDoc('passed')))
    writeFileSync(join(dir, 'dynamodb.gsi.json'), JSON.stringify(suiteDoc('passed')))
    writeFileSync(
      join(dir, 'dynamodb.gsi.indeterminate.json'),
      JSON.stringify({ runLevel: [{ reason: 'table-active-timeout', phase: 'provisioning' }] }),
    )
    const [target] = readTargets([file])
    expect(target.missingLanes).toContain('gsi')
  })

  it('an unreadable lane degrades rather than aborting the regeneration', () => {
    const dir = laneDir()
    const file = join(dir, 'dynamodb.json')
    writeFileSync(file, JSON.stringify(suiteDoc('passed')))
    writeFileSync(join(dir, 'dynamodb.gsi.json'), '{"testResults": [truncated')
    expect(() => readTargets([file])).not.toThrow()
    expect(readTargets([file])[0].missingLanes).toContain('gsi')
  })

  // Real AWS is observed in three runs: the gating job plus the two slower
  // lanes. The fixture is split the same way, and each lane's paths carry a
  // different absolute prefix because each lane runs in its own CI job.
  const LANE_TESTS = {
    gating: { 'tests/tier1/a.test.ts': [['a', 'passed'], ['b', 'passed']] },
    integrations: {
      'tests/tier2/export/exportImport.test.ts': [['export > writes to S3', 'passed']],
    },
    gsi: {
      'tests/tier2/updateTable/gsi.test.ts': [['updateTable > adds a GSI', 'passed']],
    },
  }
  const under = (prefix, files) =>
    Object.fromEntries(Object.entries(files).map(([f, a]) => [`${prefix}/${f}`, a]))

  const gating = rawDoc(under('/gate', LANE_TESTS.gating), Date.UTC(2026, 6, 6))
  const integrations = rawDoc(under('/int', LANE_TESTS.integrations), Date.UTC(2026, 6, 7))
  const gsi = rawDoc(under('/gsi', LANE_TESTS.gsi), Date.UTC(2026, 6, 8))
  // Every test the fixture's suite contains, standing in for the manifest.
  const whole = rawDoc(
    under('/repo', { ...LANE_TESTS.gating, ...LANE_TESTS.integrations, ...LANE_TESTS.gsi }),
  )

  // Lay out a results directory and read it back the way the CLI does.
  const readDir = (files) => {
    const dir = mkdtempSync(join(tmpdir(), 'lanes-'))
    for (const [name, doc] of Object.entries(files)) {
      writeFileSync(join(dir, name), JSON.stringify(doc))
    }
    return readTargets(readdirSync(dir).map((f) => join(dir, f)))
  }
  // `whole` is this fixture's suite manifest: the four tests the lanes divide
  // between them.
  const summaryOf = (targets) =>
    buildSummary(targets, { registry: REGISTRY, health: HEALTHY, suite: testIdentities(whole) })
  const baselineRow = (summary) => tableRows(summary).find((r) => r.slug === GROUND_TRUTH_SLUG)

  it('unions the lanes into one document, with no test counted twice', () => {
    // The gating document passed in twice over: a lane that repeats what the
    // gate already ran must add nothing.
    const merged = mergeLanes(gating, [integrations, gsi, gating])
    expect(testIdentities(merged)).toEqual(testIdentities(whole))
    expect(merged.testResults).toHaveLength(3)
    expect(merged.testResults.flatMap((tr) => tr.assertionResults)).toHaveLength(4)
  })

  it('lets the gating run keep the answer when a lane restates it', () => {
    const restated = rawDoc(under('/int', { 'tests/tier1/a.test.ts': [['a', 'failed']] }))
    const merged = mergeLanes(gating, [restated])
    const verdicts = merged.testResults.flatMap((tr) => tr.assertionResults)
    expect(verdicts.filter((ar) => ar.fullName === 'a')).toEqual([
      expect.objectContaining({ status: 'passed' }),
    ])
  })

  it('leaves the document and the published row untouched when no lane is present', () => {
    const targets = readDir({ 'dynamodb.json': gating, 'alpha.json': whole })
    expect(targets.find((t) => t.slug === GROUND_TRUTH_SLUG).raw).toEqual(gating)

    // The pinned row: the whole suite at 100%, which is what the lanes not
    // being merged in has always published.
    expect(baselineRow(summaryOf(targets))).toMatchObject({
      total: '100.0%',
      divergence: '0.0%',
      coverage: '100.0%',
      tier1: '0.0%',
      passed: 4,
      failed: 0,
      skipped: 0,
      count: 4,
    })
  })

  it('stays pinned when the lanes fall short of the suite, and says which are missing', () => {
    const gt = summaryOf(
      readDir({ 'dynamodb.json': gating, 'dynamodb.gsi.json': gsi, 'alpha.json': whole }),
    ).groundTruth

    // Three of the suite's four tests were observed, so a row derived from
    // them would span less than the figures beneath it are divided by. The pin
    // is honest here; a narrower measurement would not be.
    expect(gt).toMatchObject({
      derived: false,
      testsObserved: 3,
      suiteSize: 4,
      missingLanes: ['integrations'],
      counts: null,
    })
  })

  it('derives the row from the merge once it spans the suite', () => {
    const summary = summaryOf(
      readDir({
        'dynamodb.json': gating,
        'dynamodb.gsi.json': gsi,
        'dynamodb.integrations.json': integrations,
        'alpha.json': whole,
      }),
    )
    expect(summary.groundTruth).toMatchObject({
      derived: true,
      testsObserved: 4,
      suiteSize: 4,
      missingLanes: [],
      rate: 100,
      counts: { passed: 4, failed: 0, skipped: 0, indeterminate: 0, count: 4 },
    })
    // Measured, not pinned: the row's counts are the merged document's.
    expect(baselineRow(summary)).toMatchObject({ passed: 4, count: 4, total: '100.0%' })
    // And a lane document is evidence, never a row of its own.
    expect(Object.keys(summary.targets)).toEqual(['alpha'])
  })

  it('dates each lane it merged, so three captures never read as one', () => {
    const summary = summaryOf(
      readDir({
        'dynamodb.json': gating,
        'dynamodb.gsi.json': gsi,
        'dynamodb.integrations.json': integrations,
        'alpha.json': whole,
      }),
    )
    expect(summary.groundTruth.lanes).toEqual([
      { name: 'gating', runDate: '2026-07-06', tests: 2 },
      { name: 'integrations', runDate: '2026-07-07', tests: 1 },
      { name: 'gsi', runDate: '2026-07-08', tests: 1 },
    ])
  })
})

describe('tableRows / renderTable', () => {
  const docs = {
    [GROUND_TRUTH_SLUG]: suiteDoc('passed'),
    alpha: suiteDoc('passed'),
    beta: suiteDoc('failed'),
    empty: rawDoc({ '/repo/tests/tier1/a.test.ts': [] }),
  }
  const summary = buildSummary(
    Object.entries(docs).map(([slug, doc]) => target(slug, doc)),
    { registry: REGISTRY, health: HEALTHY, suite: testIdentities(suiteDoc('passed')) },
  )
  const rows = tableRows(summary)

  it('renders the ground-truth row first, ungraded, at an earned 100% across all regions', () => {
    // 100% by self-agreement: each real region scores 100% against its own
    // recorded behaviour, so the max over any observed set is 100%. Its figures
    // publish and its grade does not: a letter measures distance from real
    // DynamoDB, so the yardstick has none to wear. The table had kept grading it
    // A+ after the site moved it out of the board, which put a letter nothing
    // could beat on the first row a reader meets.
    expect(rows[0]).toMatchObject({
      target: label(GROUND_TRUTH_SLUG),
      grade: BASELINE_LABEL,
      total: '100.0%',
      divergence: '0.0%',
      coverage: '100.0%',
      failed: 0,
      passed: 3, // the suite size, from the manifest
    })
  })

  it('sorts targets by headline rate, dateless "-" rates last', () => {
    expect(rows.map((r) => r.target)).toEqual([
      label(GROUND_TRUTH_SLUG),
      'alpha',
      'beta',
      'empty',
    ])
    // A target that implemented nothing has no divergence to grade, so its
    // grade is the same "-" as its figures rather than an invented letter.
    expect(rows.at(-1)).toMatchObject({ total: '-', divergence: '-', grade: '-' })
  })

  it('publishes the best-matching region\'s divergence, with the cohort kept for the drilldown', () => {
    // Regional variation is not published beside the figure. A count of
    // matching regions is not a quality measure and was read as one: a target
    // equally wrong in every region counted higher than one perfect in a few
    // and near-perfect in the rest - even where the first diverges less in its
    // worst region than the second does in its best. What the README publishes
    // is the count - "N of M observed" - with the naming label kept alongside
    // for surfaces that name the regions.
    //
    // alpha matches us-east-1 alone (it beats the eu-west-2 baseline).
    const alpha = rows.find((r) => r.target === 'alpha')
    expect(alpha).toMatchObject({
      grade: 'A+',
      total: '100.0%',
      divergence: '0.0%',
      coverage: '100.0%',
      cohort: '1 of 2',
      cohortLabel: 'us-east-1',
      passed: 3,
      failed: 0,
    })
    // beta fails the split test everywhere (a fail without an observation is
    // evidence of nothing beyond "not the pinned answer"), so it ties across
    // every region: a full count, earned by being indistinguishable rather
    // than by being right.
    // Diverging on a third of the suite lands in the D band however much of
    // it the target covers - the grade restates divergence, coverage can only
    // cap it further.
    const beta = rows.find((r) => r.target === 'beta')
    expect(beta).toMatchObject({
      grade: 'D',
      total: '66.7%',
      divergence: '33.3%',
      cohort: '2 of 2',
      cohortLabel: 'all regions',
      passed: 2,
      failed: 1,
    })
  })

  it('orders by divergence ascending, so a narrow but correct target is not ranked below a broad wrong one', () => {
    // beta diverges on a third of the suite; alpha on none of it. Coverage
    // breaks ties, and neither figure is folded into the other.
    const order = rows.map((r) => r.target)
    expect(order.indexOf('alpha')).toBeLessThan(order.indexOf('beta'))
  })

  it('names the observed regions in the caption', () => {
    expect(tableCaption(summary.regions)).toContain('`eu-west-2`, `us-east-1`')
  })

  it('badge and table cannot disagree: every grade equals the badge letter', () => {
    // Both surfaces are rendered from the one shared headline (scoreTarget),
    // the shared axes (axesOf) and the shared grading (gradeOf), so the
    // invariant is structural; this pins it against a future caller
    // reintroducing its own scoring.
    const context = { registry: REGISTRY, observed: summary.regions.observed }
    for (const slug of Object.keys(summary.targets)) {
      const badge = buildBadge(slug, docs[slug], context)
      const row = rows.find((r) => r.target === label(slug))
      expect(row.grade).toBe(badge === null ? '-' : badge.message)
    }
  })

  it('a badge is deleted when its target stops being gradeable', () => {
    // Third parties embed these in their own READMEs, so a badge left behind
    // after its results file goes serves a letter about someone else from a
    // URL they do not control. The freshness test spots the drift; only the
    // delete fixes it.
    const dir = mkdtempSync(join(tmpdir(), 'badges-'))
    const context = { registry: REGISTRY, observed: ['eu-west-2', 'us-east-1'] }
    writeFileSync(join(dir, 'alpha.json'), JSON.stringify(suiteDoc('passed')))
    writeFileSync(join(dir, 'departed.badge.json'), '{"message":"A"}\n')

    const { written, pruned } = writeBadges(dir, context)

    expect(written).toBe(1)
    expect(pruned).toBe(1)
    expect(readdirSync(dir).sort()).toEqual(['alpha.badge.json', 'alpha.json'])
  })

  it('leaves the badge of a target that is still gradeable alone', () => {
    const dir = mkdtempSync(join(tmpdir(), 'badges-'))
    const context = { registry: REGISTRY, observed: ['eu-west-2', 'us-east-1'] }
    writeFileSync(join(dir, 'alpha.json'), JSON.stringify(suiteDoc('passed')))
    writeFileSync(join(dir, 'alpha.badge.json'), '{"message":"stale"}\n')

    const { pruned } = writeBadges(dir, context)

    expect(pruned).toBe(0)
    expect(JSON.parse(readFileSync(join(dir, 'alpha.badge.json'), 'utf8')).message).not.toBe('stale')
  })
})

describe('tableRows tie-break', () => {
  // A target scoring identically to the engine it is a variant of must sort
  // below it, never above. The two Dynoxide rows are the live case: a partial
  // wasm preview can tie native on the surface it implements, and the table
  // must not read as the preview outranking the engine.
  const tied = (rate) => ({
    headline: { region: 'eu-west-2', rate },
    regions: {
      'eu-west-2': {
        rate,
        passed: 785,
        failed: 0,
        skipped: 10,
        indeterminate: 0,
        count: 795,
        tiers: {
          tier1: { p: 1, f: 0, s: 0, i: 0 },
          tier2: { p: 1, f: 0, s: 0, i: 0 },
          tier3: { p: 1, f: 0, s: 0, i: 0 },
        },
      },
    },
    version: '-',
    runDate: '2026-07-24',
  })

  // The tier columns sit beside a headline that is divergence and a sort that
  // runs on divergence. Left as correctness they read in the opposite
  // direction, so a target improving down the Divergence column climbs up the
  // tier ones.
  it('reports each tier as divergence over the whole tier, not correctness', () => {
    const summary = {
      groundTruth: { slug: GROUND_TRUTH_SLUG, runDate: '-' },
      targets: {
        alpha: {
          headline: { region: 'eu-west-2', rate: 90 },
          regions: {
            'eu-west-2': {
              rate: 90,
              passed: 9,
              failed: 1,
              skipped: 2,
              indeterminate: 0,
              count: 12,
              tiers: {
                // 1 of 4 fails: 25% divergence, where correctness over the two
                // attempted would have been 50%.
                tier1: { p: 1, f: 1, s: 2, i: 0 },
                tier2: { p: 4, f: 0, s: 0, i: 0 },
                tier3: { p: 4, f: 0, s: 0, i: 0 },
              },
            },
          },
          version: '-',
          runDate: '2026-07-29',
        },
      },
    }
    const alpha = tableRows(summary).find((r) => r.slug === 'alpha')
    expect(alpha.tier1).toBe('25.0%')
    expect(alpha.tier2).toBe('0.0%')
    expect(alpha.tier3).toBe('0.0%')
  })

  // The baseline diverges from itself nowhere, so its tier columns read 0.0%
  // rather than the 100% they read while the columns were correctness.
  it('renders the ground truth row as diverging nowhere in every tier', () => {
    const summary = {
      groundTruth: { slug: GROUND_TRUTH_SLUG, runDate: '2026-07-29' },
      targets: {},
    }
    const gt = tableRows(summary).find((r) => r.slug === GROUND_TRUTH_SLUG)
    expect([gt.tier1, gt.tier2, gt.tier3]).toEqual(['0.0%', '0.0%', '0.0%'])
    expect(gt.divergence).toBe('0.0%')
  })

  it('nests a variant under its project instead of seating it as a rival', () => {
    const summary = {
      groundTruth: { slug: GROUND_TRUTH_SLUG, runDate: '-' },
      // wasm listed first, so a build that competed for its own place would
      // take the higher slot.
      targets: { 'dynoxide-wasm': tied(100), dynoxide: tied(100) },
    }
    const rows = tableRows(summary)
    // One row for the project, not two. A reader chooses between projects; the
    // build follows from where their code runs.
    expect(rows.map((r) => r.slug)).toEqual([GROUND_TRUTH_SLUG, 'dynoxide'])
    const dynoxide = rows.find((r) => r.slug === 'dynoxide')
    expect(dynoxide.variants.map((v) => v.slug)).toEqual(['dynoxide-wasm'])
  })

  it('keeps a variant scored, not merely mentioned', () => {
    const summary = {
      groundTruth: { slug: GROUND_TRUTH_SLUG, runDate: '-' },
      targets: { dynoxide: tied(100), 'dynoxide-wasm': tied(100) },
    }
    // Nesting must not cost a variant its own figures: a build that implements
    // less has to say so where it is read.
    const wasm = tableRows(summary).find((r) => r.slug === 'dynoxide').variants[0]
    expect(wasm.divergence).toBe('0.0%')
    expect(wasm.coverage).toBe('98.7%')
  })
})

describe('renderTable variant nesting', () => {
  // A build of a project is rendered beneath it, labelled by what makes it
  // distinct. Markdown has no nested tables, so the indent carries the
  // relationship - and it is derived from declared metadata rather than from a
  // bracket in the display name, which is what used to stand in for it.
  const one = (rate) => ({
    headline: { region: 'eu-west-2', rate },
    regions: {
      'eu-west-2': {
        rate,
        passed: 785,
        failed: 0,
        skipped: 213,
        indeterminate: 0,
        count: 998,
        tiers: {
          tier1: { p: 1, f: 0, s: 0, i: 0 },
          tier2: { p: 1, f: 0, s: 0, i: 0 },
          tier3: { p: 1, f: 0, s: 0, i: 0 },
        },
      },
    },
    version: '-',
    runDate: '2026-07-24',
  })

  it('indents a variant under its project and labels it by configuration', () => {
    const summary = {
      groundTruth: { slug: GROUND_TRUTH_SLUG, runDate: '-' },
      regions: { observed: ['eu-west-2'], unresolved: [], dropped: [] },
      targets: { 'dynoxide-wasm': one(100), dynoxide: one(96.3) },
    }
    const table = renderTable(summary)
    // Named by what distinguishes it, not by repeating the project name.
    expect(table).toContain('| ↳ WebAssembly / OPFS |')
    expect(table).not.toMatch(/\[Dynoxide \(wasm\)\]/)
    // The parent names the configuration its own figures were measured on, so
    // the row does not go ambiguous the moment a second one ships.
    expect(table).toMatch(/\[Dynoxide\]\([^)]+\) · native/)
    // Directly beneath its project, not sorted away from it.
    const lines = table.split('\n').filter((l) => l.startsWith('|'))
    const parent = lines.findIndex((l) => l.includes('[Dynoxide]'))
    expect(lines[parent + 1]).toContain('↳ WebAssembly / OPFS')
  })

  it('adds no footnote when no variant row is present', () => {
    const summary = {
      groundTruth: { slug: GROUND_TRUTH_SLUG, runDate: '-' },
      regions: { observed: ['eu-west-2'], unresolved: [], dropped: [] },
      targets: { dynoxide: one(96.3) },
    }
    const table = renderTable(summary)
    expect(table).not.toContain('†')
    expect(table).not.toContain('preview')
  })
})

// ── The committed artefacts: freshness, no-drift, and the shape contract ────

describe('committed results pipeline', () => {
  const context = loadScoringContext()
  const files = readdirSync('results')
    .filter(isTargetResultFile)
    .map((f) => join('results', f))
  const targets = readTargets(files)
  const fresh = buildSummary(targets, context)

  it('results/summary.json matches a fresh build (and a re-run is deterministic)', () => {
    const committed = JSON.parse(readFileSync(SUMMARY_PATH, 'utf8'))
    expect(committed, `${SUMMARY_PATH} is stale — run \`node scripts/summarise.mjs --write\``).toEqual(
      fresh,
    )
  })

  it('badge letter equals the summary headline grade for every target (the no-drift invariant)', () => {
    for (const [slug, t] of Object.entries(fresh.targets)) {
      const badge = JSON.parse(readFileSync(join('results', `${slug}.badge.json`), 'utf8'))
      const { divergence, coverage } = axesOf(t.regions[t.headline.region])
      const expected = gradeOf(divergence, coverage).letter
      expect(badge.message, `${slug} badge disagrees with the summary headline`).toBe(expected)
    }
    // And the table's Total column is rendered from the same headline.
    // Variants nest, so flatten before asserting: the invariant covers every
    // scored target, including the ones that are not their own row.
    const rows = tableRows(fresh).flatMap((r) => [r, ...(r.variants ?? [])])
    for (const [slug, t] of Object.entries(fresh.targets)) {
      const row = rows.find((r) => r.slug === slug)
      expect(row.total).toBe(t.headline.rate === null ? '-' : `${t.headline.rate.toFixed(1)}%`)
    }
  })

  it('every published results file covers the full suite - one denominator under every figure', () => {
    // Divergence, coverage, the cap and the A+ tripwire all divide by the
    // whole-suite count, and "the denominator never moves" is a published
    // claim. A partial run (a file-level crash, or a filtered capture) would
    // shrink one target's denominator, inflate its coverage past the cap and
    // sail through the tripwire vacuously - so full-suite coverage is
    // asserted, not assumed.
    //
    // Carried rows are in scope without a fixture of their own. Every file in
    // results/ is measured against the suite manifest, so a target that missed
    // a run while the suite grew keeps its old count and fails here, which is
    // the stale-denominator case: it would otherwise publish a letter earned
    // over a smaller suite and outrank a re-tested peer.
    //
    // This calls the publishing gate rather than restating it, so the assertion
    // and the thing that runs before every write cannot drift apart.
    expect(() => assertOneDenominator(fresh)).not.toThrow()
    expect(suiteSizeOf()).toBe(
      Math.max(
        0,
        ...Object.values(fresh.targets).map((t) => t.regions[t.headline.region]?.count ?? 0),
      ),
    )
  })

  it('a zero-divergence headline stays honest across regions (the A+ tripwire)', () => {
    // Two facts hold today and the top grade leans on both, so they are
    // asserted rather than assumed.
    //
    // First, the identity: a target with zero fails in its headline region
    // may fail elsewhere only on the registry's confirmed splits - and that
    // is checked by name, not by count. A count match would hold just as
    // well for a target failing three unrelated tests while passing the
    // three splits; asserting the failing tests ARE the split tests turns
    // the target page's "only where real DynamoDB itself disagrees between
    // regions" from an inference into a checked fact. A breach here means a
    // non-split behaviour is varying by region - the scoring model changed
    // underneath the claim the methodology makes.
    //
    // Second, the tripwire: the letter survives the target's worst region. If
    // real DynamoDB's regions ever drift far enough apart that a target can be
    // perfect in one and grade lower in another, an unconditional A+ stops
    // being honest. This failing is the signal to revisit the criteria in the
    // open, under a bumped GRADING_VERSION, not to loosen the assertion.
    let guarded = 0
    for (const [slug, t] of Object.entries(fresh.targets)) {
      const headline = t.regions[t.headline.region];
      if (!headline || headline.count === 0) continue;
      if (axesOf(headline).divergence !== 0) continue;
      guarded++

      const target = targets.find((x) => x.slug === slug);
      const verdicts = classifyResults(target.raw, target.sidecar ?? null);
      for (const [region, r] of Object.entries(t.regions)) {
        const fails = verdictsForRegion(verdicts, context.registry, region).filter(
          (v) => v.verdict === 'fail',
        );
        expect(fails.length, `${slug}'s scored fail count in ${region}`).toBe(r.failed);
        for (const f of fails) {
          expect(
            splitFor(context.registry, f),
            `${slug} fails "${f.fullName}" in ${region}, and it is not one of the registry's confirmed splits - a non-split behaviour is varying by region`,
          ).toBeTruthy();
        }
      }

      // A note on the day this first fires. The comparison is the published
      // letter against the worst region's, so at full coverage it reads A+
      // versus A: the first target ever to earn A+ while any confirmed split
      // exists will fail this, and the trigger is the ordinary A+ case rather
      // than an anomaly. That is deliberate - an A+ that holds only in the
      // headline region is the claim this guard exists to question - but read
      // it as a prompt to revisit the criteria in the open, not as a defect in
      // the target that tripped it.
      // The tolerance is the row's own letter, not the A band. Three splits in
      // a thousand tests is 0.3% against 5%, so the band could not bind until
      // the registry grew seventeenfold. Comparing the letter the headline
      // publishes against the one its worst region earns binds from the first
      // split that would move it.
      const coverage = axesOf(headline).coverage;
      const worst = Math.max(
        ...Object.values(t.regions).map((r) => axesOf(r).divergence ?? 0),
      )
      expect(
        gradeOf(worst, coverage).letter,
        `${slug} publishes ${gradeOf(0, coverage).letter} from ${t.headline.region} but its worst region earns less - revisit the A+ criteria before publishing`,
      ).toBe(gradeOf(0, coverage).letter);
    }

    // The loop above only runs for a target at exactly zero headline
    // divergence, so without this the whole guard could go quiet on a sweep
    // where no target holds A+ - green, having checked nothing, with no signal
    // that its coverage had dropped to zero. If this ever fails it is not a
    // licence to delete it: it means nothing on the board currently exercises
    // the A+ claim, and the claim should come down or the guard should move to
    // a fixture that does exercise it.
    expect(
      guarded,
      'no target holds a zero-divergence headline, so the A+ tripwire asserted nothing this run',
    ).toBeGreaterThan(0)
  })

  it('the ground truth earns its 100%: the real run scores 100% against its own region', () => {
    // Not an assumption: results/dynamodb.json is a real eu-west-2 run, and
    // scored against eu-west-2's recorded expectations it passes everything.
    // Self-agreement is what pins the row, so assert it from the data.
    const dynamodb = targets.find((t) => t.slug === GROUND_TRUTH_SLUG)
    const scored = scoreTarget(dynamodb.raw, dynamodb.sidecar, context)
    const own = scored.regions['eu-west-2']
    expect(own.failed).toBe(0)
    expect(own.passed).toBeGreaterThan(0)
  })

  it('leaves every results/*.json byte-identical: summary.json is additive', () => {
    // The per-target files are a de facto public contract (the site reads
    // them, and joins results/tag-manifest.json on file path + top-level
    // describe). The whole pipeline - read, score, render, write the summary -
    // must never rewrite them, or the site's current reader and its tag lens
    // would break silently.
    const hash = (f) => createHash('sha256').update(readFileSync(f)).digest('hex')
    const before = Object.fromEntries(files.map((f) => [f, hash(f)]))

    const read = readTargets(files)
    const summary = buildSummary(read, context)
    renderTable(summary)
    writeSummaryFile(summary, read, join(mkdtempSync(join(tmpdir(), 'summarise-')), 'summary.json'))

    for (const f of files) {
      expect(hash(f), `${f} was modified by the results pipeline`).toBe(before[f])
    }
  })
})

describe('readTargets', () => {
  it('pairs sidecars and versions, and skips reserved and companion files', () => {
    const dir = mkdtempSync(join(tmpdir(), 'targets-'))
    const doc = suiteDoc('passed')
    writeFileSync(join(dir, 'alpha.json'), JSON.stringify(doc))
    writeFileSync(join(dir, 'alpha.version'), '9.9.9\n')
    writeFileSync(
      join(dir, 'alpha.indeterminate.json'),
      JSON.stringify({ target: 'alpha', runLevel: [{ reason: 'table-active-timeout' }] }),
    )
    writeFileSync(join(dir, 'alpha.badge.json'), JSON.stringify({ schemaVersion: 1 }))
    writeFileSync(join(dir, 'local.json'), JSON.stringify(doc))
    writeFileSync(join(dir, 'summary.json'), JSON.stringify({ schemaVersion: 1 }))

    const targets = readTargets(readdirSync(dir).map((f) => join(dir, f)))
    expect(targets).toHaveLength(1)
    expect(targets[0]).toMatchObject({
      slug: 'alpha',
      version: '9.9.9',
      runDate: '2026-07-06',
      sidecar: { runLevel: [{ reason: 'table-active-timeout' }] },
    })
  })
})

// The surface the site workspace imports. It used to keep its own copies of
// these maps and they drifted, so the site now imports them from here and the
// two can only disagree if one of these exports goes missing or changes shape.
// A rename that looks harmless on this side breaks a build nobody ran, so the
// contract is pinned here rather than left to the site's own tests.
describe('the shared target surface', () => {
  it('exports the maps and helpers the site imports', () => {
    for (const [name, value] of [
      ['DISPLAY', DISPLAY],
      ['REPO', REPO],
    ]) {
      expect(value, `${name} must stay exported`).toBeTypeOf('object')
      expect(Object.keys(value).length, `${name} must not be empty`).toBeGreaterThan(0)
    }
    for (const [name, fn] of [
      ['display', display],
      ['repoUrl', repoUrl],
      ['label', label],
    ]) {
      expect(fn, `${name} must stay exported`).toBeTypeOf('function')
    }
  })

  it('names and links every target it scores', () => {
    // Every slug the table can render must be nameable and linkable, so a
    // target added to one map and not the other is caught here rather than
    // showing up on the published board as a bare slug.
    for (const slug of Object.keys(DISPLAY)) {
      expect(display(slug), `${slug} needs a display name`).toBe(DISPLAY[slug])
      expect(repoUrl(slug), `${slug} needs a project URL`).toBeTruthy()
      expect(label(slug)).toBe(`[${DISPLAY[slug]}](${REPO[slug]})`)
    }
    expect(Object.keys(REPO).sort()).toEqual(Object.keys(DISPLAY).sort())
  })

  it('degrades predictably for a slug it has never seen', () => {
    // The site renders whatever the results directory contains, so an unknown
    // slug has to produce something printable rather than undefined.
    expect(display('some-new-thing')).toBe('some new thing')
    expect(repoUrl('some-new-thing')).toBeNull()
    expect(label('some-new-thing')).toBe('some new thing')
  })
})
