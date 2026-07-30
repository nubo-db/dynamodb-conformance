import { describe, it, expect } from 'vitest'
import { createHash } from 'node:crypto'
import { mkdtempSync, readdirSync, readFileSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { buildBadge } from './badges.mjs'
import { GROUND_TRUTH_SLUG, axesOf, loadScoringContext, scoreTarget, verdictsForRegion } from './lib/score.mjs'
import { classifyResults } from './lib/classify.mjs'
import { splitFor } from './lib/registry.mjs'
import { gradeOf } from './lib/grade.mjs'
import {
  DISPLAY,
  REPO,
  SUMMARY_PATH,
  SUMMARY_SCHEMA_VERSION,
  buildSummary,
  display,
  label,
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
    expect(summary.groundTruth).toEqual({ slug: GROUND_TRUTH_SLUG, rate: 100, runDate: DAY })
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
    { registry: REGISTRY, health: HEALTHY },
  )
  const rows = tableRows(summary)

  it('renders the ground-truth row first, at an earned 100% across all regions', () => {
    // 100% by self-agreement: each real region scores 100% against its own
    // recorded behaviour, so the max over any observed set is 100%.
    expect(rows[0]).toMatchObject({
      target: label(GROUND_TRUTH_SLUG),
      grade: 'A+',
      total: '100.0%',
      divergence: '0.0%',
      coverage: '100.0%',
      failed: 0,
      passed: 3, // the suite size: the largest count seen in a full run
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
    .filter((f) => f.endsWith('.json'))
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
    // Divergence, coverage, the caps and the A+ tripwire all divide by the
    // whole-suite count, and "the denominator never moves" is a published
    // claim. A partial run (a file-level crash, or a filtered capture) would
    // shrink one target's denominator, inflate its coverage past the caps and
    // sail through the tripwire vacuously - so full-suite coverage is
    // asserted, not assumed.
    const size = Math.max(
      0,
      ...Object.values(fresh.targets).map((t) => t.regions[t.headline.region]?.count ?? 0),
    )
    for (const [slug, t] of Object.entries(fresh.targets)) {
      const count = t.regions[t.headline.region]?.count ?? 0
      if (count === 0) continue // a target that scored nothing publishes "-", not a shrunken figure
      expect(count, `${slug} scored ${count} of the ${size}-test suite`).toBe(size)
    }
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
    // Second, the tripwire: every such target's worst region stays within the
    // A band. Real DynamoDB's regions currently disagree on three behaviours
    // in about a thousand; if they ever drift far enough apart that a target
    // can be perfect somewhere while diverging past 5% somewhere else, an
    // unconditional A+ stops being honest. This failing is the signal to
    // revisit the criteria in the open, under a bumped GRADING_VERSION - not
    // to loosen the assertion.
    for (const [slug, t] of Object.entries(fresh.targets)) {
      const headline = t.regions[t.headline.region];
      if (!headline || headline.count === 0) continue;
      if (axesOf(headline).divergence !== 0) continue;

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
        const divergence = axesOf(r).divergence ?? 0;
        expect(
          divergence,
          `${slug} is perfect in ${t.headline.region} but leaves the A band in ${region} - revisit the A+ criteria before publishing`,
        ).toBeLessThan(5);
      }
    }
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

    const summary = buildSummary(readTargets(files), context)
    renderTable(summary)
    writeSummaryFile(summary, join(mkdtempSync(join(tmpdir(), 'summarise-')), 'summary.json'))

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
