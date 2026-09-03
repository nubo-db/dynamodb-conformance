import { describe, it, expect } from 'vitest'
import {
  createTableRegistry,
  isSuiteTable,
  uniqueTableName,
  absentTableName,
  compositeIndexedTableDef,
  partiqlIndexTableDef,
  PARTIQL_UNPROJECTED_ATTR,
} from './helpers.js'
import { resolveTablePrefix, CI_TABLE_PREFIX, LOCAL_TABLE_NAMESPACE } from './table-namespace.js'
import type { TestTableDef } from './types.js'

// The declaration registry behind demand-driven provisioning. Asserted against
// a synthetic creator: what needs proving is how many times a table is created
// and how many times the sweep runs, neither of which the suite can observe
// about itself.

const def = (name: string): TestTableDef => ({
  name,
  hashKey: { name: 'pk', type: 'S' },
})

// Declarations are keyed by the file that made them, which under vitest is this
// test file.
const thisFile = expect.getState().testPath ?? 'unknown'

/** A creator that records what it was asked to create. */
function recordingCreator() {
  const calls: string[] = []
  return {
    calls,
    create: async (d: TestTableDef) => {
      calls.push(d.name)
    },
  }
}

describe('createTableRegistry — declarations', () => {
  it('collects the defs a file declares', () => {
    const r = createTableRegistry()
    r.declare(def('a'), def('b'))
    expect(r.declaredDefs().map((d) => d.name)).toEqual(['a', 'b'])
  })

  it('attributes declarations to the declaring file', () => {
    const r = createTableRegistry()
    r.declare(def('a'))
    expect(r.declaredBy(thisFile).map((d) => d.name)).toEqual(['a'])
    expect(r.declaredBy('some/other/file.test.ts')).toEqual([])
  })

  it('deduplicates a def declared by several files', () => {
    const r = createTableRegistry()
    const shared = def('shared')
    r.declare(shared)
    r.declare(shared)
    r.declare(shared)
    expect(r.declaredDefs()).toHaveLength(1)
  })

  it('starts empty, so a run that selects nothing creates nothing', () => {
    expect(createTableRegistry().declaredDefs()).toEqual([])
  })
})

describe('createTableRegistry — provisioning', () => {
  it('creates each declared table exactly once across repeated provisions', async () => {
    const r = createTableRegistry()
    const { calls, create } = recordingCreator()
    r.declare(def('a'), def('b'))

    // The shared beforeAll runs per test file; forty files declaring the same
    // def must still create it once.
    await r.provision(create)
    await r.provision(create)
    await r.provision(create)

    expect(calls).toEqual(['a', 'b'])
  })

  it('creates only what a later file newly declared', async () => {
    const r = createTableRegistry()
    const { calls, create } = recordingCreator()

    r.declare(def('first'))
    await r.provision(create)
    r.declare(def('first'), def('second'))
    await r.provision(create)

    expect(calls).toEqual(['first', 'second'])
  })

  it('creates only the declared subset, not every table in the suite', async () => {
    const r = createTableRegistry()
    const { calls, create } = recordingCreator()
    r.declare(def('only-this-one'))
    await r.provision(create)
    expect(calls).toEqual(['only-this-one'])
  })

  it('creates nothing for a file that declared nothing', async () => {
    // The property that makes an excluded axis honest. `--tags-filter` skips
    // tests but still imports the file, so an excluded file's declaration is
    // registered; only its hook never runs. Provisioning the whole registry
    // would create its tables on behalf of whichever file runs next.
    const r = createTableRegistry()
    const { calls, create } = recordingCreator()
    r.declare(def('declared-by-an-excluded-file'))
    await r.provision(create, 'a/file/that/declared/nothing.test.ts')
    expect(calls).toEqual([])
  })

  it('creates only the requesting file\'s tables, not another file\'s', async () => {
    const r = createTableRegistry()
    const { calls, create } = recordingCreator()
    r.declare(def('ours'))
    await r.provision(create, thisFile)
    expect(calls).toEqual(['ours'])
  })

  it('does not create concurrently for a def already in flight', async () => {
    const r = createTableRegistry()
    const { calls, create } = recordingCreator()
    r.declare(def('a'))

    await Promise.all([r.provision(create), r.provision(create), r.provision(create)])

    expect(calls).toEqual(['a'])
  })

  it('propagates a creation failure so setup can record it at run level', async () => {
    const r = createTableRegistry()
    r.declare(def('a'))
    await expect(
      r.provision(async () => {
        throw new Error('CreateTable exploded')
      }),
    ).rejects.toThrow('CreateTable exploded')
  })

  it('retries a failed creation on the next attempt rather than replaying the rejection', async () => {
    const r = createTableRegistry()
    r.declare(def('a'))
    let attempts = 0
    const flaky = async () => {
      attempts++
      if (attempts === 1) throw new Error('transient')
    }

    await expect(r.provision(flaky)).rejects.toThrow('transient')
    await expect(r.provision(flaky)).resolves.toBeUndefined()
    expect(attempts).toBe(2)
  })

  it('does not retry a table that already succeeded when a sibling failed', async () => {
    const r = createTableRegistry()
    r.declare(def('good'), def('bad'))
    const seen: string[] = []
    const create = async (d: TestTableDef) => {
      seen.push(d.name)
      if (d.name === 'bad' && seen.filter((n) => n === 'bad').length === 1) {
        throw new Error('transient')
      }
    }

    await expect(r.provision(create)).rejects.toThrow('transient')
    await r.provision(create)

    expect(seen).toEqual(['good', 'bad', 'bad'])
  })
})

describe('createTableRegistry — the leftover sweep', () => {
  it('sweeps once however many files call it', async () => {
    const r = createTableRegistry()
    let sweeps = 0
    const sweep = async () => {
      sweeps++
    }

    await r.sweepOnce(sweep)
    await r.sweepOnce(sweep)
    await r.sweepOnce(sweep)

    expect(sweeps).toBe(1)
  })

  it('does not sweep again once tables have been provisioned', async () => {
    // A sweep sharing provisioning's guard would delete tables still in use.
    const r = createTableRegistry()
    const { create } = recordingCreator()
    let sweeps = 0
    const sweep = async () => {
      sweeps++
    }

    r.declare(def('a'))
    await r.sweepOnce(sweep)
    await r.provision(create)
    r.declare(def('b'))
    await r.sweepOnce(sweep)
    await r.provision(create)

    expect(sweeps).toBe(1)
  })

  it('sweeps once under concurrent callers', async () => {
    const r = createTableRegistry()
    let sweeps = 0
    const sweep = async () => {
      sweeps++
    }

    await Promise.all([r.sweepOnce(sweep), r.sweepOnce(sweep), r.sweepOnce(sweep)])

    expect(sweeps).toBe(1)
  })

  it('retries a failed sweep, so a transient fault does not skip cleanup for the run', async () => {
    const r = createTableRegistry()
    let attempts = 0
    const flaky = async () => {
      attempts++
      if (attempts === 1) throw new Error('ListTables exploded')
    }

    await expect(r.sweepOnce(flaky)).rejects.toThrow('ListTables exploded')
    await r.sweepOnce(flaky)
    await r.sweepOnce(flaky)

    expect(attempts).toBe(2)
  })

  it('sweeps before any table is created, on every file', async () => {
    // src/setup.ts awaits the sweep then provisions. This pins the ordering
    // that sequence exists to guarantee: nothing may be created before the
    // one sweep has finished, or the sweep deletes it again.
    const r = createTableRegistry()
    const order: string[] = []
    const sweep = async () => {
      await Promise.resolve()
      order.push('sweep')
    }
    const create = async (d: TestTableDef) => {
      order.push(`create:${d.name}`)
    }

    r.declare(def('a'))
    await r.sweepOnce(sweep)
    await r.provision(create)
    r.declare(def('b'))
    await r.sweepOnce(sweep)
    await r.provision(create)

    expect(order).toEqual(['sweep', 'create:a', 'create:b'])
  })

  it('keeps its guard independent of the provisioning memo', async () => {
    const r = createTableRegistry()
    const { calls, create } = recordingCreator()
    let sweeps = 0

    r.declare(def('a'))
    await expect(
      r.sweepOnce(async () => {
        sweeps++
        throw new Error('transient')
      }),
    ).rejects.toThrow('transient')
    await r.provision(create)
    await r.sweepOnce(async () => {
      sweeps++
    })

    expect({ sweeps, calls }).toEqual({ sweeps: 2, calls: ['a'] })
  })
})

describe('isSuiteTable', () => {
  it('selects a table in the given namespace', () => {
    expect(isSuiteTable(`${CI_TABLE_PREFIX}hash_1_0`, CI_TABLE_PREFIX)).toBe(true)
  })

  it('rejects a table in the other namespace, which is what protects a live run', () => {
    expect(isSuiteTable(`${CI_TABLE_PREFIX}hash_1_0`, '_capture_20260818_abcdef_')).toBe(false)
    expect(isSuiteTable(`${LOCAL_TABLE_NAMESPACE}hash_1_0`, CI_TABLE_PREFIX)).toBe(false)
  })

  it('rejects another local session in the same namespace', () => {
    expect(isSuiteTable('_capture_20260818_aaaaaa_hash_1_0', '_capture_20260818_bbbbbb_')).toBe(false)
  })

  it('rejects a table belonging to nobody', () => {
    expect(isSuiteTable('orders', CI_TABLE_PREFIX)).toBe(false)
  })
})

describe('uniqueTableName', () => {
  it('names into the namespace this run resolved', () => {
    expect(isSuiteTable(uniqueTableName('hash'))).toBe(true)
  })

  it('leaves at least three characters after the prefix, which some targets require', () => {
    const name = uniqueTableName('h')
    expect(name.slice(resolveTablePrefix().length).length).toBeGreaterThanOrEqual(3)
  })

  it('does not repeat a name within a run', () => {
    expect(uniqueTableName('hash')).not.toBe(uniqueTableName('hash'))
  })
})

describe('absentTableName', () => {
  // A name outside the namespace is refused by IAM before DynamoDB can answer
  // that the table is missing, so the test sees the wrong exception.
  it('sits inside the namespace this run can reach', () => {
    expect(isSuiteTable(absentTableName('nonexistent_table'))).toBe(true)
  })

  it('keeps the suffix it was given', () => {
    expect(absentTableName('nonexistent_table').endsWith('nonexistent_table')).toBe(true)
  })
})

// The fixture the PartiQL index-qualifier cases are captured against. Asserted
// here because the rules those cases encode are properties of the fixture as
// much as of DynamoDB: an index that projects everything cannot demonstrate a
// reach-back, and an attribute some index carries cannot demonstrate a
// rejection.
describe('partiqlIndexTableDef', () => {
  const gsis = partiqlIndexTableDef.gsis ?? []
  const lsis = partiqlIndexTableDef.lsis ?? []

  it('carries the five indexes the capture used, at the capture projections', () => {
    expect(gsis.map((g) => [g.indexName, g.projectionType])).toEqual([
      ['gsi-all', 'ALL'],
      ['gsi-inc', 'INCLUDE'],
      ['gsi-keys', 'KEYS_ONLY'],
    ])
    expect(lsis.map((l) => [l.indexName, l.projectionType])).toEqual([
      ['lsi-all', 'ALL'],
      ['lsi-keys', 'KEYS_ONLY'],
      ['lsi-inc', 'INCLUDE'],
    ])
  })

  // Two GSIs on one partition key is what lets a token minted against one be
  // replayed against the other, which is a case in its own right.
  it('gives two GSIs the same partition key attribute', () => {
    expect(gsis.find((g) => g.indexName === 'gsi-inc')!.hashKey.name).toBe('gsiPk2')
    expect(gsis.find((g) => g.indexName === 'gsi-keys')!.hashKey.name).toBe('gsiPk2')
  })

  // Scoped to the non-ALL indexes on purpose. An ALL index projects everything,
  // so it carries this attribute too and declares no NonKeyAttributes to say so:
  // asserting over every index would be an assertion that cannot fail rather
  // than one that holds.
  it('leaves the unprojected attribute out of every non-ALL index', () => {
    const partial = [
      ...gsis.filter((g) => g.projectionType !== 'ALL'),
      ...lsis.filter((l) => l.projectionType !== 'ALL'),
    ]
    expect(partial.length).toBeGreaterThan(0)

    for (const idx of partial) {
      const carried = [
        'hashKey' in idx ? idx.hashKey.name : undefined,
        idx.rangeKey?.name,
        ...(idx.nonKeyAttributes ?? []),
      ].filter(Boolean)
      expect(carried, idx.indexName).not.toContain(PARTIQL_UNPROJECTED_ATTR)
    }
  })

  // The other half of the same contract: the ALL indexes do carry it, which is
  // why a rejection case must not name one of them.
  it('records which indexes do project it, so a later case does not name one', () => {
    const all = [
      ...gsis.filter((g) => g.projectionType === 'ALL').map((g) => g.indexName),
      ...lsis.filter((l) => l.projectionType === 'ALL').map((l) => l.indexName),
    ]
    expect(all).toEqual(['gsi-all', 'lsi-all'])
  })

  // An ALL index projects everything, so it can never reject a projection or a
  // filter. The rules need an index that does not.
  it('includes an index projecting a named attribute and not that one', () => {
    const inc = gsis.find((g) => g.indexName === 'gsi-inc')!
    expect(inc.nonKeyAttributes).toEqual(['projattr'])
    expect(inc.nonKeyAttributes).not.toContain(PARTIQL_UNPROJECTED_ATTR)
  })

  // Separating rows walked from rows kept needs an LSI carrying a non-key
  // attribute to filter on: a KEYS_ONLY index has none, and its sort key is a
  // key condition rather than a filter.
  it('includes an LSI projecting a non-key attribute', () => {
    const inc = lsis.find((l) => l.indexName === 'lsi-inc')!
    expect(inc.nonKeyAttributes).toEqual(['projattr'])
    expect(inc.rangeKey.name).not.toBe('projattr')
  })

  it('is a separate table from the shared indexed def', () => {
    expect(partiqlIndexTableDef.name).not.toBe(compositeIndexedTableDef.name)
  })

  it('is a composite-key table, since the LSIs need a range key', () => {
    expect(partiqlIndexTableDef.hashKey.name).toBe('pk')
    expect(partiqlIndexTableDef.rangeKey?.name).toBe('sk')
  })
})
