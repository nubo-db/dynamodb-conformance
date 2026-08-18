import {
  ExecuteStatementCommand,
  PutItemCommand,
  DynamoDBServiceException,
} from '@aws-sdk/client-dynamodb'
import type { AttributeValue } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { isUnsupportedFault } from '../../../src/infra.js'
import {
  declareTables,
  partiqlIndexTableDef,
  PARTIQL_UNPROJECTED_ATTR,
  cleanupItems,
  waitForGsiConsistency,
} from '../../../src/helpers.js'

declareTables(partiqlIndexTableDef)

const TABLE = partiqlIndexTableDef.name

// Mirrors the capture fixture. Three properties are load-bearing: s3 carries no
// index attribute at all, so every index excludes it and a qualified read that
// returns it is reading the table; s1 and s4 share a gsi-all partition key, so a
// bounded read across them has a second row to continue to; and `nonproj` is
// projected by no index, which is what the projection and filter rules turn on.
const ITEMS: Record<string, AttributeValue>[] = [
  {
    pk: { S: 'p' }, sk: { S: 's1' },
    gsiPk: { S: 'x' }, gsiPk2: { S: 'y' },
    lsiSk: { S: 'l1' }, lsiSk2: { S: 'k1' }, lsiSk3: { S: 'm1' },
    projattr: { S: 'proj1' }, [PARTIQL_UNPROJECTED_ATTR]: { S: 'np1' },
  },
  {
    pk: { S: 'p' }, sk: { S: 's2' },
    gsiPk: { S: 'z' }, gsiPk2: { S: 'y' },
    lsiSk: { S: 'l2' }, lsiSk2: { S: 'k2' }, lsiSk3: { S: 'm2' },
    projattr: { S: 'proj2' }, [PARTIQL_UNPROJECTED_ATTR]: { S: 'np2' },
  },
  // No index attribute of any kind, so no index holds it.
  { pk: { S: 'p' }, sk: { S: 's3' }, [PARTIQL_UNPROJECTED_ATTR]: { S: 'np3' } },
  {
    pk: { S: 'p' }, sk: { S: 's4' },
    gsiPk: { S: 'x' }, gsiPk2: { S: 'y' },
    lsiSk: { S: 'l4' }, lsiSk2: { S: 'k4' }, lsiSk3: { S: 'm4' },
    projattr: { S: 'proj4' }, [PARTIQL_UNPROJECTED_ATTR]: { S: 'np4' },
  },
]

/** Run a statement and hand back the rows and the per-arm capacity. */
async function select(Statement: string, extra: Record<string, unknown> = {}) {
  const res = await ddb.send(
    new ExecuteStatementCommand({ Statement, ReturnConsumedCapacity: 'INDEXES', ...extra }),
  )
  const cc = res.ConsumedCapacity
  return {
    items: res.Items ?? [],
    nextToken: res.NextToken,
    total: cc?.CapacityUnits ?? 0,
    table: cc?.Table?.CapacityUnits ?? 0,
    gsi: (name: string) => cc?.GlobalSecondaryIndexes?.[name]?.CapacityUnits ?? 0,
    lsi: (name: string) => cc?.LocalSecondaryIndexes?.[name]?.CapacityUnits ?? 0,
  }
}

/** The attribute names one row came back with, sorted so order means nothing. */
const attrsOf = (item: Record<string, AttributeValue>) => Object.keys(item).sort()

describe('ExecuteStatement — index qualifier', { tags: ['partiql', 'data-plane', 'gsi', 'lsi'] }, () => {
  let supported = true

  beforeAll(async () => {
    try {
      await ddb.send(new ExecuteStatementCommand({
        Statement: `SELECT * FROM "${TABLE}" WHERE pk = 'partiql-canary'`,
      }))
    } catch (e: unknown) {
      // The suite's definition of "not implemented", so a target declining
      // PartiQL skips rather than failing every case here.
      if (isUnsupportedFault(e) || (e instanceof Error && e.name === 'UnrecognizedClientException')) {
        supported = false
        return
      }
    }

    for (const Item of ITEMS) {
      await ddb.send(new PutItemCommand({ TableName: TABLE, Item }))
    }

    // A GSI fills asynchronously, and a read of one still filling is
    // indistinguishable from an engine returning the wrong rows.
    await waitForGsiConsistency({
      tableName: TABLE,
      indexName: 'gsi-all',
      partitionKey: { name: 'gsiPk', value: { S: 'x' } },
      expectedCount: 2,
    })
    await waitForGsiConsistency({
      tableName: TABLE,
      indexName: 'gsi-inc',
      partitionKey: { name: 'gsiPk2', value: { S: 'y' } },
      expectedCount: 3,
    })
  })

  beforeEach(({ skip }) => { if (!supported) skip() })

  afterAll(async () => {
    if (!supported) return
    await cleanupItems(TABLE, ITEMS.map((i) => ({ pk: i.pk, sk: i.sk })))
  })

  describe('membership', () => {
    it('a qualified SELECT omits an item the index does not hold', async () => {
      const viaIndex = await select(`SELECT pk, sk FROM "${TABLE}"."gsi-all" WHERE gsiPk = 'x'`)
      expect(viaIndex.items.map((i) => i.sk.S).sort()).toEqual(['s1', 's4'])

      // The same rows plus s3 are all in the table, so the absence above is the
      // index's membership rather than the rows not existing.
      const viaTable = await select(`SELECT pk, sk FROM "${TABLE}" WHERE pk = 'p'`)
      expect(viaTable.items.map((i) => i.sk.S).sort()).toEqual(['s1', 's2', 's3', 's4'])
    })
  })

  describe('projection follows the index, not the item', () => {
    it('a KEYS_ONLY GSI returns the index key and the table key and nothing else', async () => {
      const r = await select(`SELECT * FROM "${TABLE}"."gsi-keys" WHERE gsiPk2 = 'y'`)
      expect(r.items.length).toBe(3)
      for (const item of r.items) {
        expect(attrsOf(item)).toEqual(['gsiPk2', 'pk', 'sk'])
      }
    })

    it('an INCLUDE GSI adds the named attribute and no others', async () => {
      const r = await select(`SELECT * FROM "${TABLE}"."gsi-inc" WHERE gsiPk2 = 'y'`)
      expect(r.items.length).toBe(3)
      for (const item of r.items) {
        expect(attrsOf(item)).toEqual(['gsiPk2', 'pk', 'projattr', 'sk'])
      }
    })

    it('a KEYS_ONLY LSI returns the index key and the table key and nothing else', async () => {
      const r = await select(`SELECT * FROM "${TABLE}"."lsi-keys" WHERE pk = 'p'`)
      expect(r.items.length).toBe(3)
      for (const item of r.items) {
        expect(attrsOf(item)).toEqual(['lsiSk2', 'pk', 'sk'])
      }
    })

    // The obvious reading of `SELECT *` is that it selects everything. It does
    // not: a star against a non-ALL index is the index's own projection, served
    // without touching the base table.
    it('SELECT * against a non-ALL index does not reach back', async () => {
      const gsi = await select(`SELECT * FROM "${TABLE}"."gsi-keys" WHERE gsiPk2 = 'y'`)
      expect(gsi.table).toBe(0)
      expect(gsi.gsi('gsi-keys')).toBe(gsi.total)

      const lsi = await select(`SELECT * FROM "${TABLE}"."lsi-keys" WHERE pk = 'p'`)
      expect(lsi.table).toBe(0)
      expect(lsi.lsi('lsi-keys')).toBe(lsi.total)
    })
  })

  describe('the reach-back splits by index kind', () => {
    it('an LSI serves an unprojected attribute from the base table', async () => {
      const r = await select(
        `SELECT ${PARTIQL_UNPROJECTED_ATTR} FROM "${TABLE}"."lsi-keys" WHERE pk = 'p'`,
      )
      expect(r.items.length).toBe(3)
      expect(r.items.every((i) => i[PARTIQL_UNPROJECTED_ATTR]?.S !== undefined)).toBe(true)
      // Charged on the table arm, which is the base-table fetch made visible.
      expect(r.table).toBeGreaterThan(0)
    })

    // A GSI does not share a partition with the table, so it has no cheap way
    // back and rejects instead.
    it('a GSI rejects an unprojected attribute rather than reaching back', async () => {
      try {
        await select(`SELECT ${PARTIQL_UNPROJECTED_ATTR} FROM "${TABLE}"."gsi-inc" WHERE gsiPk2 = 'y'`)
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(DynamoDBServiceException)
        expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      }
    })

    it('naming only projected attributes on an LSI charges no table arm', async () => {
      const r = await select(`SELECT lsiSk2 FROM "${TABLE}"."lsi-keys" WHERE pk = 'p'`)
      expect(r.table).toBe(0)
    })

    // The reach-back is sized on the rows it walked, not the rows the filter
    // kept, so a filter matching nothing costs what matching everything costs.
    it('a filtered reach-back is charged on rows walked, not rows kept', async () => {
      // The filter is on a projected non-key attribute, so it is applied to rows
      // already read. A condition on the index sort key would narrow the scan
      // instead, and would legitimately cost less.
      const none = await select(
        `SELECT ${PARTIQL_UNPROJECTED_ATTR} FROM "${TABLE}"."lsi-inc" WHERE pk = 'p' AND projattr = 'no-such-value'`,
      )
      const all = await select(
        `SELECT ${PARTIQL_UNPROJECTED_ATTR} FROM "${TABLE}"."lsi-inc" WHERE pk = 'p'`,
      )
      expect(none.items.length).toBe(0)
      expect(all.items.length).toBe(3)
      expect(none.table).toBe(all.table)
    })

    it('a bounded reach-back walks fewer rows and is charged less', async () => {
      const one = await select(
        `SELECT ${PARTIQL_UNPROJECTED_ATTR} FROM "${TABLE}"."lsi-keys" WHERE pk = 'p'`,
        { Limit: 1 },
      )
      const all = await select(
        `SELECT ${PARTIQL_UNPROJECTED_ATTR} FROM "${TABLE}"."lsi-keys" WHERE pk = 'p'`,
      )
      expect(one.items.length).toBe(1)
      expect(one.table).toBeLessThan(all.table)
    })
  })

  describe('capacity lands on the index arm', () => {
    it('a keyed GSI read charges the index and a zero table arm', async () => {
      const r = await select(`SELECT pk FROM "${TABLE}"."gsi-all" WHERE gsiPk = 'x'`)
      expect(r.table).toBe(0)
      expect(r.gsi('gsi-all')).toBeGreaterThan(0)
      expect(r.gsi('gsi-all')).toBe(r.total)
    })

    it('a keyed LSI read charges the index and a zero table arm', async () => {
      const r = await select(`SELECT pk FROM "${TABLE}"."lsi-all" WHERE pk = 'p'`)
      expect(r.table).toBe(0)
      expect(r.lsi('lsi-all')).toBeGreaterThan(0)
      expect(r.lsi('lsi-all')).toBe(r.total)
    })

    // ConsistentRead doubles an index arm the same way it doubles a table arm,
    // so the existing rate needs a new arm to land on rather than a new rule.
    it('ConsistentRead doubles the LSI arm', async () => {
      const eventual = await select(`SELECT pk FROM "${TABLE}"."lsi-all" WHERE pk = 'p'`)
      const consistent = await select(
        `SELECT pk FROM "${TABLE}"."lsi-all" WHERE pk = 'p'`,
        { ConsistentRead: true },
      )
      expect(consistent.lsi('lsi-all')).toBe(eventual.lsi('lsi-all') * 2)
      expect(consistent.total).toBe(eventual.total * 2)
    })

    it('ConsistentRead doubles both arms of a reach-back', async () => {
      const eventual = await select(
        `SELECT ${PARTIQL_UNPROJECTED_ATTR} FROM "${TABLE}"."lsi-keys" WHERE pk = 'p'`,
      )
      const consistent = await select(
        `SELECT ${PARTIQL_UNPROJECTED_ATTR} FROM "${TABLE}"."lsi-keys" WHERE pk = 'p'`,
        { ConsistentRead: true },
      )
      expect(consistent.table).toBe(eventual.table * 2)
      expect(consistent.lsi('lsi-keys')).toBe(eventual.lsi('lsi-keys') * 2)
    })

    it('rejects ConsistentRead against a GSI qualifier', async () => {
      try {
        await select(`SELECT pk FROM "${TABLE}"."gsi-all" WHERE gsiPk = 'x'`, { ConsistentRead: true })
        expect.unreachable('should have thrown')
      } catch (err) {
        expect(err).toBeInstanceOf(DynamoDBServiceException)
        expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      }
    })
  })
})
