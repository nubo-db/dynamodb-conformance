import { CreateTableCommand, type VectorIndex } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { region } from '../../../src/aws-config.js'
import { uniqueTableName, deleteTable } from '../../../src/helpers.js'
import { ceilingsFor } from '../../../src/regions.js'
import { IndeterminateError } from '../../../src/indeterminate.js'
import {
  skipUnlessVectorIndexes,
  describeVectorIndex,
  waitForVectorIndexActive,
} from '../../../src/vector.js'

// Vector index lifecycle on the CreateTable path, characterised against real
// DynamoDB in eu-west-2 (2026-08-11, issue #125). This path is quick (~15s
// to ACTIVE) and never reports the Backfilling field — that field only
// appears for indexes added via UpdateTable, whose creation runs on GSI-like
// timescales and lives in updateLifecycle.test.ts, in the slow lane.

const tablesToCleanup: string[] = []

function vix(over: Partial<VectorIndex> = {}): VectorIndex {
  return {
    IndexName: 'vix',
    VectorAttribute: { AttributeName: 'embedding' },
    Dimensions: 3,
    DistanceFunction: 'COSINE',
    Projection: { ProjectionType: 'ALL' },
    ...over,
  }
}

afterAll(async () => {
  await Promise.all(tablesToCleanup.map(deleteTable))
})

describe('CreateTable — vector index lifecycle', { tags: ['create-table', 'control-plane', 'vector'] }, () => {
  skipUnlessVectorIndexes()

  it('walks CREATING to ACTIVE and describes the index faithfully', async () => {
    const tableName = uniqueTableName('vec_life')
    tablesToCleanup.push(tableName)
    await ddb.send(
      new CreateTableCommand({
        TableName: tableName,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
        VectorIndexes: [vix()],
      }),
    )

    // Collect every status the index reports on its way to ACTIVE: the walk
    // is CREATING -> ACTIVE with no BACKFILLING or UPDATING status value,
    // and the Backfilling field never appears on this creation path.
    const seenStatuses = new Set<string>()
    const deadline = Date.now() + ceilingsFor(region).tableActiveMs
    for (;;) {
      const ix = await describeVectorIndex(tableName, 'vix')
      if (ix?.IndexStatus) seenStatuses.add(ix.IndexStatus)
      expect(ix?.Backfilling).toBeUndefined()
      if (ix?.IndexStatus === 'ACTIVE') break
      if (Date.now() > deadline) {
        // A ceiling expiring is a failed observation, never a divergence.
        throw new IndeterminateError(
          'vector-index-timeout',
          `Vector index vix on ${tableName} never became ACTIVE within its ceiling`,
        )
      }
      await new Promise((r) => setTimeout(r, 1000))
    }
    expect([...seenStatuses].every((s) => s === 'CREATING' || s === 'ACTIVE')).toBe(true)

    const ix = (await describeVectorIndex(tableName, 'vix'))!
    expect(ix.IndexName).toBe('vix')
    expect(ix.Dimensions).toBe(3)
    expect(ix.DistanceFunction).toBe('COSINE')
    expect(ix.VectorAttribute?.AttributeName).toBe('embedding')
    expect(ix.Projection?.ProjectionType).toBe('ALL')
    expect(ix.IndexArn).toContain(`/index/vix`)
    expect(ix.ItemCount).toBe(0)
    expect(ix.IndexSizeBytes).toBe(0)
  }, 150_000)

  it('round-trips a SearchSchema of HASH and INLINE_FILTER elements', async () => {
    const tableName = uniqueTableName('vec_life')
    tablesToCleanup.push(tableName)
    await ddb.send(
      new CreateTableCommand({
        TableName: tableName,
        AttributeDefinitions: [
          { AttributeName: 'pk', AttributeType: 'S' },
          { AttributeName: 'tenant', AttributeType: 'S' },
          { AttributeName: 'category', AttributeType: 'S' },
        ],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
        VectorIndexes: [
          vix({
            SearchSchema: [
              { AttributeName: 'tenant', SearchSchemaElementType: 'HASH' },
              { AttributeName: 'category', SearchSchemaElementType: 'INLINE_FILTER' },
            ],
          }),
        ],
      }),
    )
    await waitForVectorIndexActive(tableName, 'vix')
    const ix = (await describeVectorIndex(tableName, 'vix'))!
    expect(ix.SearchSchema).toEqual([
      { AttributeName: 'tenant', SearchSchemaElementType: 'HASH' },
      { AttributeName: 'category', SearchSchemaElementType: 'INLINE_FILTER' },
    ])
  }, 150_000)

  it('accepts the 4096-dimension boundary', async () => {
    const tableName = uniqueTableName('vec_life')
    tablesToCleanup.push(tableName)
    await ddb.send(
      new CreateTableCommand({
        TableName: tableName,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
        VectorIndexes: [vix({ Dimensions: 4096 })],
      }),
    )
    await waitForVectorIndexActive(tableName, 'vix')
    const ix = (await describeVectorIndex(tableName, 'vix'))!
    expect(ix.Dimensions).toBe(4096)
  }, 150_000)
})
