import { CreateTableCommand, DescribeTableCommand, UpdateTableCommand, PutItemCommand, SearchVectorsCommand, DynamoDBServiceException } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, waitUntilActive, deleteTable, expectDynamoError } from '../../../src/helpers.js'
import { skipUnlessVectorIndexes, describeVectorIndex } from '../../../src/vector.js'
import { IndeterminateError } from '../../../src/indeterminate.js'

// The UpdateTable path of the vector index lifecycle. Separated from
// lifecycle.test.ts because index creation here runs on GSI timescales
// (~17 minutes observed for a 25-item table, eu-west-2, 2026-08-11), so this
// file rides in the slow online-index lane beside
// tests/tier2/updateTable/gsi.test.ts rather than in the gating run.

const tablesToCleanup: string[] = []

afterAll(async () => {
  await Promise.all(tablesToCleanup.map(deleteTable))
})

describe('UpdateTable — vector index lifecycle', { tags: ['update-table', 'search-vectors', 'control-plane', 'vector', 'slow'] }, () => {
  skipUnlessVectorIndexes()

  // Characterised 2026-08-11 (eu-west-2): an index added to a 25-item table
  // took ~17 minutes to become searchable, on the same online-index machinery
  // as GSIs. The walk on this path differs from the CreateTable path in three
  // observed ways: the table status moves to UPDATING, the Backfilling field
  // IS reported (false during early CREATING, true while backfilling, absent
  // once done), and SearchVectors rejects the whole time — first with "The
  // table does not have the specified index", then with "Cannot search
  // backfilling vector index". DescribeTable flipping to ACTIVE can lead the
  // search plane by a beat, so readiness is proven by a search succeeding,
  // never by the description alone.
  it('adds an index that backfills like a GSI, enforces one online action, then deletes it', async () => {
    const tableName = uniqueTableName('vec_upd')
    tablesToCleanup.push(tableName)
    await ddb.send(
      new CreateTableCommand({
        TableName: tableName,
        AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )
    await waitUntilActive(tableName)
    for (let i = 0; i < 5; i++) {
      await ddb.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            pk: { S: `item-${i}` },
            embedding: { L: [{ N: String(i) }, { N: '1' }, { N: '0' }] },
          },
        }),
      )
    }

    await ddb.send(
      new UpdateTableCommand({
        TableName: tableName,
        VectorIndexUpdates: [
          {
            Create: {
              IndexName: 'vix',
              VectorAttribute: { AttributeName: 'embedding' },
              Dimensions: 3,
              DistanceFunction: 'COSINE',
              Projection: { ProjectionType: 'ALL' },
            },
          },
        ],
      }),
    )

    // Walk to searchable: statuses stay within CREATING/ACTIVE, Backfilling
    // is reported on this path, and every pre-ready search failure is one of
    // the two characterised rejections. Readiness is a successful search.
    const seenStatuses = new Set<string>()
    let sawBackfillingField = false
    const deadline = Date.now() + 2_400_000
    for (;;) {
      const ix = await describeVectorIndex(tableName, 'vix')
      if (ix?.IndexStatus) seenStatuses.add(ix.IndexStatus)
      if (ix?.Backfilling !== undefined) sawBackfillingField = true
      try {
        const res = await ddb.send(
          new SearchVectorsCommand({
            TableName: tableName,
            IndexName: 'vix',
            SearchVector: [{ N: '1' }, { N: '1' }, { N: '0' }],
            TopK: 5,
          }),
        )
        expect(res.SearchResults).toHaveLength(5)
        break
      } catch (err) {
        expect(err).toBeInstanceOf(DynamoDBServiceException)
        expect((err as DynamoDBServiceException).name).toBe('ValidationException')
        expect((err as DynamoDBServiceException).message).toMatch(
          /The table does not have the specified index: vix|Cannot search backfilling vector index: vix/,
        )
      }
      if (Date.now() > deadline) {
        throw new IndeterminateError(
          'vector-index-timeout',
          `Vector index vix on ${tableName} never became searchable`,
        )
      }
      await new Promise((r) => setTimeout(r, 5000))
    }
    expect([...seenStatuses].every((s) => s === 'CREATING' || s === 'ACTIVE')).toBe(true)
    expect(sawBackfillingField).toBe(true)

    // One online index action per call, enforced with the GSI machinery's
    // own error.
    const extra = (name: string) => ({
      Create: {
        IndexName: name,
        VectorAttribute: { AttributeName: 'embedding' },
        Dimensions: 3,
        DistanceFunction: 'COSINE' as const,
        Projection: { ProjectionType: 'KEYS_ONLY' as const },
      },
    })
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateTableCommand({
            TableName: tableName,
            VectorIndexUpdates: [extra('vix2'), extra('vix3')],
          }),
        ),
      'LimitExceededException',
      'Subscriber limit exceeded: Only 1 online index can be created or deleted simultaneously per table',
    )

    // Delete removes the index and leaves the base table untouched.
    await ddb.send(
      new UpdateTableCommand({
        TableName: tableName,
        VectorIndexUpdates: [{ Delete: { IndexName: 'vix' } }],
      }),
    )
    const deleteDeadline = Date.now() + 300_000
    for (;;) {
      const ix = await describeVectorIndex(tableName, 'vix')
      if (!ix) break
      expect(ix.IndexStatus === 'DELETING' || ix.IndexStatus === 'ACTIVE').toBe(true)
      if (Date.now() > deleteDeadline) {
        throw new IndeterminateError(
          'vector-index-timeout',
          `Vector index vix on ${tableName} never finished deleting`,
        )
      }
      await new Promise((r) => setTimeout(r, 2000))
    }
    const table = await ddb.send(new DescribeTableCommand({ TableName: tableName }))
    expect(table.Table?.TableStatus).toBe('ACTIVE')
    expect(table.Table?.VectorIndexes ?? []).toHaveLength(0)
  }, 2_460_000)
})
