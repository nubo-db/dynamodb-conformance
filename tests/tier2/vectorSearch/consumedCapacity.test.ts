import { CreateTableCommand, PutItemCommand, SearchVectorsCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, deleteTable } from '../../../src/helpers.js'
import {
  skipUnlessSearchVectors,
  supportsSearchVectors,
  waitForVectorIndexActive,
  waitForVectorSearchable,
} from '../../../src/vector.js'

// The two vector capacity units, characterised against real DynamoDB in
// eu-west-2 (2026-08-11, issue #125). They are metered in bytes processed and
// reported in shapes that break the classic ConsumedCapacity mould:
//
// - SearchVectors reports a bare { VectorSearchRequestBytes } object with no
//   CapacityUnits and no TableName, identically under TOTAL and INDEXES.
// - A write reports a VectorIndexes map (per-index VectorWriteRequestBytes)
//   beside the classic fields, under INDEXES only — and only when the write
//   actually changes what the index stores. Overwriting an item with an
//   identical vector reports no vector write at all: replication is
//   delta-based.
//
// Byte magnitudes are asserted as positive numbers, not pinned: the observed
// figures (1024 per index for a 3-dimension vector) look size-derived, but a
// single characterisation run is not enough history to pin a formula.

const tableName = uniqueTableName('vec_cap')
const vec = (...ns: number[]) => ns.map((n) => ({ N: String(n) }))

let created = false

beforeAll(async () => {
  if (!(await supportsSearchVectors())) return
  await ddb.send(
    new CreateTableCommand({
      TableName: tableName,
      AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
      BillingMode: 'PAY_PER_REQUEST',
      VectorIndexes: [
        {
          IndexName: 'vix',
          VectorAttribute: { AttributeName: 'embedding' },
          Dimensions: 3,
          DistanceFunction: 'COSINE',
          Projection: { ProjectionType: 'ALL' },
        },
      ],
    }),
  )
  created = true
  await waitForVectorIndexActive(tableName, 'vix')
  await ddb.send(
    new PutItemCommand({
      TableName: tableName,
      Item: { pk: { S: 'seed' }, embedding: { L: vec(1, 0, 0) } },
    }),
  )
  await waitForVectorSearchable({
    tableName,
    indexName: 'vix',
    searchVector: vec(1, 0, 0),
    expectedCount: 1,
  })
})

afterAll(async () => {
  if (created) await deleteTable(tableName)
})

describe('SearchVectors — ConsumedCapacity shape', { tags: ['search-vectors', 'data-plane', 'vector'] }, () => {
  skipUnlessSearchVectors()

  it('reports VectorSearchRequestBytes with no classic capacity fields', async () => {
    const res = await ddb.send(
      new SearchVectorsCommand({
        TableName: tableName,
        IndexName: 'vix',
        SearchVector: vec(1, 0, 0),
        TopK: 1,
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )
    const cc = res.ConsumedCapacity!
    expect(typeof cc.VectorSearchRequestBytes).toBe('number')
    expect(cc.VectorSearchRequestBytes!).toBeGreaterThan(0)
    const asClassic = cc as { CapacityUnits?: unknown; TableName?: unknown; Table?: unknown }
    expect(asClassic.CapacityUnits).toBeUndefined()
    expect(asClassic.TableName).toBeUndefined()
    expect(asClassic.Table).toBeUndefined()
  })

  it('reports the same shape under INDEXES as under TOTAL', async () => {
    const input = {
      TableName: tableName,
      IndexName: 'vix',
      SearchVector: vec(1, 0, 0),
      TopK: 1,
    }
    const total = await ddb.send(
      new SearchVectorsCommand({ ...input, ReturnConsumedCapacity: 'TOTAL' }),
    )
    const indexes = await ddb.send(
      new SearchVectorsCommand({ ...input, ReturnConsumedCapacity: 'INDEXES' }),
    )
    expect(indexes.ConsumedCapacity?.VectorSearchRequestBytes).toBe(
      total.ConsumedCapacity?.VectorSearchRequestBytes,
    )
  })

  it('reports nothing under NONE', async () => {
    const res = await ddb.send(
      new SearchVectorsCommand({
        TableName: tableName,
        IndexName: 'vix',
        SearchVector: vec(1, 0, 0),
        TopK: 1,
        ReturnConsumedCapacity: 'NONE',
      }),
    )
    expect(res.ConsumedCapacity).toBeUndefined()
  })
})

describe('PutItem — vector write capacity shape', { tags: ['put-item', 'data-plane', 'vector'] }, () => {
  skipUnlessSearchVectors()

  it('INDEXES adds a per-index VectorWriteRequestBytes map beside the classic fields', async () => {
    const res = await ddb.send(
      new PutItemCommand({
        TableName: tableName,
        ReturnConsumedCapacity: 'INDEXES',
        Item: { pk: { S: 'fresh-write' }, embedding: { L: vec(0, 1, 0) } },
      }),
    )
    const cc = res.ConsumedCapacity!
    expect(cc.CapacityUnits).toBe(1)
    expect(cc.Table?.CapacityUnits).toBe(1)
    const vix = cc.VectorIndexes?.vix
    expect(vix).toBeDefined()
    expect(typeof vix!.VectorWriteRequestBytes).toBe('number')
    expect(vix!.VectorWriteRequestBytes!).toBeGreaterThan(0)
  })

  it('TOTAL folds nothing in: no vector fields at all', async () => {
    const res = await ddb.send(
      new PutItemCommand({
        TableName: tableName,
        ReturnConsumedCapacity: 'TOTAL',
        Item: { pk: { S: 'total-write' }, embedding: { L: vec(0, 0, 1) } },
      }),
    )
    const cc = res.ConsumedCapacity!
    expect(cc.CapacityUnits).toBe(1)
    expect(cc.VectorIndexes).toBeUndefined()
    expect(cc.Table).toBeUndefined()
  })

  it('a write not touching the vector attribute reports no vector write', async () => {
    const res = await ddb.send(
      new PutItemCommand({
        TableName: tableName,
        ReturnConsumedCapacity: 'INDEXES',
        Item: { pk: { S: 'plain-write' }, label: { S: 'no-vector' } },
      }),
    )
    const cc = res.ConsumedCapacity!
    expect(cc.CapacityUnits).toBe(1)
    expect(cc.VectorIndexes).toBeUndefined()
  })

  it('an identical overwrite reports no vector write: replication is delta-based', async () => {
    const item = { pk: { S: 'idempotent' }, embedding: { L: vec(0.5, 0.5, 0) } }
    const first = await ddb.send(
      new PutItemCommand({ TableName: tableName, ReturnConsumedCapacity: 'INDEXES', Item: item }),
    )
    expect(first.ConsumedCapacity?.VectorIndexes?.vix?.VectorWriteRequestBytes).toBeGreaterThan(0)

    const repeat = await ddb.send(
      new PutItemCommand({ TableName: tableName, ReturnConsumedCapacity: 'INDEXES', Item: item }),
    )
    expect(repeat.ConsumedCapacity?.CapacityUnits).toBe(1)
    expect(repeat.ConsumedCapacity?.VectorIndexes).toBeUndefined()
  })
})
