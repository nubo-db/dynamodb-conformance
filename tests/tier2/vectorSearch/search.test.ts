import { CreateTableCommand, PutItemCommand, GetItemCommand, SearchVectorsCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, deleteTable } from '../../../src/helpers.js'
import {
  skipUnlessVectorSearch,
  supportsVectorSearch,
  waitForVectorIndexActive,
  waitForVectorSearchable,
} from '../../../src/vector.js'

// Deterministic SearchVectors behaviour on a fixture built for separation,
// characterised against real DynamoDB in eu-west-2 (2026-08-11, issue #125).
//
// The search is approximate with no exact-search guarantee at any size, so
// nothing here trusts ANN luck: the four vectors are pairwise far apart under
// all three distance functions, the query vector is identical to one stored
// vector (the extremal score), and a 100-repeat pre-run against real AWS
// showed a single stable ordering with byte-identical scores throughout.
// Scores are the analytic values rounded through f32 (the index's storage
// precision), asserted with Math.fround.

const tableName = uniqueTableName('vec_search')
const vec = (...ns: number[]) => ns.map((n) => ({ N: String(n) }))

// pk -> embedding. 'a' equals the query vector; 'd' is a unit-norm mix; 'b'
// is orthogonal; 'c' is opposite.
const FIXTURE: Record<string, number[]> = {
  a: [1, 0, 0],
  b: [0, 1, 0],
  c: [-1, 0, 0],
  d: [0.6, 0.8, 0],
}

async function scoresFor(indexName: string): Promise<Record<string, number>> {
  const res = await ddb.send(
    new SearchVectorsCommand({
      TableName: tableName,
      IndexName: indexName,
      SearchVector: vec(1, 0, 0),
      TopK: 10,
    }),
  )
  const out: Record<string, number> = {}
  for (const r of res.SearchResults ?? []) out[r.Item!.pk!.S!] = r.Score!
  return out
}

let created = false

beforeAll(async () => {
  if (!(await supportsVectorSearch())) return
  // Registered before the create so a partial setup still gets torn down.
  created = true
  await ddb.send(
    new CreateTableCommand({
      TableName: tableName,
      AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
      KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
      BillingMode: 'PAY_PER_REQUEST',
      VectorIndexes: (
        [
          ['cosine', 'COSINE'],
          ['euclidean', 'EUCLIDEAN'],
          ['dot', 'DOT_PRODUCT'],
        ] as const
      ).map(([name, fn]) => ({
        IndexName: name,
        VectorAttribute: { AttributeName: 'embedding' },
        Dimensions: 3,
        DistanceFunction: fn,
        Projection: { ProjectionType: 'ALL' },
      })),
    }),
  )
  for (const index of ['cosine', 'euclidean', 'dot']) {
    await waitForVectorIndexActive(tableName, index)
  }
  for (const [pk, embedding] of Object.entries(FIXTURE)) {
    await ddb.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { pk: { S: pk }, label: { S: `item-${pk}` }, embedding: { L: vec(...embedding) } },
      }),
    )
  }
  for (const index of ['cosine', 'euclidean', 'dot']) {
    await waitForVectorSearchable({
      tableName,
      indexName: index,
      searchVector: vec(1, 0, 0),
      expectedCount: 4,
    })
  }
})

afterAll(async () => {
  if (created) await deleteTable(tableName)
})

describe('SearchVectors — deterministic search behaviour', { tags: ['search-vectors', 'data-plane', 'vector'] }, () => {
  skipUnlessVectorSearch()

  it('COSINE: identical vector scores 0, opposite scores 2, lower is closer', async () => {
    const scores = await scoresFor('cosine')
    expect(scores.a).toBe(0)
    expect(scores.d).toBeCloseTo(Math.fround(0.4), 6)
    expect(scores.b).toBe(1)
    expect(scores.c).toBe(2)
  })

  it('EUCLIDEAN: identical vector scores 0, straight-line distance otherwise', async () => {
    const scores = await scoresFor('euclidean')
    expect(scores.a).toBe(0)
    expect(scores.d).toBeCloseTo(Math.fround(Math.sqrt(0.8)), 6)
    expect(scores.b).toBeCloseTo(Math.fround(Math.SQRT2), 6)
    expect(scores.c).toBe(2)
  })

  it('DOT_PRODUCT: higher is closer and scores can be negative', async () => {
    const scores = await scoresFor('dot')
    expect(scores.a).toBe(1)
    expect(scores.d).toBeCloseTo(Math.fround(0.6), 6)
    expect(scores.b).toBe(0)
    expect(scores.c).toBe(-1)
  })

  it('returns the identical vector as the top match', async () => {
    const res = await ddb.send(
      new SearchVectorsCommand({
        TableName: tableName,
        IndexName: 'cosine',
        SearchVector: vec(1, 0, 0),
        TopK: 1,
      }),
    )
    expect(res.SearchResults).toHaveLength(1)
    expect(res.SearchResults![0].Item?.pk?.S).toBe('a')
  })

  it('omits the vector attribute by default and returns it when projected', async () => {
    const byDefault = await ddb.send(
      new SearchVectorsCommand({
        TableName: tableName,
        IndexName: 'cosine',
        SearchVector: vec(1, 0, 0),
        TopK: 1,
      }),
    )
    expect(Object.keys(byDefault.SearchResults![0].Item!).sort()).toEqual(['label', 'pk'])

    const projected = await ddb.send(
      new SearchVectorsCommand({
        TableName: tableName,
        IndexName: 'cosine',
        SearchVector: vec(1, 0, 0),
        TopK: 1,
        ProjectionExpression: 'pk, embedding',
      }),
    )
    const embedding = projected.SearchResults![0].Item?.embedding?.L
    // The index hands back its own f32 copy, so the values compare
    // numerically, not textually ('1' comes back as '1.0').
    expect(embedding?.map((v) => Number(v.N))).toEqual([1, 0, 0])
  })

  it('caps results at the item count with no pagination surface', async () => {
    const res = await ddb.send(
      new SearchVectorsCommand({
        TableName: tableName,
        IndexName: 'cosine',
        SearchVector: vec(1, 0, 0),
        TopK: 100,
      }),
    )
    expect(res.SearchResults).toHaveLength(4)
    expect((res as { NextToken?: unknown }).NextToken).toBeUndefined()
    expect((res as { LastEvaluatedKey?: unknown }).LastEvaluatedKey).toBeUndefined()
  })

  it('stores full precision on the base table and f32 in the index', async () => {
    // 16777217 is the first integer a 32-bit float cannot represent.
    const precise = ['0.1', '16777217', '1.000000059604644775390625']
    await ddb.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { pk: { S: 'f32' }, embedding: { L: precise.map((N) => ({ N })) } },
      }),
    )
    const back = await ddb.send(
      new GetItemCommand({
        TableName: tableName,
        Key: { pk: { S: 'f32' } },
        ConsistentRead: true,
      }),
    )
    expect(back.Item?.embedding?.L?.map((v) => v.N)).toEqual(precise)

    await waitForVectorSearchable({
      tableName,
      indexName: 'cosine',
      searchVector: vec(1, 0, 0),
      expectedCount: 5,
    })
    const res = await ddb.send(
      new SearchVectorsCommand({
        TableName: tableName,
        IndexName: 'cosine',
        SearchVector: precise.map((N) => ({ N })),
        TopK: 10,
        ProjectionExpression: 'pk, embedding',
      }),
    )
    const hit = res.SearchResults?.find((r) => r.Item?.pk?.S === 'f32')
    expect(hit).toBeDefined()
    const projected = hit!.Item!.embedding!.L!.map((v) => Number(v.N))
    // The index serialises its f32 copy as the shortest decimal naming that
    // float ('0.1', not '0.100000001...'), so the comparison happens in f32
    // space rather than on the parsed doubles.
    expect(projected.map(Math.fround)).toEqual(precise.map((n) => Math.fround(Number(n))))
    expect(projected[1]).toBe(16777216)
  })
})
