import { CreateTableCommand, PutItemCommand, GetItemCommand, UpdateItemCommand, SearchVectorsCommand, DynamoDBServiceException } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, deleteTable } from '../../../src/helpers.js'
import {
  skipUnlessSearchVectors,
  supportsSearchVectors,
  waitForVectorIndexActive,
  waitForVectorSearchable,
} from '../../../src/vector.js'
import { IndeterminateError } from '../../../src/indeterminate.js'

// Write-path validation on a vector-indexed table, characterised against real
// DynamoDB in eu-west-2 (2026-08-11, issue #125). Two indexes on the same
// vector attribute: 'plain' (no SearchSchema) and 'schema' (HASH tenant +
// INLINE_FILTER category). The split matters for the silent de-index case: an
// item written without the HASH attribute still lands in 'plain', and is only
// unreachable through 'schema' — the write-rejection messages cite whichever
// index the violated constraint belongs to.

const tableName = uniqueTableName('vec_write')
const vec = (...ns: number[]) => ns.map((n) => ({ N: String(n) }))

async function expectExactRejection(
  fn: () => Promise<unknown>,
  message: string,
): Promise<void> {
  try {
    await fn()
    expect.unreachable('should have thrown')
  } catch (err) {
    expect(err).toBeInstanceOf(DynamoDBServiceException)
    expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    expect((err as DynamoDBServiceException).message).toBe(message)
  }
}

describe('PutItem — vector index write validation', { tags: ['put-item', 'search-vectors', 'data-plane', 'vector'] }, () => {
  skipUnlessSearchVectors()

  let created = false

  beforeAll(async () => {
    if (!(await supportsSearchVectors())) return
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
          {
            IndexName: 'plain',
            VectorAttribute: { AttributeName: 'embedding' },
            Dimensions: 3,
            DistanceFunction: 'COSINE',
            Projection: { ProjectionType: 'ALL' },
          },
          {
            IndexName: 'schema',
            VectorAttribute: { AttributeName: 'embedding' },
            Dimensions: 3,
            DistanceFunction: 'COSINE',
            Projection: { ProjectionType: 'ALL' },
            SearchSchema: [
              { AttributeName: 'tenant', SearchSchemaElementType: 'HASH' },
              { AttributeName: 'category', SearchSchemaElementType: 'INLINE_FILTER' },
            ],
          },
        ],
      }),
    )
    created = true
    await waitForVectorIndexActive(tableName, 'plain')
    await waitForVectorIndexActive(tableName, 'schema')
    // Two well-formed items; positive evidence for the de-index assertions.
    for (const [pk, embedding] of [
      ['a', vec(1, 0, 0)],
      ['b', vec(0, 1, 0)],
    ] as const) {
      await ddb.send(
        new PutItemCommand({
          TableName: tableName,
          Item: {
            pk: { S: pk },
            tenant: { S: 't1' },
            category: { S: 'c1' },
            embedding: { L: embedding },
          },
        }),
      )
    }
    await waitForVectorSearchable({
      tableName,
      indexName: 'schema',
      searchVector: vec(1, 0, 0),
      expectedCount: 2,
      searchConditionExpression: 'tenant = :t',
      expressionAttributeValues: { ':t': { S: 't1' } },
    })
  })

  afterAll(async () => {
    if (created) await deleteTable(tableName)
  })

  it('rejects a vector with the wrong dimension count', async () => {
    await expectExactRejection(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: tableName,
            Item: { pk: { S: 'bad-dims' }, tenant: { S: 't1' }, embedding: { L: vec(1, 0) } },
          }),
        ),
      'One or more parameter values were invalid. Invalid size for parameter embedding, Expected: 3, Actual: 2 IndexName: plain',
    )
  })

  it('rejects a vector whose element is not a number, naming the element', async () => {
    await expectExactRejection(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: tableName,
            Item: {
              pk: { S: 'bad-type' },
              tenant: { S: 't1' },
              embedding: { L: [{ N: '1' }, { S: 'oops' }, { N: '0' }] },
            },
          }),
        ),
      'One or more parameter values were invalid. Invalid type for parameter embedding[1], Expected: 32-bit floating point number, Actual: S. IndexName: plain',
    )
  })

  it('rejects a vector attribute that is not a list', async () => {
    await expectExactRejection(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: tableName,
            Item: { pk: { S: 'bad-shape' }, tenant: { S: 't1' }, embedding: { S: 'not-a-vector' } },
          }),
        ),
      'One or more parameter values were invalid. Invalid type for parameter embedding, Expected: 32-bit floating point number list IndexName: plain',
    )
  })

  it('rejects an empty string in the SearchSchema HASH attribute', async () => {
    await expectExactRejection(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: tableName,
            Item: { pk: { S: 'bad-hash' }, tenant: { S: '' }, embedding: { L: vec(0, 0, 1) } },
          }),
        ),
      'One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty string value. IndexName: schema, IndexKey: tenant',
    )
  })

  it('accepts a write missing the HASH attribute but leaves it unreachable through that index', async () => {
    // The documented silent de-index: the write succeeds on the base table
    // and the item still lands in indexes whose constraints it satisfies.
    await ddb.send(
      new PutItemCommand({
        TableName: tableName,
        Item: { pk: { S: 'no-tenant' }, category: { S: 'c1' }, embedding: { L: vec(0, 0, 1) } },
      }),
    )
    const got = await ddb.send(
      new GetItemCommand({ TableName: tableName, Key: { pk: { S: 'no-tenant' } } }),
    )
    expect(got.Item?.pk?.S).toBe('no-tenant')

    // Positive evidence first: the plain index (no SearchSchema) reflects it.
    await waitForVectorSearchable({
      tableName,
      indexName: 'plain',
      searchVector: vec(0, 0, 1),
      expectedCount: 3,
    })
    // Through the schema index the item is unreachable: every search is
    // scoped to one tenant partition and the item belongs to none.
    const res = await ddb.send(
      new SearchVectorsCommand({
        TableName: tableName,
        IndexName: 'schema',
        SearchVector: vec(0, 0, 1),
        TopK: 10,
        SearchConditionExpression: 'tenant = :t',
        ExpressionAttributeValues: { ':t': { S: 't1' } },
      }),
    )
    const pks = (res.SearchResults ?? []).map((r) => r.Item?.pk?.S)
    expect(pks).toContain('a')
    expect(pks).toContain('b')
    expect(pks).not.toContain('no-tenant')
  })

  it('removes an item from the index when its vector attribute is removed', async () => {
    await ddb.send(
      new UpdateItemCommand({
        TableName: tableName,
        Key: { pk: { S: 'b' } },
        UpdateExpression: 'REMOVE embedding',
      }),
    )
    // The index is eventually consistent; poll until 'b' disappears, and
    // surface a ceiling expiry as a failed observation, not a divergence.
    const deadline = Date.now() + 10_000
    for (;;) {
      const res = await ddb.send(
        new SearchVectorsCommand({
          TableName: tableName,
          IndexName: 'plain',
          SearchVector: vec(0, 1, 0),
          TopK: 10,
        }),
      )
      const pks = (res.SearchResults ?? []).map((r) => r.Item?.pk?.S)
      if (!pks.includes('b')) {
        expect(pks).toContain('a')
        break
      }
      if (Date.now() > deadline) {
        throw new IndeterminateError(
          'vector-consistency-timeout',
          `Vector index plain on ${tableName} still reflects a removed embedding`,
        )
      }
      await new Promise((r) => setTimeout(r, 500))
    }
  })
})
