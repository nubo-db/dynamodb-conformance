import { CreateTableCommand, PutItemCommand, DynamoDBServiceException } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, deleteTable } from '../../../src/helpers.js'
import {
  skipUnlessVectorSearch,
  supportsVectorSearch,
  waitForVectorIndexActive,
} from '../../../src/vector.js'

// Exact rejection messages for writes into a vector-indexed table,
// characterised against real DynamoDB in eu-west-2 (2026-08-11, issue #125).
// Each message names the violated index; 'plain' carries the vector-shape
// constraints and 'schema' carries the SearchSchema HASH constraint, matching
// the table in tests/tier2/vectorSearch/writeValidation.test.ts, whose
// behavioural cells (the accepted-but-de-indexed write, vector removal) stay
// in tier 2 per the tier split.

const tableName = uniqueTableName('vec_wmsg')
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

describe('PutItem vector writes — exact error messages', { tags: ['put-item', 'data-plane', 'vector', 'negative-path'] }, () => {
  skipUnlessVectorSearch()

  let created = false

  beforeAll(async () => {
    if (!(await supportsVectorSearch())) return
    // Registered before the create so a partial setup still gets torn down.
    created = true
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
    await waitForVectorIndexActive(tableName, 'plain')
    await waitForVectorIndexActive(tableName, 'schema')
  })

  afterAll(async () => {
    if (created) await deleteTable(tableName)
  })

  it('vector with the wrong dimension count', async () => {
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

  it('vector element that is not a number, naming the element', async () => {
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

  it('vector attribute that is not a list', async () => {
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

  it('empty string in the SearchSchema HASH attribute', async () => {
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
})
