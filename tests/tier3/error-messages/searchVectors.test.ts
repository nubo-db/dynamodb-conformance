import {
  CreateTableCommand,
  SearchVectorsCommand,
  DynamoDBServiceException,
  type SearchVectorsCommandInput,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, deleteTable } from '../../../src/helpers.js'
import {
  skipUnlessSearchVectors,
  supportsSearchVectors,
  waitForVectorIndexActive,
} from '../../../src/vector.js'

// Exact SearchVectors rejection messages, characterised against real DynamoDB
// in eu-west-2 (2026-08-11, issue #125). Two observations that diverge from
// the AWS documentation are pinned as observed:
//
// - The TopK message embeds the offending value and the range; the docs quote
//   only the leading fragment.
// - The comparator message carries an "Invalid SearchConditionExpression: "
//   prefix the docs omit, and it fires for non-equality operators on BOTH
//   element kinds. The developer guide (equality only, everywhere) wins over
//   the API reference's claim that INLINE_FILTER attributes accept comparison
//   and range operators.

const tableName = uniqueTableName('vec_smsg')
const vec = (...ns: number[]) => ns.map((n) => ({ N: String(n) }))

async function expectExactRejection(
  input: SearchVectorsCommandInput,
  message: string,
): Promise<void> {
  try {
    await ddb.send(new SearchVectorsCommand(input))
    expect.unreachable('should have thrown')
  } catch (err) {
    expect(err).toBeInstanceOf(DynamoDBServiceException)
    expect((err as DynamoDBServiceException).name).toBe('ValidationException')
    expect((err as DynamoDBServiceException).message).toBe(message)
  }
}

describe('SearchVectors — exact error messages', { tags: ['search-vectors', 'data-plane', 'vector', 'negative-path'] }, () => {
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
  })

  afterAll(async () => {
    if (created) await deleteTable(tableName)
  })

  it('TopK above the maximum', async () => {
    await expectExactRejection(
      { TableName: tableName, IndexName: 'plain', SearchVector: vec(1, 0, 0), TopK: 101 },
      "Provided TopK value '101' is out of valid range. The value must be between 1 and 100 inclusive",
    )
  })

  it('missing SearchConditionExpression against a HASH-schema index', async () => {
    await expectExactRejection(
      { TableName: tableName, IndexName: 'schema', SearchVector: vec(1, 0, 0), TopK: 1 },
      'SearchConditionExpression must be provided when SearchSchema has a HASH key',
    )
  })

  it('non-equality comparator on the HASH element', async () => {
    await expectExactRejection(
      {
        TableName: tableName,
        IndexName: 'schema',
        SearchVector: vec(1, 0, 0),
        TopK: 1,
        SearchConditionExpression: 'tenant < :t',
        ExpressionAttributeValues: { ':t': { S: 't9' } },
      },
      'Invalid SearchConditionExpression: Invalid comparator used in SearchConditionExpression',
    )
  })

  it('non-equality comparator on an INLINE_FILTER element', async () => {
    await expectExactRejection(
      {
        TableName: tableName,
        IndexName: 'schema',
        SearchVector: vec(1, 0, 0),
        TopK: 1,
        SearchConditionExpression: 'tenant = :t AND category < :c',
        ExpressionAttributeValues: { ':t': { S: 't1' }, ':c': { S: 'c9' } },
      },
      'Invalid SearchConditionExpression: Invalid comparator used in SearchConditionExpression',
    )
  })

  it('query vector dimension mismatch', async () => {
    await expectExactRejection(
      { TableName: tableName, IndexName: 'plain', SearchVector: vec(1, 0), TopK: 1 },
      'Input search vector dimension 2 does not match vector index dimension 3',
    )
  })

  it('condition attribute outside the SearchSchema', async () => {
    await expectExactRejection(
      {
        TableName: tableName,
        IndexName: 'plain',
        SearchVector: vec(1, 0, 0),
        TopK: 1,
        SearchConditionExpression: 'tenant = :t',
        ExpressionAttributeValues: { ':t': { S: 't1' } },
      },
      'SearchConditionExpression must not contain any attributes that is not in SearchSchema. Invalid attribute: tenant',
    )
  })

  it('L-wrapped search vector', async () => {
    await expectExactRejection(
      {
        TableName: tableName,
        IndexName: 'plain',
        SearchVector: [{ L: vec(1, 0, 0) }],
        TopK: 1,
      },
      'Search vector contains invalid values. All values in the search vector must be a 32-bit floating-point number attribute',
    )
  })
})
