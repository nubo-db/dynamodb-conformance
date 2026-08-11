import { CreateTableCommand, SearchVectorsCommand, type CreateTableCommandInput } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  deleteTable,
  expectDynamoError,
} from '../../../src/helpers.js'
import {
  skipUnlessVectorIndexes,
  skipUnlessSearchVectors,
  supportsSearchVectors,
  waitForVectorIndexActive,
} from '../../../src/vector.js'

// Request-model-layer rejections (the "N validation errors detected" family)
// for the vector surface. The service-layer rejections with documented exact
// wording live in tests/tier3/error-messages/{vectorIndexes,searchVectors}.test.ts;
// this file covers the cells whose messages come from the request model, plus
// the not-found shape, asserted at error-code level.

function vectorTableInput(name: string): CreateTableCommandInput {
  return {
    TableName: name,
    AttributeDefinitions: [{ AttributeName: 'pk', AttributeType: 'S' }],
    KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
    BillingMode: 'PAY_PER_REQUEST',
    VectorIndexes: [
      {
        IndexName: 'vix',
        VectorAttribute: { AttributeName: 'embedding' },
        Dimensions: 3,
        DistanceFunction: 'COSINE',
        Projection: { ProjectionType: 'KEYS_ONLY' },
      },
    ],
  }
}

describe('CreateTable — vector index request validation', { tags: ['create-table', 'control-plane', 'vector', 'negative-path'] }, () => {
  skipUnlessVectorIndexes()

  it('rejects Dimensions of 0 at the request model layer', async () => {
    const input = vectorTableInput(uniqueTableName('vec_val'))
    input.VectorIndexes![0].Dimensions = 0
    await expectDynamoError(
      () => ddb.send(new CreateTableCommand(input)),
      'ValidationException',
      'greater than or equal to 1',
    )
  })

  it('rejects an index name shorter than 3 characters', async () => {
    const input = vectorTableInput(uniqueTableName('vec_val'))
    input.VectorIndexes![0].IndexName = 'ab'
    await expectDynamoError(
      () => ddb.send(new CreateTableCommand(input)),
      'ValidationException',
      'length greater than or equal to 3',
    )
  })
})

describe('SearchVectors — request validation', { tags: ['search-vectors', 'data-plane', 'vector', 'negative-path'] }, () => {
  skipUnlessSearchVectors()

  const tableName = uniqueTableName('vec_sval')
  let created = false

  beforeAll(async () => {
    if (!(await supportsSearchVectors())) return
    await ddb.send(new CreateTableCommand(vectorTableInput(tableName)))
    created = true
    await waitForVectorIndexActive(tableName, 'vix')
  })

  afterAll(async () => {
    if (created) await deleteTable(tableName)
  })

  it('rejects TopK of 0 at the request model layer', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new SearchVectorsCommand({
            TableName: tableName,
            IndexName: 'vix',
            SearchVector: [{ N: '1' }, { N: '0' }, { N: '0' }],
            TopK: 0,
          }),
        ),
      'ValidationException',
      'greater than or equal to 1',
    )
  })

  it('rejects a search against an index the table does not have', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new SearchVectorsCommand({
            TableName: tableName,
            IndexName: 'no-such-index',
            SearchVector: [{ N: '1' }, { N: '0' }, { N: '0' }],
            TopK: 1,
          }),
        ),
      'ValidationException',
      'does not have the specified index',
    )
  })

})
