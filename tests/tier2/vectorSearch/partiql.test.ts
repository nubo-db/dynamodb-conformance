import { CreateTableCommand, PutItemCommand, ExecuteStatementCommand } from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, deleteTable, expectDynamoError } from '../../../src/helpers.js'
import {
  skipUnlessSearchVectors,
  supportsSearchVectors,
  waitForVectorIndexActive,
} from '../../../src/vector.js'

// PartiQL cannot reach a vector index, and the base-table item carrying the
// embedding reads back through PartiQL like any other item. Characterised
// against real DynamoDB in eu-west-2 (2026-08-11, issue #125).

const tableName = uniqueTableName('vec_pq')

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
      Item: { pk: { S: 'a' }, embedding: { L: [{ N: '1' }, { N: '0' }, { N: '0' }] } },
    }),
  )
})

afterAll(async () => {
  if (created) await deleteTable(tableName)
})

describe('PartiQL — vector indexes are out of reach', { tags: ['partiql', 'data-plane', 'vector'] }, () => {
  skipUnlessSearchVectors()

  it('rejects a PartiQL read of a vector index', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new ExecuteStatementCommand({
            Statement: `SELECT * FROM "${tableName}"."vix"`,
          }),
        ),
      'ValidationException',
      'Scan operation not supported on this index type',
    )
  })

  it('reads the vector attribute off the base table like any other item', async () => {
    const res = await ddb.send(
      new ExecuteStatementCommand({
        Statement: `SELECT * FROM "${tableName}" WHERE pk = 'a'`,
      }),
    )
    expect(res.Items?.[0]?.embedding?.L?.map((v) => Number(v.N))).toEqual([1, 0, 0])
  })
})
