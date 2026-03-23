import {
  CreateTableCommand,
  DescribeTableCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, waitUntilActive, deleteTable, expectDynamoError } from '../../../src/helpers.js'

describe('CreateTable — LSI', () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('creates a table with an LSI (requires composite key)', async () => {
    const name = uniqueTableName('ct_lsi')
    tablesToCleanup.push(name)

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'pk', AttributeType: 'S' },
          { AttributeName: 'sk', AttributeType: 'S' },
          { AttributeName: 'lsiSk', AttributeType: 'N' },
        ],
        KeySchema: [
          { AttributeName: 'pk', KeyType: 'HASH' },
          { AttributeName: 'sk', KeyType: 'RANGE' },
        ],
        LocalSecondaryIndexes: [
          {
            IndexName: 'lsi1',
            KeySchema: [
              { AttributeName: 'pk', KeyType: 'HASH' },
              { AttributeName: 'lsiSk', KeyType: 'RANGE' },
            ],
            Projection: { ProjectionType: 'ALL' },
          },
        ],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )

    await waitUntilActive(name)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const lsis = desc.Table!.LocalSecondaryIndexes!
    expect(lsis).toHaveLength(1)
    expect(lsis[0].IndexName).toBe('lsi1')
    expect(lsis[0].KeySchema).toEqual([
      { AttributeName: 'pk', KeyType: 'HASH' },
      { AttributeName: 'lsiSk', KeyType: 'RANGE' },
    ])
  })

  it('rejects LSI on a hash-only table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_lsi_nork'),
          AttributeDefinitions: [
            { AttributeName: 'pk', AttributeType: 'S' },
            { AttributeName: 'lsiSk', AttributeType: 'N' },
          ],
          KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
          LocalSecondaryIndexes: [
            {
              IndexName: 'lsi1',
              KeySchema: [
                { AttributeName: 'pk', KeyType: 'HASH' },
                { AttributeName: 'lsiSk', KeyType: 'RANGE' },
              ],
              Projection: { ProjectionType: 'ALL' },
            },
          ],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      ),
      'ValidationException',
    )
  })

  it('rejects LSI with a hash key different from the base table', async () => {
    await expectDynamoError(
      () => ddb.send(
        new CreateTableCommand({
          TableName: uniqueTableName('ct_lsi_wronghash'),
          AttributeDefinitions: [
            { AttributeName: 'pk', AttributeType: 'S' },
            { AttributeName: 'sk', AttributeType: 'S' },
            { AttributeName: 'other', AttributeType: 'S' },
            { AttributeName: 'lsiSk', AttributeType: 'N' },
          ],
          KeySchema: [
            { AttributeName: 'pk', KeyType: 'HASH' },
            { AttributeName: 'sk', KeyType: 'RANGE' },
          ],
          LocalSecondaryIndexes: [
            {
              IndexName: 'lsi1',
              KeySchema: [
                { AttributeName: 'other', KeyType: 'HASH' },
                { AttributeName: 'lsiSk', KeyType: 'RANGE' },
              ],
              Projection: { ProjectionType: 'ALL' },
            },
          ],
          BillingMode: 'PAY_PER_REQUEST',
        }),
      ),
      'ValidationException',
    )
  })
})
