import {
  CreateTableCommand,
  DescribeTableCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { uniqueTableName, waitUntilActive, deleteTable } from '../../../src/helpers.js'

describe('CreateTable — GSI', () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('creates a table with a hash-only GSI', async () => {
    const name = uniqueTableName('ct_gsi_hash')
    tablesToCleanup.push(name)

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'pk', AttributeType: 'S' },
          { AttributeName: 'gsiPk', AttributeType: 'S' },
        ],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        GlobalSecondaryIndexes: [
          {
            IndexName: 'gsi1',
            KeySchema: [{ AttributeName: 'gsiPk', KeyType: 'HASH' }],
            Projection: { ProjectionType: 'ALL' },
          },
        ],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )

    await waitUntilActive(name)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const gsis = desc.Table!.GlobalSecondaryIndexes!
    expect(gsis).toHaveLength(1)
    expect(gsis[0].IndexName).toBe('gsi1')
    expect(gsis[0].KeySchema).toEqual([
      { AttributeName: 'gsiPk', KeyType: 'HASH' },
    ])
    expect(gsis[0].Projection?.ProjectionType).toBe('ALL')
  })

  it('creates a table with a composite GSI', async () => {
    const name = uniqueTableName('ct_gsi_comp')
    tablesToCleanup.push(name)

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'pk', AttributeType: 'S' },
          { AttributeName: 'gsiPk', AttributeType: 'S' },
          { AttributeName: 'gsiSk', AttributeType: 'N' },
        ],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        GlobalSecondaryIndexes: [
          {
            IndexName: 'gsi1',
            KeySchema: [
              { AttributeName: 'gsiPk', KeyType: 'HASH' },
              { AttributeName: 'gsiSk', KeyType: 'RANGE' },
            ],
            Projection: { ProjectionType: 'KEYS_ONLY' },
          },
        ],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )

    await waitUntilActive(name)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const gsis = desc.Table!.GlobalSecondaryIndexes!
    expect(gsis[0].Projection?.ProjectionType).toBe('KEYS_ONLY')
    expect(gsis[0].KeySchema).toHaveLength(2)
  })

  it('creates a table with INCLUDE projection on GSI', async () => {
    const name = uniqueTableName('ct_gsi_include')
    tablesToCleanup.push(name)

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'pk', AttributeType: 'S' },
          { AttributeName: 'gsiPk', AttributeType: 'S' },
        ],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        GlobalSecondaryIndexes: [
          {
            IndexName: 'gsi1',
            KeySchema: [{ AttributeName: 'gsiPk', KeyType: 'HASH' }],
            Projection: {
              ProjectionType: 'INCLUDE',
              NonKeyAttributes: ['attr1', 'attr2'],
            },
          },
        ],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )

    await waitUntilActive(name)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const gsi = desc.Table!.GlobalSecondaryIndexes![0]
    expect(gsi.Projection?.ProjectionType).toBe('INCLUDE')
    expect(gsi.Projection?.NonKeyAttributes).toEqual(
      expect.arrayContaining(['attr1', 'attr2']),
    )
  })

  it('creates a table with multiple GSIs', async () => {
    const name = uniqueTableName('ct_multi_gsi')
    tablesToCleanup.push(name)

    await ddb.send(
      new CreateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'pk', AttributeType: 'S' },
          { AttributeName: 'g1', AttributeType: 'S' },
          { AttributeName: 'g2', AttributeType: 'N' },
        ],
        KeySchema: [{ AttributeName: 'pk', KeyType: 'HASH' }],
        GlobalSecondaryIndexes: [
          {
            IndexName: 'gsi1',
            KeySchema: [{ AttributeName: 'g1', KeyType: 'HASH' }],
            Projection: { ProjectionType: 'ALL' },
          },
          {
            IndexName: 'gsi2',
            KeySchema: [{ AttributeName: 'g2', KeyType: 'HASH' }],
            Projection: { ProjectionType: 'KEYS_ONLY' },
          },
        ],
        BillingMode: 'PAY_PER_REQUEST',
      }),
    )

    await waitUntilActive(name)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(desc.Table!.GlobalSecondaryIndexes).toHaveLength(2)
  })
})
