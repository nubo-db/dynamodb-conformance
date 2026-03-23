import {
  CreateTableCommand,
  UpdateTableCommand,
  DescribeTableCommand,
  PutItemCommand,
  QueryCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  waitUntilActive,
  deleteTable,
  expectDynamoError,
  waitForGsiConsistency,
} from '../../../src/helpers.js'

/** Create a base table with pk (S) + sk (S) for GSI lifecycle tests */
async function createBaseTable(name: string): Promise<void> {
  await ddb.send(
    new CreateTableCommand({
      TableName: name,
      AttributeDefinitions: [
        { AttributeName: 'pk', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'pk', KeyType: 'HASH' },
        { AttributeName: 'sk', KeyType: 'RANGE' },
      ],
      BillingMode: 'PAY_PER_REQUEST',
    }),
  )
  await waitUntilActive(name)
}

/** Create a base table that already has a GSI (for remove/validation tests) */
async function createTableWithGsi(name: string): Promise<void> {
  await ddb.send(
    new CreateTableCommand({
      TableName: name,
      AttributeDefinitions: [
        { AttributeName: 'pk', AttributeType: 'S' },
        { AttributeName: 'sk', AttributeType: 'S' },
        { AttributeName: 'gsiPk', AttributeType: 'S' },
      ],
      KeySchema: [
        { AttributeName: 'pk', KeyType: 'HASH' },
        { AttributeName: 'sk', KeyType: 'RANGE' },
      ],
      GlobalSecondaryIndexes: [
        {
          IndexName: 'existingGsi',
          KeySchema: [{ AttributeName: 'gsiPk', KeyType: 'HASH' }],
          Projection: { ProjectionType: 'ALL' },
        },
      ],
      BillingMode: 'PAY_PER_REQUEST',
    }),
  )
  await waitUntilActive(name)
}

describe('UpdateTable — add GSI', () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('adds a hash-only GSI to an existing table', { timeout: 1_260_000 }, async () => {
    const name = uniqueTableName('ut_gsi_add_hash')
    tablesToCleanup.push(name)
    await createBaseTable(name)

    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'gsiPk', AttributeType: 'S' },
        ],
        GlobalSecondaryIndexUpdates: [
          {
            Create: {
              IndexName: 'gsi1',
              KeySchema: [{ AttributeName: 'gsiPk', KeyType: 'HASH' }],
              Projection: { ProjectionType: 'ALL' },
            },
          },
        ],
      }),
    )

    await waitUntilActive(name, 1_200_000)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const gsis = desc.Table!.GlobalSecondaryIndexes!
    expect(gsis).toHaveLength(1)
    expect(gsis[0].IndexName).toBe('gsi1')
    expect(gsis[0].IndexStatus).toBe('ACTIVE')
    expect(gsis[0].KeySchema).toEqual([
      { AttributeName: 'gsiPk', KeyType: 'HASH' },
    ])
    expect(gsis[0].Projection?.ProjectionType).toBe('ALL')
  })

  it('adds a composite GSI (hash + range) to an existing table', { timeout: 1_260_000 }, async () => {
    const name = uniqueTableName('ut_gsi_add_comp')
    tablesToCleanup.push(name)
    await createBaseTable(name)

    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'gsiPk', AttributeType: 'S' },
          { AttributeName: 'gsiSk', AttributeType: 'N' },
        ],
        GlobalSecondaryIndexUpdates: [
          {
            Create: {
              IndexName: 'gsiComposite',
              KeySchema: [
                { AttributeName: 'gsiPk', KeyType: 'HASH' },
                { AttributeName: 'gsiSk', KeyType: 'RANGE' },
              ],
              Projection: { ProjectionType: 'ALL' },
            },
          },
        ],
      }),
    )

    await waitUntilActive(name, 1_200_000)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const gsis = desc.Table!.GlobalSecondaryIndexes!
    expect(gsis).toHaveLength(1)
    expect(gsis[0].IndexName).toBe('gsiComposite')
    expect(gsis[0].KeySchema).toEqual([
      { AttributeName: 'gsiPk', KeyType: 'HASH' },
      { AttributeName: 'gsiSk', KeyType: 'RANGE' },
    ])
  })

  it('adds a GSI with KEYS_ONLY projection', { timeout: 1_260_000 }, async () => {
    const name = uniqueTableName('ut_gsi_keys_only')
    tablesToCleanup.push(name)
    await createBaseTable(name)

    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'gsiPk', AttributeType: 'S' },
        ],
        GlobalSecondaryIndexUpdates: [
          {
            Create: {
              IndexName: 'gsiKeysOnly',
              KeySchema: [{ AttributeName: 'gsiPk', KeyType: 'HASH' }],
              Projection: { ProjectionType: 'KEYS_ONLY' },
            },
          },
        ],
      }),
    )

    await waitUntilActive(name, 1_200_000)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const gsi = desc.Table!.GlobalSecondaryIndexes![0]
    expect(gsi.IndexName).toBe('gsiKeysOnly')
    expect(gsi.Projection?.ProjectionType).toBe('KEYS_ONLY')
  })

  it('adds a GSI with INCLUDE projection and NonKeyAttributes', { timeout: 1_260_000 }, async () => {
    const name = uniqueTableName('ut_gsi_include')
    tablesToCleanup.push(name)
    await createBaseTable(name)

    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'gsiPk', AttributeType: 'S' },
        ],
        GlobalSecondaryIndexUpdates: [
          {
            Create: {
              IndexName: 'gsiInclude',
              KeySchema: [{ AttributeName: 'gsiPk', KeyType: 'HASH' }],
              Projection: {
                ProjectionType: 'INCLUDE',
                NonKeyAttributes: ['attr1', 'attr2'],
              },
            },
          },
        ],
      }),
    )

    await waitUntilActive(name, 1_200_000)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const gsi = desc.Table!.GlobalSecondaryIndexes![0]
    expect(gsi.IndexName).toBe('gsiInclude')
    expect(gsi.Projection?.ProjectionType).toBe('INCLUDE')
    expect(gsi.Projection?.NonKeyAttributes).toEqual(
      expect.arrayContaining(['attr1', 'attr2']),
    )
  })

  it('can query a newly created GSI after putting items', { timeout: 1_260_000 }, async () => {
    const name = uniqueTableName('ut_gsi_query')
    tablesToCleanup.push(name)
    await createBaseTable(name)

    // Put items before adding the GSI
    await ddb.send(
      new PutItemCommand({
        TableName: name,
        Item: {
          pk: { S: 'item1' },
          sk: { S: 'a' },
          gsiPk: { S: 'group1' },
          data: { S: 'hello' },
        },
      }),
    )
    await ddb.send(
      new PutItemCommand({
        TableName: name,
        Item: {
          pk: { S: 'item2' },
          sk: { S: 'b' },
          gsiPk: { S: 'group1' },
          data: { S: 'world' },
        },
      }),
    )
    await ddb.send(
      new PutItemCommand({
        TableName: name,
        Item: {
          pk: { S: 'item3' },
          sk: { S: 'c' },
          gsiPk: { S: 'group2' },
          data: { S: 'other' },
        },
      }),
    )

    // Add the GSI
    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'gsiPk', AttributeType: 'S' },
        ],
        GlobalSecondaryIndexUpdates: [
          {
            Create: {
              IndexName: 'gsiQuery',
              KeySchema: [{ AttributeName: 'gsiPk', KeyType: 'HASH' }],
              Projection: { ProjectionType: 'ALL' },
            },
          },
        ],
      }),
    )

    await waitUntilActive(name, 1_200_000)

    // Wait for the GSI to backfill existing items
    await waitForGsiConsistency({
      tableName: name,
      indexName: 'gsiQuery',
      partitionKey: { name: 'gsiPk', value: { S: 'group1' } },
      expectedCount: 2,
      timeoutMs: 30_000,
    })

    // Query the GSI
    const res = await ddb.send(
      new QueryCommand({
        TableName: name,
        IndexName: 'gsiQuery',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'gsiPk' },
        ExpressionAttributeValues: { ':pk': { S: 'group1' } },
      }),
    )

    expect(res.Count).toBe(2)
    const pks = res.Items!.map((item) => item.pk.S).sort()
    expect(pks).toEqual(['item1', 'item2'])
  })

  it('adds multiple GSIs sequentially', { timeout: 2_520_000 }, async () => {
    const name = uniqueTableName('ut_gsi_multi')
    tablesToCleanup.push(name)
    await createBaseTable(name)

    // Add first GSI
    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'g1', AttributeType: 'S' },
        ],
        GlobalSecondaryIndexUpdates: [
          {
            Create: {
              IndexName: 'gsi1',
              KeySchema: [{ AttributeName: 'g1', KeyType: 'HASH' }],
              Projection: { ProjectionType: 'ALL' },
            },
          },
        ],
      }),
    )

    await waitUntilActive(name, 1_200_000)

    // Add second GSI
    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        AttributeDefinitions: [
          { AttributeName: 'g2', AttributeType: 'N' },
        ],
        GlobalSecondaryIndexUpdates: [
          {
            Create: {
              IndexName: 'gsi2',
              KeySchema: [{ AttributeName: 'g2', KeyType: 'HASH' }],
              Projection: { ProjectionType: 'KEYS_ONLY' },
            },
          },
        ],
      }),
    )

    await waitUntilActive(name, 1_200_000)

    const desc = await ddb.send(new DescribeTableCommand({ TableName: name }))
    const gsis = desc.Table!.GlobalSecondaryIndexes!
    expect(gsis).toHaveLength(2)
    const names = gsis.map((g) => g.IndexName).sort()
    expect(names).toEqual(['gsi1', 'gsi2'])
    expect(gsis.every((g) => g.IndexStatus === 'ACTIVE')).toBe(true)
  })
})

describe('UpdateTable — remove GSI', () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('removes a GSI from a table', { timeout: 1_260_000 }, async () => {
    const name = uniqueTableName('ut_gsi_remove')
    tablesToCleanup.push(name)
    await createTableWithGsi(name)

    // Verify the GSI exists before removal
    const before = await ddb.send(new DescribeTableCommand({ TableName: name }))
    expect(before.Table!.GlobalSecondaryIndexes).toHaveLength(1)

    // Remove the GSI
    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        GlobalSecondaryIndexUpdates: [
          {
            Delete: { IndexName: 'existingGsi' },
          },
        ],
      }),
    )

    await waitUntilActive(name, 1_200_000)

    const after = await ddb.send(new DescribeTableCommand({ TableName: name }))
    // GSI list should be empty or undefined
    const gsis = after.Table!.GlobalSecondaryIndexes ?? []
    expect(gsis).toHaveLength(0)
  })

  it('base table operations still work after removing a GSI', { timeout: 1_260_000 }, async () => {
    const name = uniqueTableName('ut_gsi_rm_ops')
    tablesToCleanup.push(name)
    await createTableWithGsi(name)

    // Remove the GSI
    await ddb.send(
      new UpdateTableCommand({
        TableName: name,
        GlobalSecondaryIndexUpdates: [
          {
            Delete: { IndexName: 'existingGsi' },
          },
        ],
      }),
    )

    await waitUntilActive(name, 1_200_000)

    // Put an item — should still work
    await ddb.send(
      new PutItemCommand({
        TableName: name,
        Item: {
          pk: { S: 'afterRemove' },
          sk: { S: '1' },
          data: { S: 'still works' },
        },
      }),
    )

    // Get the item back
    const res = await ddb.send(
      new GetItemCommand({
        TableName: name,
        Key: {
          pk: { S: 'afterRemove' },
          sk: { S: '1' },
        },
      }),
    )

    expect(res.Item).toBeDefined()
    expect(res.Item!.data.S).toBe('still works')

    // Query the base table
    const queryRes = await ddb.send(
      new QueryCommand({
        TableName: name,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: 'afterRemove' } },
      }),
    )

    expect(queryRes.Count).toBe(1)
  })
})

describe('UpdateTable — GSI validation', () => {
  const tablesToCleanup: string[] = []

  afterAll(async () => {
    await Promise.all(tablesToCleanup.map(deleteTable))
  })

  it('rejects adding a GSI with a name that already exists', { timeout: 1_260_000 }, async () => {
    const name = uniqueTableName('ut_gsi_dup')
    tablesToCleanup.push(name)
    await createTableWithGsi(name)

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateTableCommand({
            TableName: name,
            AttributeDefinitions: [
              { AttributeName: 'gsiPk', AttributeType: 'S' },
            ],
            GlobalSecondaryIndexUpdates: [
              {
                Create: {
                  IndexName: 'existingGsi',
                  KeySchema: [{ AttributeName: 'gsiPk', KeyType: 'HASH' }],
                  Projection: { ProjectionType: 'ALL' },
                },
              },
            ],
          }),
        ),
      'ValidationException',
    )
  })

  it('rejects adding a GSI with an attribute not in AttributeDefinitions', async () => {
    const name = uniqueTableName('ut_gsi_no_attr')
    tablesToCleanup.push(name)
    await createBaseTable(name)

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateTableCommand({
            TableName: name,
            // Intentionally omitting AttributeDefinitions for 'unknownAttr'
            GlobalSecondaryIndexUpdates: [
              {
                Create: {
                  IndexName: 'gsiBadAttr',
                  KeySchema: [{ AttributeName: 'unknownAttr', KeyType: 'HASH' }],
                  Projection: { ProjectionType: 'ALL' },
                },
              },
            ],
          }),
        ),
      'ValidationException',
    )
  })

  it('rejects removing a non-existent GSI', async () => {
    const name = uniqueTableName('ut_gsi_rm_none')
    tablesToCleanup.push(name)
    await createBaseTable(name)

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateTableCommand({
            TableName: name,
            GlobalSecondaryIndexUpdates: [
              {
                Delete: { IndexName: 'doesNotExist' },
              },
            ],
          }),
        ),
      'ResourceNotFoundException',
    )
  })
})
