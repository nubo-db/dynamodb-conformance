import {
  PutItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, cleanupItems, waitForGsiConsistency, expectDynamoError } from '../../../src/helpers.js'

describe('Query — Select COUNT', () => {
  const pk = 'query-select-count'
  const items = [
    { pk: { S: pk }, sk: { S: 'a' }, status: { S: 'active' } },
    { pk: { S: pk }, sk: { S: 'b' }, status: { S: 'inactive' } },
    { pk: { S: pk }, sk: { S: 'c' }, status: { S: 'active' } },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: compositeTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      compositeTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('Select COUNT returns count without items', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        Select: 'COUNT',
        ConsistentRead: true,
      }),
    )

    expect(result.Count).toBeGreaterThan(0)
    expect(result.ScannedCount).toBe(result.Count)
    expect(result.Items).toBeUndefined()
  })

  it('Select COUNT with FilterExpression returns different Count and ScannedCount', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: '#s = :status',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':status': { S: 'active' },
        },
        Select: 'COUNT',
        ConsistentRead: true,
      }),
    )

    expect(result.Count).toBe(2)
    expect(result.ScannedCount).toBe(3)
    expect(result.Items).toBeUndefined()
  })
})

describe('Query — Select modes on indexes', () => {
  const pk = 'query-select-idx'
  const items = [
    {
      pk: { S: pk },
      sk: { S: 'a' },
      lsi1sk: { S: 'gsi-hash-1' },
      lsi2sk: { S: 'gsi-range-1' },
      data: { S: 'projected-data' },
      extra: { S: 'not-projected' },
    },
    {
      pk: { S: pk },
      sk: { S: 'b' },
      lsi1sk: { S: 'gsi-hash-1' },
      lsi2sk: { S: 'gsi-range-2' },
      data: { S: 'projected-data-2' },
      extra: { S: 'also-not-projected' },
    },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: compositeTableDef.name, Item: item }),
        ),
      ),
    )
    // Wait for GSI to reflect the items
    await waitForGsiConsistency({
      tableName: compositeTableDef.name,
      indexName: 'gsi2',
      partitionKey: { name: 'lsi1sk', value: { S: 'gsi-hash-1' } },
      expectedCount: 2,
    })
  })

  afterAll(async () => {
    await cleanupItems(
      compositeTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('Select ALL_PROJECTED_ATTRIBUTES on GSI returns only projected attributes', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'gsi2',
        KeyConditionExpression: '#hk = :hk',
        ExpressionAttributeNames: { '#hk': 'lsi1sk' },
        ExpressionAttributeValues: { ':hk': { S: 'gsi-hash-1' } },
        Select: 'ALL_PROJECTED_ATTRIBUTES',
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(2)
    for (const item of result.Items!) {
      // GSI2 is INCLUDE with nonKeyAttributes: ['data']
      // Should have: table keys (pk, sk), GSI keys (lsi1sk, lsi2sk), projected (data)
      expect(item.lsi1sk).toBeDefined()
      expect(item.lsi2sk).toBeDefined()
      expect(item.data).toBeDefined()
      // Should NOT have non-projected attributes
      expect(item.extra).toBeUndefined()
    }
  })

  it('Select ALL_ATTRIBUTES on LSI returns all base table attributes', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi1',
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: pk } },
        Select: 'ALL_ATTRIBUTES',
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(2)
    for (const item of result.Items!) {
      // LSI1 has projectionType ALL, so all base table attributes should be present
      expect(item.pk).toBeDefined()
      expect(item.sk).toBeDefined()
      expect(item.lsi1sk).toBeDefined()
      expect(item.data).toBeDefined()
      expect(item.extra).toBeDefined()
    }
  })
})

describe('Query — Select SPECIFIC_ATTRIBUTES', () => {
  const pk = 'query-select-specific'
  const items = [
    { pk: { S: pk }, sk: { S: 'a' }, data: { S: 'hello' }, extra: { S: 'world' } },
    { pk: { S: pk }, sk: { S: 'b' }, data: { S: 'foo' }, extra: { S: 'bar' } },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: compositeTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      compositeTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('Select SPECIFIC_ATTRIBUTES with ProjectionExpression succeeds', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk', '#sk': 'sk' },
        ExpressionAttributeValues: { ':pk': { S: pk } },
        ProjectionExpression: '#pk, #sk',
        Select: 'SPECIFIC_ATTRIBUTES',
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(2)
    for (const item of result.Items!) {
      expect(item.pk).toBeDefined()
      expect(item.sk).toBeDefined()
      expect(item.data).toBeUndefined()
      expect(item.extra).toBeUndefined()
    }
  })

  it('Select SPECIFIC_ATTRIBUTES without ProjectionExpression is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new QueryCommand({
            TableName: compositeTableDef.name,
            KeyConditionExpression: '#pk = :pk',
            ExpressionAttributeNames: { '#pk': 'pk' },
            ExpressionAttributeValues: { ':pk': { S: pk } },
            Select: 'SPECIFIC_ATTRIBUTES',
            ConsistentRead: true,
          }),
        ),
      'ValidationException',
    )
  })
})
