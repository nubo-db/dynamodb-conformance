import {
  PutItemCommand,
  GetItemCommand,
  QueryCommand,
  ScanCommand,
  BatchGetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  cleanupItems,
  expectDynamoError,
} from '../../../src/helpers.js'

describe('Legacy API — AttributesToGet (legacy ProjectionExpression)', () => {
  const hashKeys = [
    { pk: { S: 'a2g-1' } },
    { pk: { S: 'a2g-2' } },
    { pk: { S: 'a2g-3' } },
  ]

  const compositeKeys = [
    { pk: { S: 'a2g-comp' }, sk: { S: '1' } },
    { pk: { S: 'a2g-comp' }, sk: { S: '2' } },
    { pk: { S: 'a2g-comp' }, sk: { S: '3' } },
  ]

  beforeAll(async () => {
    await Promise.all([
      ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'a2g-1' }, name: { S: 'alice' }, age: { N: '30' }, role: { S: 'admin' } },
        }),
      ),
      ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'a2g-2' }, name: { S: 'bob' }, age: { N: '25' }, role: { S: 'user' } },
        }),
      ),
      ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'a2g-3' }, name: { S: 'charlie' }, age: { N: '35' }, role: { S: 'user' } },
        }),
      ),
      ...compositeKeys.map((key, i) =>
        ddb.send(
          new PutItemCommand({
            TableName: compositeTableDef.name,
            Item: { ...key, name: { S: `item-${i}` }, data: { S: `data-${i}` } },
          }),
        ),
      ),
    ])
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, hashKeys)
    await cleanupItems(compositeTableDef.name, compositeKeys)
  })

  it('GetItem with AttributesToGet returns only specified attributes', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'a2g-1' } },
        AttributesToGet: ['name', 'age'],
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.name.S).toBe('alice')
    expect(result.Item!.age.N).toBe('30')
    expect(result.Item!.role).toBeUndefined()
  })

  it('GetItem with AttributesToGet does NOT auto-include key attributes', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'a2g-1' } },
        AttributesToGet: ['name'],
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.name.S).toBe('alice')
    expect(result.Item!.pk).toBeUndefined()
  })

  it('Query with AttributesToGet returns only specified attributes', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditions: {
          pk: {
            ComparisonOperator: 'EQ',
            AttributeValueList: [{ S: 'a2g-comp' }],
          },
        },
        AttributesToGet: ['sk', 'name'],
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(3)
    for (const item of result.Items!) {
      expect(item.sk).toBeDefined()
      expect(item.name).toBeDefined()
      expect(item.data).toBeUndefined()
      expect(item.pk).toBeUndefined()
    }
  })

  it('Scan with AttributesToGet returns only specified attributes', async () => {
    // Scan the hash table and filter to our test items
    const allItems: Record<string, any>[] = []
    let lastKey: Record<string, any> | undefined

    do {
      const result = await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          AttributesToGet: ['pk', 'name'],
          ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
        }),
      )
      const matching = (result.Items ?? []).filter((i) => i.pk?.S?.startsWith('a2g-'))
      allItems.push(...matching)
      lastKey = result.LastEvaluatedKey
    } while (lastKey)

    expect(allItems.length).toBeGreaterThanOrEqual(3)
    for (const item of allItems) {
      expect(item.pk).toBeDefined()
      expect(item.name).toBeDefined()
      expect(item.age).toBeUndefined()
      expect(item.role).toBeUndefined()
    }
  })

  it('BatchGetItem with AttributesToGet returns only specified attributes', async () => {
    const result = await ddb.send(
      new BatchGetItemCommand({
        RequestItems: {
          [hashTableDef.name]: {
            Keys: [{ pk: { S: 'a2g-1' } }, { pk: { S: 'a2g-2' } }],
            AttributesToGet: ['pk', 'role'],
            ConsistentRead: true,
          },
        },
      }),
    )

    const items = result.Responses![hashTableDef.name]
    expect(items).toHaveLength(2)
    for (const item of items) {
      expect(item.pk).toBeDefined()
      expect(item.role).toBeDefined()
      expect(item.name).toBeUndefined()
      expect(item.age).toBeUndefined()
    }
  })

  it('Mixing AttributesToGet with ProjectionExpression throws ValidationException', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new GetItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'a2g-1' } },
            AttributesToGet: ['name'],
            ProjectionExpression: '#n',
            ExpressionAttributeNames: { '#n': 'name' },
          }),
        ),
      'ValidationException',
    )
  })

  it('AttributesToGet with single attribute', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'a2g-1' } },
        AttributesToGet: ['role'],
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.role.S).toBe('admin')
    expect(Object.keys(result.Item!)).toEqual(['role'])
  })

  it('AttributesToGet with non-existent attribute returns item without that attr', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'a2g-1' } },
        AttributesToGet: ['name', 'nonexistent'],
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.name.S).toBe('alice')
    expect(result.Item!.nonexistent).toBeUndefined()
  })
})
