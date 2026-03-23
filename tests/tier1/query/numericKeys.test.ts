import {
  PutItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeNTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Query — numeric sort key', () => {
  const pk = 'numq'
  const skValues = [1, 5, 10, 20, 50, 100]
  const items = skValues.map((n) => ({
    pk: { S: pk },
    sk: { N: String(n) },
    val: { S: `item-${n}` },
  }))

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: compositeNTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      compositeNTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('returns items in ascending numeric order', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeNTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(6)
    const sortKeys = result.Items!.map((i) => Number(i.sk.N))
    expect(sortKeys).toEqual([1, 5, 10, 20, 50, 100])
  })

  it('returns items in descending numeric order with ScanIndexForward=false', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeNTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        ScanIndexForward: false,
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => Number(i.sk.N))
    expect(sortKeys).toEqual([100, 50, 20, 10, 5, 1])
  })

  it('supports > comparison on numeric sort key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeNTableDef.name,
        KeyConditionExpression: 'pk = :pk AND sk > :val',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':val': { N: '15' },
        },
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => Number(i.sk.N))
    expect(sortKeys).toEqual([20, 50, 100])
  })

  it('supports BETWEEN on numeric sort key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeNTableDef.name,
        KeyConditionExpression: 'pk = :pk AND sk BETWEEN :lo AND :hi',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':lo': { N: '5' },
          ':hi': { N: '50' },
        },
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => Number(i.sk.N))
    expect(sortKeys).toEqual([5, 10, 20, 50])
  })

  it('respects Limit on numeric sort key query', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeNTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        Limit: 3,
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(3)
    const sortKeys = result.Items!.map((i) => Number(i.sk.N))
    expect(sortKeys).toEqual([1, 5, 10])
    expect(result.LastEvaluatedKey).toBeDefined()
  })

  it('orders numerically, not lexicographically (10 comes after 5, not after 1)', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeNTableDef.name,
        KeyConditionExpression: 'pk = :pk AND sk BETWEEN :lo AND :hi',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':lo': { N: '1' },
          ':hi': { N: '10' },
        },
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => Number(i.sk.N))
    // Lexicographic order would be [1, 10, 5]; numeric order is [1, 5, 10]
    expect(sortKeys).toEqual([1, 5, 10])
  })
})

describe('Query — numeric sort key with negatives and zero', () => {
  const pk = 'numq-neg'
  const skValues = [-100, -5, -1, 0, 1, 5, 10, 100]
  const items = skValues.map((n) => ({
    pk: { S: pk },
    sk: { N: String(n) },
    val: { S: `item-${n}` },
  }))

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: compositeNTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      compositeNTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('numeric sort key orders correctly across negative, zero, and positive', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeNTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: pk } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(8)
    const sortKeys = result.Items!.map((i) => Number(i.sk.N))
    expect(sortKeys).toEqual([-100, -5, -1, 0, 1, 5, 10, 100])
  })

  it('numeric sort key with >= on negative value', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeNTableDef.name,
        KeyConditionExpression: '#pk = :pk AND sk >= :val',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':val': { N: '-1' },
        },
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => Number(i.sk.N))
    expect(sortKeys).toEqual([-1, 0, 1, 5, 10, 100])
  })
})
