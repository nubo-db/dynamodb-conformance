import {
  PutItemCommand,
  QueryCommand,
  DeleteItemCommand,
  type AttributeValue,
  type QueryCommandOutput,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Query — basic', () => {
  const pk = 'query-basic'
  const items = [
    { pk: { S: pk }, sk: { S: 'a' }, val: { N: '1' } },
    { pk: { S: pk }, sk: { S: 'b' }, val: { N: '2' } },
    { pk: { S: pk }, sk: { S: 'c' }, val: { N: '3' } },
    { pk: { S: pk }, sk: { S: 'd' }, val: { N: '4' } },
    { pk: { S: pk }, sk: { S: 'e' }, val: { N: '5' } },
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

  it('returns all items for a partition key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(5)
    expect(result.Count).toBe(5)
    expect(result.ScannedCount).toBe(5)
  })

  it('returns items in ascending sort key order by default', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => i.sk.S)
    expect(sortKeys).toEqual(['a', 'b', 'c', 'd', 'e'])
  })

  it('returns items in descending order with ScanIndexForward=false', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        ScanIndexForward: false,
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => i.sk.S)
    expect(sortKeys).toEqual(['e', 'd', 'c', 'b', 'a'])
  })

  it('returns empty results for a non-existent partition key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'nonexistent-pk' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(0)
    expect(result.Count).toBe(0)
  })
})

describe('Query — key conditions', () => {
  const pk = 'query-keycond'
  const items = [
    { pk: { S: pk }, sk: { S: 'alpha' } },
    { pk: { S: pk }, sk: { S: 'beta' } },
    { pk: { S: pk }, sk: { S: 'gamma' } },
    { pk: { S: pk }, sk: { S: 'delta' } },
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

  it('supports begins_with on sort key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk AND begins_with(sk, :prefix)',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':prefix': { S: 'b' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].sk.S).toBe('beta')
  })

  it('supports BETWEEN on sort key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk AND sk BETWEEN :lo AND :hi',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':lo': { S: 'beta' },
          ':hi': { S: 'delta' },
        },
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => i.sk.S)
    expect(sortKeys).toEqual(['beta', 'delta'])
  })

  it('supports < on sort key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk AND sk < :val',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':val': { S: 'c' },
        },
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => i.sk.S)
    expect(sortKeys).toEqual(['alpha', 'beta'])
  })

  it('supports >= on sort key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk AND sk >= :val',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':val': { S: 'delta' },
        },
        ConsistentRead: true,
      }),
    )

    const sortKeys = result.Items!.map((i) => i.sk.S)
    expect(sortKeys).toEqual(['delta', 'gamma'])
  })
})

describe('Query — FilterExpression', () => {
  const pk = 'query-filter'
  const items = [
    { pk: { S: pk }, sk: { S: '1' }, status: { S: 'active' } },
    { pk: { S: pk }, sk: { S: '2' }, status: { S: 'inactive' } },
    { pk: { S: pk }, sk: { S: '3' }, status: { S: 'active' } },
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

  it('filters results but ScannedCount reflects pre-filter count', async () => {
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
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(2)
    expect(result.Count).toBe(2)
    expect(result.ScannedCount).toBe(3) // all 3 items were scanned
  })
})

describe('Query — Limit and pagination', () => {
  const pk = 'query-page'
  const items = Array.from({ length: 10 }, (_, i) => ({
    pk: { S: pk },
    sk: { S: String(i).padStart(3, '0') },
  }))

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

  it('respects Limit parameter', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        Limit: 3,
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(3)
    expect(result.LastEvaluatedKey).toBeDefined()
  })

  it('paginates through all items', async () => {
    const allItems: Record<string, AttributeValue>[] = []
    let lastKey: Record<string, AttributeValue> | undefined = undefined

    do {
      const page: QueryCommandOutput = await ddb.send(
        new QueryCommand({
          TableName: compositeTableDef.name,
          KeyConditionExpression: 'pk = :pk',
          ExpressionAttributeValues: { ':pk': { S: pk } },
          Limit: 4,
          ExclusiveStartKey: lastKey,
          ConsistentRead: true,
        }),
      )
      allItems.push(...page.Items!)
      lastKey = page.LastEvaluatedKey
    } while (lastKey)

    expect(allItems).toHaveLength(10)
  })

  it('returns no LastEvaluatedKey when all items fit in one page', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(10)
    expect(result.LastEvaluatedKey).toBeUndefined()
  })
})

describe('Query — Limit + FilterExpression interaction', () => {
  const pk = 'query-limit-filter'
  const items = Array.from({ length: 10 }, (_, i) => ({
    pk: { S: pk },
    sk: { S: String(i).padStart(3, '0') },
    category: { S: i % 2 === 0 ? 'even' : 'odd' },
  }))

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

  it('Limit applies to scanned items, not filtered items', async () => {
    // With 10 items in the partition and a filter that matches ~5 (even category),
    // Limit=3 should scan 3 items and return however many match the filter (0-3)
    // ScannedCount should equal Limit (3), Count <= ScannedCount
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        FilterExpression: '#cat = :cat',
        ExpressionAttributeNames: { '#pk': 'pk', '#cat': 'category' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':cat': { S: 'even' },
        },
        Limit: 3,
        ConsistentRead: true,
      }),
    )

    expect(result.ScannedCount).toBe(3)
    expect(result.Count).toBeLessThanOrEqual(3)
    expect(result.Count).toBe(result.Items!.length)
    expect(result.LastEvaluatedKey).toBeDefined()
  })

  it('Limit with filter may return fewer items than Limit', async () => {
    // Limit=2, scanning items 000 (even) and 001 (odd)
    // Returns 1 item but ScannedCount=2, with LastEvaluatedKey set
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        FilterExpression: '#cat = :cat',
        ExpressionAttributeNames: { '#pk': 'pk', '#cat': 'category' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':cat': { S: 'even' },
        },
        Limit: 2,
        ConsistentRead: true,
      }),
    )

    expect(result.ScannedCount).toBe(2)
    // First 2 items scanned: sk=000 (even, matches) and sk=001 (odd, no match)
    expect(result.Count).toBe(1)
    expect(result.Items).toHaveLength(1)
    expect(result.LastEvaluatedKey).toBeDefined()
  })
})
