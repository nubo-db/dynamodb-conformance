import {
  PutItemCommand,
  ScanCommand,
  DeleteItemCommand,
  type AttributeValue,
  type ScanCommandOutput,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Scan — basic', () => {
  const items = Array.from({ length: 5 }, (_, i) => ({
    pk: { S: `scan-basic-${i}` },
    val: { N: String(i) },
  }))

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      items.map((item) => ({ pk: item.pk })),
    )
  })

  it('returns items from the table', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        ConsistentRead: true,
      }),
    )

    // Table might have items from other tests, but our items should be there
    expect(result.Items!.length).toBeGreaterThanOrEqual(5)
    expect(result.Count).toBe(result.Items!.length)
    expect(result.ScannedCount).toBe(result.Items!.length)
  })
})

describe('Scan — FilterExpression', () => {
  const items = [
    { pk: { S: 'scan-filter-0' }, status: { S: 'active' } },
    { pk: { S: 'scan-filter-1' }, status: { S: 'inactive' } },
    { pk: { S: 'scan-filter-2' }, status: { S: 'active' } },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      items.map((item) => ({ pk: item.pk })),
    )
  })

  it('filter that removes all items returns Count=0 with ScannedCount>0', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: 'pk = :impossible',
        ExpressionAttributeValues: { ':impossible': { S: 'no-item-has-this-pk-value-xyz' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Count).toBe(0)
    expect(result.ScannedCount).toBeGreaterThan(0)
    expect(result.Items).toHaveLength(0)
  })

  it('filters scan results', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#s = :status AND begins_with(pk, :prefix)',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: {
          ':status': { S: 'active' },
          ':prefix': { S: 'scan-filter-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(2)
    // ScannedCount >= Count because filter reduces results
    expect(result.ScannedCount!).toBeGreaterThanOrEqual(result.Count!)
  })
})

describe('Scan — Limit and pagination', () => {
  const pageItems = Array.from({ length: 5 }, (_, i) => ({
    pk: { S: `scan-page-${i}` },
    val: { N: String(i) },
  }))

  beforeAll(async () => {
    await Promise.all(
      pageItems.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      pageItems.map((item) => ({ pk: item.pk })),
    )
  })

  it('respects Limit parameter', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        Limit: 2,
        ConsistentRead: true,
      }),
    )

    expect(result.Items!.length).toBeLessThanOrEqual(2)
    // With items in the table, we should get a LastEvaluatedKey
    if (result.Items!.length === 2) {
      expect(result.LastEvaluatedKey).toBeDefined()
    }
  })

  it('paginates through all items', async () => {
    const allItems: Record<string, AttributeValue>[] = []
    let lastKey: Record<string, AttributeValue> | undefined = undefined

    do {
      const page: ScanCommandOutput = await ddb.send(
        new ScanCommand({
          TableName: hashTableDef.name,
          Limit: 2,
          ExclusiveStartKey: lastKey,
          ConsistentRead: true,
        }),
      )
      allItems.push(...page.Items!)
      lastKey = page.LastEvaluatedKey
    } while (lastKey)

    // Should have gotten all items (at least our 5 + possibly others)
    expect(allItems.length).toBeGreaterThanOrEqual(5)
  })
})

describe('Scan — ProjectionExpression', () => {
  beforeAll(async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'scan-proj' },
          a: { S: 'alpha' },
          b: { S: 'beta' },
        },
      }),
    )
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'scan-proj' } }])
  })

  it('returns only projected attributes', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        ProjectionExpression: 'a',
        FilterExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'scan-proj' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].a.S).toBe('alpha')
    expect(result.Items![0].b).toBeUndefined()
    expect(result.Items![0].pk).toBeUndefined()
  })
})
