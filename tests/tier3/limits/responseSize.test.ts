import {
  PutItemCommand,
  QueryCommand,
  ScanCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  createTable,
  deleteTable,
} from '../../../src/helpers.js'
import type { TestTableDef } from '../../../src/types.js'

const hashDef: TestTableDef = {
  name: uniqueTableName('lim_resp_h'),
  hashKey: { name: 'pk', type: 'S' },
  billingMode: 'PAY_PER_REQUEST',
}

const compositeDef: TestTableDef = {
  name: uniqueTableName('lim_resp_c'),
  hashKey: { name: 'pk', type: 'S' },
  rangeKey: { name: 'sk', type: 'S' },
  billingMode: 'PAY_PER_REQUEST',
}

beforeAll(async () => {
  await Promise.all([createTable(hashDef), createTable(compositeDef)])
})

afterAll(async () => {
  await Promise.all([deleteTable(hashDef.name), deleteTable(compositeDef.name)])
})

describe('Response size limit (1MB)', () => {
  // Seed ~20 items of ~60KB each into the composite table under one partition key
  // so we can query them. Total ~1.2MB — should trigger pagination.
  const partitionKey = 'query-pk'

  beforeAll(async () => {
    for (let i = 0; i < 20; i++) {
      const sk = `sk-${String(i).padStart(3, '0')}`
      await ddb.send(
        new PutItemCommand({
          TableName: compositeDef.name,
          Item: {
            pk: { S: partitionKey },
            sk: { S: sk },
            payload: { S: 'x'.repeat(60_000) },
          },
        }),
      )
    }
  })

  it('Query returns LastEvaluatedKey when response would exceed 1MB', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeDef.name,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: partitionKey } },
        ConsistentRead: true,
      }),
    )

    // With 20 items of ~60KB each (~1.2MB), DynamoDB should paginate
    expect(result.LastEvaluatedKey).toBeDefined()
    // Should have returned some items but not all 20
    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBeGreaterThan(0)
    expect(result.Items!.length).toBeLessThan(20)
  })

  it('Scan returns LastEvaluatedKey when response would exceed 1MB', async () => {
    // Seed items into hash table for scanning
    for (let i = 0; i < 20; i++) {
      await ddb.send(
        new PutItemCommand({
          TableName: hashDef.name,
          Item: { pk: { S: `scan-${i}` }, payload: { S: 'y'.repeat(60_000) } },
        }),
      )
    }

    // Scan the whole table — with enough data, should paginate
    let totalItems = 0
    let pageCount = 0
    let lastKey: Record<string, any> | undefined

    do {
      const result = await ddb.send(
        new ScanCommand({
          TableName: hashDef.name,
          ConsistentRead: true,
          ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
        }),
      )
      totalItems += result.Items?.length ?? 0
      pageCount++
      lastKey = result.LastEvaluatedKey
    } while (lastKey)

    // We should have needed pagination (multiple pages)
    expect(totalItems).toBeGreaterThanOrEqual(20)
    expect(pageCount).toBeGreaterThanOrEqual(2)
  })

  it('single large item (under 400KB) is always returned', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashDef.name,
        Item: { pk: { S: 'single-big' }, payload: { S: 'z'.repeat(390_000) } },
      }),
    )

    const get = await ddb.send(
      new GetItemCommand({
        TableName: hashDef.name,
        Key: { pk: { S: 'single-big' } },
        ConsistentRead: true,
      }),
    )
    expect(get.Item).toBeDefined()
    expect(get.Item!.payload.S).toHaveLength(390_000)
  })

  it('multiple items totalling over 1MB get paginated via Query', async () => {
    // Use the same composite key partition we seeded above
    let allItems: any[] = []
    let lastKey: Record<string, any> | undefined

    do {
      const result = await ddb.send(
        new QueryCommand({
          TableName: compositeDef.name,
          KeyConditionExpression: '#pk = :pk',
          ExpressionAttributeNames: { '#pk': 'pk' },
          ExpressionAttributeValues: { ':pk': { S: partitionKey } },
          ConsistentRead: true,
          ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
        }),
      )
      allItems = allItems.concat(result.Items ?? [])
      lastKey = result.LastEvaluatedKey
    } while (lastKey)

    // After paginating through all pages, should get all 20 items
    expect(allItems).toHaveLength(20)
  })

  it('Limit parameter interacts with 1MB limit — whichever triggers first', async () => {
    // With Limit=5, should return exactly 5 items (Limit triggers before 1MB)
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeDef.name,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: partitionKey } },
        Limit: 5,
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(5)
    // Should still have LastEvaluatedKey since there are more items
    expect(result.LastEvaluatedKey).toBeDefined()
  })
})
