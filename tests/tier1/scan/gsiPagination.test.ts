import {
  PutItemCommand,
  ScanCommand,
  type AttributeValue,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  uniqueTableName,
  createTable,
  deleteTable,
  waitForGsiConsistency,
} from '../../../src/helpers.js'
import type { TestTableDef } from '../../../src/types.js'

describe('Scan — GSI pagination', { tags: ['scan', 'data-plane', 'gsi'] }, () => {
  // Dedicated table: pk=ID(S), GSI on Type(S)+ID(S), ALL projection.
  // All items share the same GSI PK ("widget") to stress the cursor logic.
  const tableDef: TestTableDef = {
    name: uniqueTableName('gsi-page'),
    hashKey: { name: 'ID', type: 'S' },
    billingMode: 'PAY_PER_REQUEST',
    gsis: [
      {
        indexName: 'TypeIndex',
        hashKey: { name: 'Type', type: 'S' },
        rangeKey: { name: 'ID', type: 'S' },
        projectionType: 'ALL',
      },
    ],
  }

  const ITEM_COUNT = 50

  beforeAll(async () => {
    await createTable(tableDef)

    // Insert 50 items all with the same GSI PK ("widget")
    for (let i = 0; i < ITEM_COUNT; i++) {
      await ddb.send(
        new PutItemCommand({
          TableName: tableDef.name,
          Item: {
            ID: { S: `item-${String(i).padStart(3, '0')}` },
            Type: { S: 'widget' },
            Num: { N: String(i) },
          },
        }),
      )
    }

    await waitForGsiConsistency({
      tableName: tableDef.name,
      indexName: 'TypeIndex',
      partitionKey: { name: 'Type', value: { S: 'widget' } },
      expectedCount: ITEM_COUNT,
    })
  })

  afterAll(async () => {
    await deleteTable(tableDef.name)
  })

  it('returns all items across paginated GSI scan', async () => {
    const allItems: Record<string, AttributeValue>[] = []
    let exclusiveStartKey: Record<string, AttributeValue> | undefined
    let pages = 0

    do {
      const result = await ddb.send(
        new ScanCommand({
          TableName: tableDef.name,
          IndexName: 'TypeIndex',
          Limit: 10,
          ...(exclusiveStartKey
            ? { ExclusiveStartKey: exclusiveStartKey }
            : {}),
        }),
      )

      pages++
      if (result.Items) {
        allItems.push(...result.Items)
      }
      exclusiveStartKey = result.LastEvaluatedKey

      expect(pages).toBeLessThanOrEqual(10) // guard against infinite loops
    } while (exclusiveStartKey)

    expect(allItems).toHaveLength(ITEM_COUNT)
  })

  it('returns all matching items with filter across paginated GSI scan', async () => {
    // Every 5th item has Num divisible by 5 — but filters are applied
    // after Limit, so we need to paginate to get them all.
    const allItems: Record<string, AttributeValue>[] = []
    let exclusiveStartKey: Record<string, AttributeValue> | undefined
    let pages = 0

    do {
      const result = await ddb.send(
        new ScanCommand({
          TableName: tableDef.name,
          IndexName: 'TypeIndex',
          Limit: 10,
          FilterExpression: 'Num IN (:v0, :v1, :v2, :v3, :v4, :v5, :v6, :v7, :v8, :v9)',
          ExpressionAttributeValues: {
            ':v0': { N: '0' },
            ':v1': { N: '5' },
            ':v2': { N: '10' },
            ':v3': { N: '15' },
            ':v4': { N: '20' },
            ':v5': { N: '25' },
            ':v6': { N: '30' },
            ':v7': { N: '35' },
            ':v8': { N: '40' },
            ':v9': { N: '45' },
          },
          ...(exclusiveStartKey
            ? { ExclusiveStartKey: exclusiveStartKey }
            : {}),
        }),
      )

      pages++
      if (result.Items) {
        allItems.push(...result.Items)
      }
      exclusiveStartKey = result.LastEvaluatedKey

      expect(pages).toBeLessThanOrEqual(20)
    } while (exclusiveStartKey)

    expect(allItems).toHaveLength(10)
  })
})

describe('Scan — GSI pagination across tied sort keys', { tags: ['scan', 'data-plane', 'gsi'] }, () => {
  // GSI range key distinct from the base key, so the GSI sort key can tie while
  // base keys stay unique. Real AWS composes LastEvaluatedKey from the base key
  // AND the index keys; an emulator that omits the base key loops or drops rows.
  const tableDef: TestTableDef = {
    name: uniqueTableName('gsi-tie-scan'),
    hashKey: { name: 'ID', type: 'S' },
    billingMode: 'PAY_PER_REQUEST',
    gsis: [
      {
        indexName: 'TieIndex',
        hashKey: { name: 'GType', type: 'S' },
        rangeKey: { name: 'GSort', type: 'S' },
        projectionType: 'ALL',
      },
    ],
  }

  const COUNT = 5

  beforeAll(async () => {
    await createTable(tableDef)
    for (let i = 0; i < COUNT; i++) {
      await ddb.send(
        new PutItemCommand({
          TableName: tableDef.name,
          Item: { ID: { S: `id-${i}` }, GType: { S: 'tie' }, GSort: { S: 'same' } },
        }),
      )
    }
    await waitForGsiConsistency({
      tableName: tableDef.name,
      indexName: 'TieIndex',
      partitionKey: { name: 'GType', value: { S: 'tie' } },
      expectedCount: COUNT,
    })
  })

  afterAll(async () => {
    await deleteTable(tableDef.name)
  })

  it('walks every item once across a paged GSI scan with tied sort keys', async () => {
    const seen: string[] = []
    let lastKey: Record<string, AttributeValue> | undefined
    let pages = 0

    do {
      const page = await ddb.send(
        new ScanCommand({
          TableName: tableDef.name,
          IndexName: 'TieIndex',
          Limit: 1,
          ...(lastKey ? { ExclusiveStartKey: lastKey } : {}),
        }),
      )
      pages++
      for (const item of page.Items ?? []) seen.push(item.ID!.S!)
      lastKey = page.LastEvaluatedKey
      if (lastKey) {
        expect(Object.keys(lastKey).sort()).toEqual(['GSort', 'GType', 'ID'])
      }
      expect(pages).toBeLessThanOrEqual(COUNT + 1)
    } while (lastKey)

    expect(seen.sort()).toEqual(
      Array.from({ length: COUNT }, (_, i) => `id-${i}`).sort(),
    )
    expect(new Set(seen).size).toBe(COUNT)
  })
})
