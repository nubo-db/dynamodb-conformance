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

describe('Scan — GSI pagination', () => {
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
  }, 30_000)

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
