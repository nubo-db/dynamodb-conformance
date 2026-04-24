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
} from '../../../src/helpers.js'
import type { TestTableDef } from '../../../src/types.js'

// Companion to gsiPagination.test.ts (from PR #1). Exercises the LSI-scan
// cursor advancement when many items share the same base-table partition and
// the LSI sort key has duplicates across items — the 4-tuple cursor
// (base_pk, lsi_sk, base_sk) must advance correctly across pages.
describe('Scan — LSI pagination under duplicate cursor components', () => {
  const tableDef: TestTableDef = {
    name: uniqueTableName('lsi-page'),
    hashKey: { name: 'ID', type: 'S' },
    rangeKey: { name: 'SK', type: 'S' },
    billingMode: 'PAY_PER_REQUEST',
    lsis: [
      {
        indexName: 'LsiIndex',
        rangeKey: { name: 'Lsk', type: 'S' },
        projectionType: 'ALL',
      },
    ],
  }

  const ITEM_COUNT = 50

  beforeAll(async () => {
    await createTable(tableDef)

    // All items share ID='widget' so they live in one LSI partition.
    // Lsk cycles through 5 group values so the LSI sort key has duplicates;
    // SK is unique so the 4-tuple cursor is only disambiguated by the base
    // sort key. LSIs are strongly consistent so no readiness wait is needed.
    for (let i = 0; i < ITEM_COUNT; i++) {
      await ddb.send(
        new PutItemCommand({
          TableName: tableDef.name,
          Item: {
            ID: { S: 'widget' },
            SK: { S: `item-${String(i).padStart(3, '0')}` },
            Lsk: { S: `group-${i % 5}` },
            Num: { N: String(i) },
          },
        }),
      )
    }
  }, 30_000)

  afterAll(async () => {
    await deleteTable(tableDef.name)
  })

  it('returns all items across paginated LSI scan', async () => {
    const allItems: Record<string, AttributeValue>[] = []
    let exclusiveStartKey: Record<string, AttributeValue> | undefined
    let pages = 0

    do {
      const result = await ddb.send(
        new ScanCommand({
          TableName: tableDef.name,
          IndexName: 'LsiIndex',
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

      expect(pages).toBeLessThanOrEqual(10)
    } while (exclusiveStartKey)

    expect(allItems).toHaveLength(ITEM_COUNT)
  })

  it('returns all matching items with filter across paginated LSI scan', async () => {
    // Items 0, 5, 10, 15, 20, 25, 30, 35, 40, 45 match — 10 items across pages.
    // Filter is applied after Limit so pagination must not stop on a short page.
    const allItems: Record<string, AttributeValue>[] = []
    let exclusiveStartKey: Record<string, AttributeValue> | undefined
    let pages = 0

    do {
      const result = await ddb.send(
        new ScanCommand({
          TableName: tableDef.name,
          IndexName: 'LsiIndex',
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
