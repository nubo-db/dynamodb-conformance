import {
  PutItemCommand,
  ScanCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  compositeTableDef,
  cleanupItems,
  waitForGsiConsistency,
} from '../../../src/helpers.js'

describe('Scan — GSI', () => {
  const items = [
    {
      pk: { S: 'scan-gsi-1' },
      sk: { S: 'a' },
      lsi1sk: { S: 'scan-gsi-hash-A' },
      lsi2sk: { S: 'r1' },
      data: { S: 'val1' },
      extra: { S: 'should-not-appear-in-gsi2' },
    },
    {
      pk: { S: 'scan-gsi-2' },
      sk: { S: 'b' },
      lsi1sk: { S: 'scan-gsi-hash-A' },
      lsi2sk: { S: 'r2' },
      data: { S: 'val2' },
      extra: { S: 'also-not-in-gsi2' },
    },
    {
      pk: { S: 'scan-gsi-3' },
      sk: { S: 'c' },
      lsi1sk: { S: 'scan-gsi-hash-B' },
      lsi2sk: { S: 'r3' },
      data: { S: 'val3' },
      extra: { S: 'nope' },
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
    await waitForGsiConsistency({
      tableName: compositeTableDef.name,
      indexName: 'gsi1',
      partitionKey: { name: 'lsi1sk', value: { S: 'scan-gsi-hash-A' } },
      expectedCount: 2,
    })
  })

  afterAll(async () => {
    await cleanupItems(
      compositeTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('scans a GSI with ALL projection and returns all attributes', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        IndexName: 'gsi1',
        FilterExpression: 'begins_with(lsi1sk, :prefix)',
        ExpressionAttributeValues: { ':prefix': { S: 'scan-gsi-hash-' } },
      }),
    )

    expect(result.Items!.length).toBe(3)
    // ALL projection on gsi1 should include every attribute
    for (const item of result.Items!) {
      expect(item.pk).toBeDefined()
      expect(item.sk).toBeDefined()
      expect(item.lsi1sk).toBeDefined()
      expect(item.data).toBeDefined()
      expect(item.extra).toBeDefined()
    }
  })

  it('scans a GSI with INCLUDE projection and returns only projected attributes', async () => {
    // gsi2: INCLUDE with nonKeyAttributes ['data']
    // Keys: lsi1sk (HASH), lsi2sk (RANGE)
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        IndexName: 'gsi2',
        FilterExpression: 'begins_with(lsi1sk, :prefix)',
        ExpressionAttributeValues: { ':prefix': { S: 'scan-gsi-hash-' } },
      }),
    )

    expect(result.Items!.length).toBe(3)
    for (const item of result.Items!) {
      // GSI key attributes should be present
      expect(item.lsi1sk).toBeDefined()
      expect(item.lsi2sk).toBeDefined()
      // Included attribute
      expect(item.data).toBeDefined()
      // Not included: extra, pk, sk should be absent
      expect(item.extra).toBeUndefined()
    }
  })

  it('scans a GSI with FilterExpression', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        IndexName: 'gsi1',
        FilterExpression: 'lsi1sk = :v AND #d = :data',
        ExpressionAttributeNames: { '#d': 'data' },
        ExpressionAttributeValues: {
          ':v': { S: 'scan-gsi-hash-A' },
          ':data': { S: 'val1' },
        },
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].pk?.S).toBe('scan-gsi-1')
    expect(result.ScannedCount!).toBeGreaterThanOrEqual(result.Count!)
  })
})
