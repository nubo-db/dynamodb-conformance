import {
  PutItemCommand,
  ScanCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  compositeTableDef,
  cleanupItems,
} from '../../../src/helpers.js'

describe('Scan — LSI', () => {
  const items = [
    {
      pk: { S: 'scan-lsi-1' },
      sk: { S: 'a' },
      lsi1sk: { S: 'sort-a' },
      lsi2sk: { S: 'r-a' },
      data: { S: 'val1' },
    },
    {
      pk: { S: 'scan-lsi-1' },
      sk: { S: 'b' },
      lsi1sk: { S: 'sort-b' },
      lsi2sk: { S: 'r-b' },
      data: { S: 'val2' },
    },
    {
      pk: { S: 'scan-lsi-2' },
      sk: { S: 'a' },
      lsi1sk: { S: 'sort-c' },
      lsi2sk: { S: 'r-c' },
      data: { S: 'val3' },
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
  })

  afterAll(async () => {
    await cleanupItems(
      compositeTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('scans an LSI and returns items', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi1',
        FilterExpression: 'begins_with(pk, :prefix)',
        ExpressionAttributeValues: { ':prefix': { S: 'scan-lsi-' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items!.length).toBe(3)
    // lsi1 has ALL projection, so all attributes should be present
    for (const item of result.Items!) {
      expect(item.pk).toBeDefined()
      expect(item.sk).toBeDefined()
      expect(item.lsi1sk).toBeDefined()
      expect(item.data).toBeDefined()
    }
  })

  it('scans an LSI with FilterExpression', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi1',
        FilterExpression: 'pk = :pk AND #d = :data',
        ExpressionAttributeNames: { '#d': 'data' },
        ExpressionAttributeValues: {
          ':pk': { S: 'scan-lsi-1' },
          ':data': { S: 'val1' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].lsi1sk?.S).toBe('sort-a')
    expect(result.ScannedCount!).toBeGreaterThanOrEqual(result.Count!)
  })

  it('supports ConsistentRead on LSI scan', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: compositeTableDef.name,
        IndexName: 'lsi2',
        FilterExpression: 'begins_with(pk, :prefix)',
        ExpressionAttributeValues: { ':prefix': { S: 'scan-lsi-' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items!.length).toBe(3)
    // lsi2 has INCLUDE projection with nonKeyAttributes ['lsi1sk']
    for (const item of result.Items!) {
      expect(item.pk).toBeDefined()
      expect(item.sk).toBeDefined()
      expect(item.lsi2sk).toBeDefined()
      expect(item.lsi1sk).toBeDefined()
      // 'data' is NOT in the INCLUDE list
      expect(item.data).toBeUndefined()
    }
  })
})
