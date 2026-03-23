import {
  PutItemCommand,
  ScanCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Scan — Select COUNT', () => {
  const items = [
    { pk: { S: 'scan-select-0' }, category: { S: 'alpha' } },
    { pk: { S: 'scan-select-1' }, category: { S: 'beta' } },
    { pk: { S: 'scan-select-2' }, category: { S: 'alpha' } },
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

  it('Select COUNT returns count without items on Scan', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        Select: 'COUNT',
        ConsistentRead: true,
      }),
    )

    expect(result.Count).toBeGreaterThan(0)
    expect(result.Items).toBeUndefined()
  })

  it('Select COUNT with FilterExpression on Scan', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#c = :cat AND begins_with(pk, :prefix)',
        ExpressionAttributeNames: { '#c': 'category' },
        ExpressionAttributeValues: {
          ':cat': { S: 'alpha' },
          ':prefix': { S: 'scan-select-' },
        },
        Select: 'COUNT',
        ConsistentRead: true,
      }),
    )

    expect(result.Count).toBe(2)
    expect(result.ScannedCount).toBeGreaterThanOrEqual(3)
    expect(result.Items).toBeUndefined()
  })
})
