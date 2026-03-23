import {
  PutItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeBTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Query — binary sort key', () => {
  const pk = 'binq'
  const sk1 = new Uint8Array([0x01])
  const sk2 = new Uint8Array([0x01, 0x02])
  const sk3 = new Uint8Array([0x02])
  const sk4 = new Uint8Array([0xFF])

  const items = [
    { pk: { S: pk }, sk: { B: sk1 }, label: { S: '01' } },
    { pk: { S: pk }, sk: { B: sk2 }, label: { S: '0102' } },
    { pk: { S: pk }, sk: { B: sk3 }, label: { S: '02' } },
    { pk: { S: pk }, sk: { B: sk4 }, label: { S: 'ff' } },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: compositeBTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      compositeBTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('returns items in byte-wise ascending order', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeBTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(4)
    const labels = result.Items!.map((i) => i.label.S)
    // Byte-wise ascending: [0x01] < [0x01,0x02] < [0x02] < [0xFF]
    expect(labels).toEqual(['01', '0102', '02', 'ff'])
  })

  it('returns items in descending order with ScanIndexForward=false', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeBTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: pk } },
        ScanIndexForward: false,
        ConsistentRead: true,
      }),
    )

    const labels = result.Items!.map((i) => i.label.S)
    expect(labels).toEqual(['ff', '02', '0102', '01'])
  })

  it('supports > comparison on binary sort key (byte-wise)', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeBTableDef.name,
        KeyConditionExpression: 'pk = :pk AND sk > :val',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':val': { B: new Uint8Array([0x01, 0x02]) },
        },
        ConsistentRead: true,
      }),
    )

    const labels = result.Items!.map((i) => i.label.S)
    // Items with sk > [0x01, 0x02]: [0x02] and [0xFF]
    expect(labels).toEqual(['02', 'ff'])
  })

  it('supports begins_with on binary sort key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeBTableDef.name,
        KeyConditionExpression: 'pk = :pk AND begins_with(sk, :prefix)',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':prefix': { B: new Uint8Array([0x01]) },
        },
        ConsistentRead: true,
      }),
    )

    const labels = result.Items!.map((i) => i.label.S)
    // Items whose sk begins with [0x01]: [0x01] and [0x01, 0x02]
    expect(labels).toEqual(['01', '0102'])
  })
})
