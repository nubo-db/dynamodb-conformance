import {
  PutItemCommand,
  QueryCommand,
  DeleteItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  compositeTableDef,
  cleanupItems,
  waitForGsiConsistency,
} from '../../../src/helpers.js'

describe('Query — GSI', () => {
  const items = [
    {
      pk: { S: 'gsi-q-1' },
      sk: { S: 'a' },
      lsi1sk: { S: 'gsi-hash-A' },
      lsi2sk: { S: 'gsi-range-1' },
      data: { S: 'item1' },
    },
    {
      pk: { S: 'gsi-q-2' },
      sk: { S: 'b' },
      lsi1sk: { S: 'gsi-hash-A' },
      lsi2sk: { S: 'gsi-range-2' },
      data: { S: 'item2' },
    },
    {
      pk: { S: 'gsi-q-3' },
      sk: { S: 'c' },
      lsi1sk: { S: 'gsi-hash-B' },
      lsi2sk: { S: 'gsi-range-3' },
      data: { S: 'item3' },
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
    // GSI propagation can be eventually consistent, wait for items to appear
    await waitForGsiConsistency({
      tableName: compositeTableDef.name,
      indexName: 'gsi1',
      partitionKey: { name: 'lsi1sk', value: { S: 'gsi-hash-A' } },
      expectedCount: 2,
    })
  })

  afterAll(async () => {
    await cleanupItems(
      compositeTableDef.name,
      items.map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('queries a hash-only GSI (gsi1)', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'gsi1',
        KeyConditionExpression: 'lsi1sk = :v',
        ExpressionAttributeValues: { ':v': { S: 'gsi-hash-A' } },
      }),
    )

    expect(result.Items!.length).toBe(2)
  })

  it('queries a composite GSI (gsi2) with hash and range', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'gsi2',
        KeyConditionExpression: 'lsi1sk = :pk AND lsi2sk = :sk',
        ExpressionAttributeValues: {
          ':pk': { S: 'gsi-hash-A' },
          ':sk': { S: 'gsi-range-1' },
        },
      }),
    )

    expect(result.Items!.length).toBe(1)
    expect(result.Items![0].data?.S).toBe('item1')
  })

  it('returns empty results for non-existent GSI key', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'gsi1',
        KeyConditionExpression: 'lsi1sk = :v',
        ExpressionAttributeValues: { ':v': { S: 'does-not-exist' } },
      }),
    )

    expect(result.Items).toHaveLength(0)
  })

  it('sparse GSI: items without GSI key attributes are excluded', async () => {
    // Put an item without the GSI key attribute
    const sparseItem = { pk: { S: 'gsi-sparse' }, sk: { S: 'x' } }
    await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: sparseItem,
      }),
    )

    // Query the GSI — the sparse item should not appear
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        IndexName: 'gsi1',
        KeyConditionExpression: 'lsi1sk = :v',
        ExpressionAttributeValues: { ':v': { S: 'gsi-hash-A' } },
      }),
    )

    const pks = result.Items!.map((i) => i.pk.S)
    expect(pks).not.toContain('gsi-sparse')

    await ddb.send(
      new DeleteItemCommand({
        TableName: compositeTableDef.name,
        Key: { pk: sparseItem.pk, sk: sparseItem.sk },
      }),
    )
  })
})
