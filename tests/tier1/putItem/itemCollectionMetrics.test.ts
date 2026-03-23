import {
  PutItemCommand,
  DeleteItemCommand,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('ReturnItemCollectionMetrics', () => {
  const keys = [
    { pk: { S: 'icm-put-1' }, sk: { S: 'a' } },
    { pk: { S: 'icm-del-1' }, sk: { S: 'a' } },
    { pk: { S: 'icm-upd-1' }, sk: { S: 'a' } },
    { pk: { S: 'icm-none-1' }, sk: { S: 'a' } },
  ]

  afterAll(async () => {
    await cleanupItems(compositeTableDef.name, keys)
  })

  it('PutItem with SIZE returns ItemCollectionMetrics', async () => {
    const result = await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: {
          pk: { S: 'icm-put-1' },
          sk: { S: 'a' },
          lsi1sk: { S: 'lval' },
          data: { S: 'hello' },
        },
        ReturnItemCollectionMetrics: 'SIZE',
      }),
    )

    expect(result.ItemCollectionMetrics).toBeDefined()
    expect(result.ItemCollectionMetrics!.ItemCollectionKey).toBeDefined()
    expect(result.ItemCollectionMetrics!.ItemCollectionKey!.pk.S).toBe('icm-put-1')
    expect(result.ItemCollectionMetrics!.SizeEstimateRangeGB).toBeDefined()
    expect(result.ItemCollectionMetrics!.SizeEstimateRangeGB).toHaveLength(2)
  })

  it('DeleteItem with SIZE returns ItemCollectionMetrics', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: {
          pk: { S: 'icm-del-1' },
          sk: { S: 'a' },
          lsi1sk: { S: 'lval' },
          data: { S: 'to-delete' },
        },
      }),
    )

    const result = await ddb.send(
      new DeleteItemCommand({
        TableName: compositeTableDef.name,
        Key: { pk: { S: 'icm-del-1' }, sk: { S: 'a' } },
        ReturnItemCollectionMetrics: 'SIZE',
      }),
    )

    expect(result.ItemCollectionMetrics).toBeDefined()
    expect(result.ItemCollectionMetrics!.ItemCollectionKey).toBeDefined()
    expect(result.ItemCollectionMetrics!.SizeEstimateRangeGB).toBeDefined()
    expect(result.ItemCollectionMetrics!.SizeEstimateRangeGB).toHaveLength(2)
  })

  it('UpdateItem with SIZE returns ItemCollectionMetrics', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: {
          pk: { S: 'icm-upd-1' },
          sk: { S: 'a' },
          lsi1sk: { S: 'lval' },
          data: { S: 'original' },
        },
      }),
    )

    const result = await ddb.send(
      new UpdateItemCommand({
        TableName: compositeTableDef.name,
        Key: { pk: { S: 'icm-upd-1' }, sk: { S: 'a' } },
        UpdateExpression: 'SET #d = :v',
        ExpressionAttributeNames: { '#d': 'data' },
        ExpressionAttributeValues: { ':v': { S: 'updated' } },
        ReturnItemCollectionMetrics: 'SIZE',
      }),
    )

    expect(result.ItemCollectionMetrics).toBeDefined()
    expect(result.ItemCollectionMetrics!.ItemCollectionKey).toBeDefined()
    expect(result.ItemCollectionMetrics!.SizeEstimateRangeGB).toBeDefined()
    expect(result.ItemCollectionMetrics!.SizeEstimateRangeGB).toHaveLength(2)
  })

  it('PutItem with NONE does not return ItemCollectionMetrics', async () => {
    const result = await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: {
          pk: { S: 'icm-none-1' },
          sk: { S: 'a' },
          lsi1sk: { S: 'lval' },
          data: { S: 'hello' },
        },
        ReturnItemCollectionMetrics: 'NONE',
      }),
    )

    expect(result.ItemCollectionMetrics).toBeUndefined()
  })
})
