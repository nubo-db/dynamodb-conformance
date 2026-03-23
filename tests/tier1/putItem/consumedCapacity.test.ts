import {
  PutItemCommand,
  GetItemCommand,
  QueryCommand,
  ScanCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('ReturnConsumedCapacity', () => {
  const hashKeys = [
    { pk: { S: 'cc-total-1' } },
    { pk: { S: 'cc-none-1' } },
    { pk: { S: 'cc-scan-1' } },
  ]
  const compositeKeys = [
    { pk: { S: 'cc-query-1' }, sk: { S: 'a' } },
    { pk: { S: 'cc-idx-1' }, sk: { S: 'a' } },
  ]

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, hashKeys)
    await cleanupItems(compositeTableDef.name, compositeKeys)
  })

  it('PutItem with TOTAL returns ConsumedCapacity with TableName and CapacityUnits', async () => {
    const result = await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'cc-total-1' }, data: { S: 'hello' } },
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )

    expect(result.ConsumedCapacity).toBeDefined()
    expect(result.ConsumedCapacity!.TableName).toBe(hashTableDef.name)
    expect(typeof result.ConsumedCapacity!.CapacityUnits).toBe('number')
    expect(result.ConsumedCapacity!.CapacityUnits).toBeGreaterThan(0)
  })

  it('GetItem with TOTAL returns ConsumedCapacity', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'cc-total-1' }, data: { S: 'hello' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'cc-total-1' } },
        ConsistentRead: true,
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )

    expect(result.ConsumedCapacity).toBeDefined()
    expect(result.ConsumedCapacity!.TableName).toBe(hashTableDef.name)
    expect(typeof result.ConsumedCapacity!.CapacityUnits).toBe('number')
    expect(result.ConsumedCapacity!.CapacityUnits).toBeGreaterThan(0)
  })

  it('Query with TOTAL returns ConsumedCapacity', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: { pk: { S: 'cc-query-1' }, sk: { S: 'a' }, data: { S: 'val' } },
      }),
    )

    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'cc-query-1' } },
        ConsistentRead: true,
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )

    expect(result.ConsumedCapacity).toBeDefined()
    expect(result.ConsumedCapacity!.TableName).toBe(compositeTableDef.name)
    expect(typeof result.ConsumedCapacity!.CapacityUnits).toBe('number')
    expect(result.ConsumedCapacity!.CapacityUnits).toBeGreaterThan(0)
  })

  it('Scan with TOTAL returns ConsumedCapacity', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'cc-scan-1' }, data: { S: 'scanme' } },
      }),
    )

    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )

    expect(result.ConsumedCapacity).toBeDefined()
    expect(result.ConsumedCapacity!.TableName).toBe(hashTableDef.name)
    expect(typeof result.ConsumedCapacity!.CapacityUnits).toBe('number')
  })

  it('PutItem with NONE does not return ConsumedCapacity', async () => {
    const result = await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'cc-none-1' }, data: { S: 'hello' } },
        ReturnConsumedCapacity: 'NONE',
      }),
    )

    expect(result.ConsumedCapacity).toBeUndefined()
  })

  it('Query with INDEXES returns per-index capacity breakdown', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: {
          pk: { S: 'cc-idx-1' },
          sk: { S: 'a' },
          lsi1sk: { S: 'lval' },
          data: { S: 'indexed' },
        },
      }),
    )

    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'cc-idx-1' } },
        ConsistentRead: true,
        ReturnConsumedCapacity: 'INDEXES',
      }),
    )

    expect(result.ConsumedCapacity).toBeDefined()
    expect(result.ConsumedCapacity!.TableName).toBe(compositeTableDef.name)
    expect(typeof result.ConsumedCapacity!.CapacityUnits).toBe('number')
    expect(result.ConsumedCapacity!.Table).toBeDefined()
    expect(typeof result.ConsumedCapacity!.Table!.CapacityUnits).toBe('number')
  })
})
