import {
  PutItemCommand,
  GetItemCommand,
  DeleteItemCommand,
  UpdateItemCommand,
  QueryCommand,
  ScanCommand,
  BatchWriteItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('ConsumedCapacity across operations', () => {
  const hashKeys = [
    { pk: { S: 'cc-get-1' } },
    { pk: { S: 'cc-del-1' } },
    { pk: { S: 'cc-upd-1' } },
    { pk: { S: 'cc-scan-1' } },
    { pk: { S: 'cc-bw-1' } },
    { pk: { S: 'cc-bw-2' } },
  ]
  const compositeKeys = [
    { pk: { S: 'cc-qidx-1' }, sk: { S: 'a' } },
  ]

  beforeAll(async () => {
    await Promise.all([
      ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'cc-get-1' }, data: { S: 'getme' } },
        }),
      ),
      ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'cc-del-1' }, data: { S: 'deleteme' } },
        }),
      ),
      ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'cc-upd-1' }, data: { S: 'updateme' } },
        }),
      ),
      ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: 'cc-scan-1' }, data: { S: 'scanme' } },
        }),
      ),
      ddb.send(
        new PutItemCommand({
          TableName: compositeTableDef.name,
          Item: {
            pk: { S: 'cc-qidx-1' },
            sk: { S: 'a' },
            lsi1sk: { S: 'lval' },
            data: { S: 'indexed' },
          },
        }),
      ),
    ])
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, hashKeys)
    await cleanupItems(compositeTableDef.name, compositeKeys)
  })

  it('GetItem with TOTAL returns ConsumedCapacity', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'cc-get-1' } },
        ConsistentRead: true,
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )

    expect(result.ConsumedCapacity).toBeDefined()
    expect(result.ConsumedCapacity!.TableName).toBe(hashTableDef.name)
    expect(typeof result.ConsumedCapacity!.CapacityUnits).toBe('number')
    expect(result.ConsumedCapacity!.CapacityUnits).toBeGreaterThan(0)
  })

  it('DeleteItem with TOTAL returns ConsumedCapacity', async () => {
    const result = await ddb.send(
      new DeleteItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'cc-del-1' } },
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )

    expect(result.ConsumedCapacity).toBeDefined()
    expect(result.ConsumedCapacity!.TableName).toBe(hashTableDef.name)
    expect(typeof result.ConsumedCapacity!.CapacityUnits).toBe('number')
    expect(result.ConsumedCapacity!.CapacityUnits).toBeGreaterThan(0)
  })

  it('UpdateItem with TOTAL returns ConsumedCapacity', async () => {
    const result = await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'cc-upd-1' } },
        UpdateExpression: 'SET #d = :v',
        ExpressionAttributeNames: { '#d': 'data' },
        ExpressionAttributeValues: { ':v': { S: 'updated' } },
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )

    expect(result.ConsumedCapacity).toBeDefined()
    expect(result.ConsumedCapacity!.TableName).toBe(hashTableDef.name)
    expect(typeof result.ConsumedCapacity!.CapacityUnits).toBe('number')
    expect(result.ConsumedCapacity!.CapacityUnits).toBeGreaterThan(0)
  })

  it('Query with INDEXES returns per-index breakdown', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        ExpressionAttributeNames: { '#pk': 'pk' },
        ExpressionAttributeValues: { ':pk': { S: 'cc-qidx-1' } },
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

  it('Scan with TOTAL returns ConsumedCapacity', async () => {
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

  it('BatchWriteItem with TOTAL returns per-table ConsumedCapacity', async () => {
    const result = await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [hashTableDef.name]: [
            {
              PutRequest: {
                Item: { pk: { S: 'cc-bw-1' }, data: { S: 'batch1' } },
              },
            },
            {
              PutRequest: {
                Item: { pk: { S: 'cc-bw-2' }, data: { S: 'batch2' } },
              },
            },
          ],
        },
        ReturnConsumedCapacity: 'TOTAL',
      }),
    )

    expect(result.ConsumedCapacity).toBeDefined()
    expect(result.ConsumedCapacity!.length).toBeGreaterThan(0)
    const tableCapacity = result.ConsumedCapacity!.find(
      (c) => c.TableName === hashTableDef.name,
    )
    expect(tableCapacity).toBeDefined()
    expect(typeof tableCapacity!.CapacityUnits).toBe('number')
    expect(tableCapacity!.CapacityUnits).toBeGreaterThan(0)
  })
})
