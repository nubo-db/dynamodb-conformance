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

describe('ConsumedCapacity across operations', { tags: ['get-item', 'data-plane'] }, () => {
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

// Real DynamoDB reports the ReadCapacityUnits / WriteCapacityUnits split only on
// transactional operations (TransactWriteItems, TransactGetItems, ExecuteTransaction —
// see tests/tier2/transactions and tests/tier2/partiql/executeTransaction). A plain
// single-item or single-table operation reports the aggregate CapacityUnits alone, with
// no read/write breakdown, under both TOTAL and INDEXES. This is the read/update/delete
// side; the PutItem side lives in tests/tier1/putItem/consumedCapacity.test.ts. Sibling
// to the transactional split coverage, which asserts the split's presence. Magnitudes
// characterised against real DynamoDB (eu-west-2). See #69.
describe('ConsumedCapacity — single-item ops report only the aggregate, no read/write split', { tags: ['get-item', 'data-plane'] }, () => {
  const present = { pk: { S: 'cc-split-present' } }
  const delKey = { pk: { S: 'cc-split-del' } }

  beforeAll(async () => {
    await Promise.all([
      ddb.send(new PutItemCommand({ TableName: hashTableDef.name, Item: { pk: present.pk, data: { S: 'x' } } })),
      ddb.send(new PutItemCommand({ TableName: hashTableDef.name, Item: { pk: delKey.pk, data: { S: 'x' } } })),
      ddb.send(new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: { pk: { S: 'cc-split-q' }, sk: { S: 'a' }, lsi1sk: { S: 'l' }, data: { S: 'x' } },
      })),
    ])
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [present, delKey])
    await cleanupItems(compositeTableDef.name, [{ pk: { S: 'cc-split-q' }, sk: { S: 'a' } }])
  })

  it('a strongly-consistent GetItem reports 1 CapacityUnit and no read/write split', async () => {
    const res = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: present,
      ConsistentRead: true,
      ReturnConsumedCapacity: 'TOTAL',
    }))
    expect(res.ConsumedCapacity!.CapacityUnits).toBe(1)
    expect(res.ConsumedCapacity!.ReadCapacityUnits).toBeUndefined()
    expect(res.ConsumedCapacity!.WriteCapacityUnits).toBeUndefined()
  })

  it('an eventually-consistent GetItem reports 0.5 CapacityUnits and no read/write split', async () => {
    const res = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: present,
      ReturnConsumedCapacity: 'TOTAL',
    }))
    // A half-unit eventually-consistent read is itself a conformance signal.
    expect(res.ConsumedCapacity!.CapacityUnits).toBe(0.5)
    expect(res.ConsumedCapacity!.ReadCapacityUnits).toBeUndefined()
    expect(res.ConsumedCapacity!.WriteCapacityUnits).toBeUndefined()
  })

  it('a Query under INDEXES omits the split at the top level and on Table', async () => {
    const res = await ddb.send(new QueryCommand({
      TableName: compositeTableDef.name,
      KeyConditionExpression: '#pk = :pk',
      ExpressionAttributeNames: { '#pk': 'pk' },
      ExpressionAttributeValues: { ':pk': { S: 'cc-split-q' } },
      ConsistentRead: true,
      ReturnConsumedCapacity: 'INDEXES',
    }))
    expect(res.ConsumedCapacity!.CapacityUnits).toBe(1)
    expect(res.ConsumedCapacity!.ReadCapacityUnits).toBeUndefined()
    expect(res.ConsumedCapacity!.Table!.CapacityUnits).toBe(1)
    expect(res.ConsumedCapacity!.Table!.ReadCapacityUnits).toBeUndefined()
  })

  it('an UpdateItem reports 1 CapacityUnit and no read/write split', async () => {
    const res = await ddb.send(new UpdateItemCommand({
      TableName: hashTableDef.name,
      Key: present,
      UpdateExpression: 'SET #d = :v',
      ExpressionAttributeNames: { '#d': 'data' },
      ExpressionAttributeValues: { ':v': { S: 'y' } },
      ReturnConsumedCapacity: 'TOTAL',
    }))
    expect(res.ConsumedCapacity!.CapacityUnits).toBe(1)
    expect(res.ConsumedCapacity!.WriteCapacityUnits).toBeUndefined()
    expect(res.ConsumedCapacity!.ReadCapacityUnits).toBeUndefined()
  })

  it('a DeleteItem reports 1 CapacityUnit and no read/write split', async () => {
    const res = await ddb.send(new DeleteItemCommand({
      TableName: hashTableDef.name,
      Key: delKey,
      ReturnConsumedCapacity: 'TOTAL',
    }))
    expect(res.ConsumedCapacity!.CapacityUnits).toBe(1)
    expect(res.ConsumedCapacity!.WriteCapacityUnits).toBeUndefined()
    expect(res.ConsumedCapacity!.ReadCapacityUnits).toBeUndefined()
  })
})
