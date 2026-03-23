import {
  BatchWriteItemCommand,
  GetItemCommand,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('BatchWriteItem — basic', () => {
  afterAll(async () => {
    const keys = Array.from({ length: 10 }, (_, i) => ({ pk: { S: `bw-${i}` } }))
    await cleanupItems(hashTableDef.name, keys)
  })

  it('writes multiple items in a single batch', async () => {
    const items = Array.from({ length: 5 }, (_, i) => ({
      PutRequest: {
        Item: { pk: { S: `bw-${i}` }, val: { N: String(i) } },
      },
    }))

    const result = await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: { [hashTableDef.name]: items },
      }),
    )

    // All items should be processed
    const unprocessed = result.UnprocessedItems?.[hashTableDef.name]
    expect(unprocessed ?? []).toHaveLength(0)

    // Verify items exist
    for (let i = 0; i < 5; i++) {
      const get = await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: `bw-${i}` } },
          ConsistentRead: true,
        }),
      )
      expect(get.Item).toBeDefined()
      expect(get.Item!.val.N).toBe(String(i))
    }
  })

  it('deletes multiple items in a single batch', async () => {
    // First put some items
    await Promise.all(
      Array.from({ length: 3 }, (_, i) =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: `bw-del-${i}` }, val: { S: 'to-delete' } },
          }),
        ),
      ),
    )

    const deletes = Array.from({ length: 3 }, (_, i) => ({
      DeleteRequest: { Key: { pk: { S: `bw-del-${i}` } } },
    }))

    await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: { [hashTableDef.name]: deletes },
      }),
    )

    // Verify items are gone
    for (let i = 0; i < 3; i++) {
      const get = await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: `bw-del-${i}` } },
          ConsistentRead: true,
        }),
      )
      expect(get.Item).toBeUndefined()
    }
  })

  it('supports mixed puts and deletes in one batch', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'bw-mix-del' }, val: { S: 'delete-me' } },
      }),
    )

    await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [hashTableDef.name]: [
            {
              PutRequest: {
                Item: { pk: { S: 'bw-mix-put' }, val: { S: 'new' } },
              },
            },
            { DeleteRequest: { Key: { pk: { S: 'bw-mix-del' } } } },
          ],
        },
      }),
    )

    const putResult = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'bw-mix-put' } },
        ConsistentRead: true,
      }),
    )
    expect(putResult.Item).toBeDefined()

    const delResult = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'bw-mix-del' } },
        ConsistentRead: true,
      }),
    )
    expect(delResult.Item).toBeUndefined()

    await cleanupItems(hashTableDef.name, [{ pk: { S: 'bw-mix-put' } }])
  })
})

describe('BatchWriteItem — multiple tables', () => {
  const hashKeys = [
    { pk: { S: 'bw-multi-h1' } },
    { pk: { S: 'bw-multi-h2' } },
  ]
  const compositeKeys = [
    { pk: { S: 'bw-multi-c1' }, sk: { S: 'a' } },
    { pk: { S: 'bw-multi-c2' }, sk: { S: 'b' } },
  ]

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, hashKeys)
    await cleanupItems(compositeTableDef.name, compositeKeys)
  })

  it('writes items to multiple tables in one batch', async () => {
    const result = await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [hashTableDef.name]: [
            { PutRequest: { Item: { pk: { S: 'bw-multi-h1' }, val: { S: 'h1' } } } },
            { PutRequest: { Item: { pk: { S: 'bw-multi-h2' }, val: { S: 'h2' } } } },
          ],
          [compositeTableDef.name]: [
            { PutRequest: { Item: { pk: { S: 'bw-multi-c1' }, sk: { S: 'a' }, val: { S: 'c1' } } } },
            { PutRequest: { Item: { pk: { S: 'bw-multi-c2' }, sk: { S: 'b' }, val: { S: 'c2' } } } },
          ],
        },
      }),
    )

    const unprocessedHash = result.UnprocessedItems?.[hashTableDef.name]
    expect(unprocessedHash ?? []).toHaveLength(0)
    const unprocessedComposite = result.UnprocessedItems?.[compositeTableDef.name]
    expect(unprocessedComposite ?? []).toHaveLength(0)

    // Verify items in hashTableDef
    for (const key of hashKeys) {
      const get = await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: key,
          ConsistentRead: true,
        }),
      )
      expect(get.Item).toBeDefined()
    }

    // Verify items in compositeTableDef
    for (const key of compositeKeys) {
      const get = await ddb.send(
        new GetItemCommand({
          TableName: compositeTableDef.name,
          Key: key,
          ConsistentRead: true,
        }),
      )
      expect(get.Item).toBeDefined()
    }
  })

  it('deletes items from multiple tables in one batch', async () => {
    // Ensure items exist first
    await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [hashTableDef.name]: [
            { PutRequest: { Item: { pk: { S: 'bw-multi-h1' }, val: { S: 'h1' } } } },
            { PutRequest: { Item: { pk: { S: 'bw-multi-h2' }, val: { S: 'h2' } } } },
          ],
          [compositeTableDef.name]: [
            { PutRequest: { Item: { pk: { S: 'bw-multi-c1' }, sk: { S: 'a' }, val: { S: 'c1' } } } },
            { PutRequest: { Item: { pk: { S: 'bw-multi-c2' }, sk: { S: 'b' }, val: { S: 'c2' } } } },
          ],
        },
      }),
    )

    // Delete from both tables
    await ddb.send(
      new BatchWriteItemCommand({
        RequestItems: {
          [hashTableDef.name]: hashKeys.map((key) => ({
            DeleteRequest: { Key: key },
          })),
          [compositeTableDef.name]: compositeKeys.map((key) => ({
            DeleteRequest: { Key: key },
          })),
        },
      }),
    )

    // Verify items gone from hashTableDef
    for (const key of hashKeys) {
      const get = await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: key,
          ConsistentRead: true,
        }),
      )
      expect(get.Item).toBeUndefined()
    }

    // Verify items gone from compositeTableDef
    for (const key of compositeKeys) {
      const get = await ddb.send(
        new GetItemCommand({
          TableName: compositeTableDef.name,
          Key: key,
          ConsistentRead: true,
        }),
      )
      expect(get.Item).toBeUndefined()
    }
  })
})
