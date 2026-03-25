import {
  PutItemCommand,
  TransactGetItemsCommand,
  TransactWriteItemsCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  cleanupItems,
  expectDynamoError,
} from '../../../src/helpers.js'

const hashKeys = [
  { pk: { S: 'tg-basic-1' } },
  { pk: { S: 'tg-basic-2' } },
  { pk: { S: 'tg-basic-3' } },
  { pk: { S: 'tg-cross-hash' } },
  { pk: { S: 'tg-single' } },
]

const compositeKeys = [
  { pk: { S: 'tg-cross-comp' }, sk: { S: 'sk1' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, hashKeys)
  await cleanupItems(compositeTableDef.name, compositeKeys)
})

describe('TransactGetItems - basic', () => {
  it('gets multiple items atomically', async () => {
    // Seed items
    for (const i of [1, 2, 3]) {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: `tg-basic-${i}` }, idx: { N: String(i) } },
        }),
      )
    }

    const result = await ddb.send(
      new TransactGetItemsCommand({
        TransactItems: [1, 2, 3].map((i) => ({
          Get: {
            TableName: hashTableDef.name,
            Key: { pk: { S: `tg-basic-${i}` } },
          },
        })),
      }),
    )

    expect(result.Responses).toBeDefined()
    expect(result.Responses).toHaveLength(3)
    for (let i = 0; i < 3; i++) {
      expect(result.Responses![i].Item).toBeDefined()
      expect(result.Responses![i].Item!.idx.N).toBe(String(i + 1))
    }
  })

  it('gets items across tables (hashTableDef + compositeTableDef)', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tg-cross-hash' }, data: { S: 'hash-val' } },
      }),
    )
    await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: {
          pk: { S: 'tg-cross-comp' },
          sk: { S: 'sk1' },
          data: { S: 'comp-val' },
        },
      }),
    )

    const result = await ddb.send(
      new TransactGetItemsCommand({
        TransactItems: [
          {
            Get: {
              TableName: hashTableDef.name,
              Key: { pk: { S: 'tg-cross-hash' } },
            },
          },
          {
            Get: {
              TableName: compositeTableDef.name,
              Key: { pk: { S: 'tg-cross-comp' }, sk: { S: 'sk1' } },
            },
          },
        ],
      }),
    )

    expect(result.Responses).toHaveLength(2)
    expect(result.Responses![0].Item!.data.S).toBe('hash-val')
    expect(result.Responses![1].Item!.data.S).toBe('comp-val')
  })

  it('returns empty Item for non-existing items in the mix', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tg-basic-1' }, idx: { N: '1' } },
      }),
    )

    const result = await ddb.send(
      new TransactGetItemsCommand({
        TransactItems: [
          {
            Get: {
              TableName: hashTableDef.name,
              Key: { pk: { S: 'tg-basic-1' } },
            },
          },
          {
            Get: {
              TableName: hashTableDef.name,
              Key: { pk: { S: 'tg-nonexistent-xyz' } },
            },
          },
        ],
      }),
    )

    expect(result.Responses).toHaveLength(2)
    expect(result.Responses![0].Item).toBeDefined()
    expect(result.Responses![0].Item!.pk.S).toBe('tg-basic-1')
    // Non-existing item returns an empty object (no Item property)
    expect(
      result.Responses![1].Item === undefined ||
        Object.keys(result.Responses![1].Item!).length === 0,
    ).toBe(true)
  })

  it('gets a single item', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tg-single' }, data: { S: 'solo' } },
      }),
    )

    const result = await ddb.send(
      new TransactGetItemsCommand({
        TransactItems: [
          {
            Get: {
              TableName: hashTableDef.name,
              Key: { pk: { S: 'tg-single' } },
            },
          },
        ],
      }),
    )

    expect(result.Responses).toHaveLength(1)
    expect(result.Responses![0].Item!.data.S).toBe('solo')
  })

  it('returns all items when all exist', async () => {
    // Ensure all three basic items exist
    for (const i of [1, 2, 3]) {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: `tg-basic-${i}` }, idx: { N: String(i) } },
        }),
      )
    }

    const result = await ddb.send(
      new TransactGetItemsCommand({
        TransactItems: [1, 2, 3].map((i) => ({
          Get: {
            TableName: hashTableDef.name,
            Key: { pk: { S: `tg-basic-${i}` } },
          },
        })),
      }),
    )

    expect(result.Responses).toHaveLength(3)
    for (const resp of result.Responses!) {
      expect(resp.Item).toBeDefined()
      expect(resp.Item!.pk.S).toMatch(/^tg-basic-/)
    }
  })
})

describe('TransactGetItems - validation', () => {
  it('rejects empty TransactItems', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new TransactGetItemsCommand({
            TransactItems: [],
          }),
        ),
      'ValidationException',
    )
  })

  it('rejects get on non-existent table', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new TransactGetItemsCommand({
            TransactItems: [
              {
                Get: {
                  TableName: '_conformance_nonexistent_table',
                  Key: { pk: { S: 'x' } },
                },
              },
            ],
          }),
        ),
      'ResourceNotFoundException',
    )
  })

  it('rejects duplicate keys in same transaction', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new TransactGetItemsCommand({
            TransactItems: [
              {
                Get: {
                  TableName: hashTableDef.name,
                  Key: { pk: { S: 'tg-basic-1' } },
                },
              },
              {
                Get: {
                  TableName: hashTableDef.name,
                  Key: { pk: { S: 'tg-basic-1' } },
                },
              },
            ],
          }),
        ),
      'ValidationException',
    )
  })
})
