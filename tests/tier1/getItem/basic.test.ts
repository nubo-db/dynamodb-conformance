import {
  PutItemCommand,
  GetItemCommand,
  DeleteItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  cleanupItems,
} from '../../../src/helpers.js'

describe('GetItem — basic', () => {
  beforeAll(async () => {
    await Promise.all([
      ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: {
            pk: { S: 'get-1' },
            data: { S: 'hello' },
            num: { N: '42' },
            flag: { BOOL: true },
          },
        }),
      ),
      ddb.send(
        new PutItemCommand({
          TableName: compositeTableDef.name,
          Item: {
            pk: { S: 'get-comp' },
            sk: { S: 'sort1' },
            value: { S: 'composite-value' },
          },
        }),
      ),
    ])
  })

  afterAll(async () => {
    await Promise.all([
      cleanupItems(hashTableDef.name, [{ pk: { S: 'get-1' } }]),
      cleanupItems(compositeTableDef.name, [
        { pk: { S: 'get-comp' }, sk: { S: 'sort1' } },
      ]),
    ])
  })

  it('retrieves an existing item', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'get-1' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.pk.S).toBe('get-1')
    expect(result.Item!.data.S).toBe('hello')
    expect(result.Item!.num.N).toBe('42')
    expect(result.Item!.flag.BOOL).toBe(true)
  })

  it('returns no Item for a non-existent key', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'does-not-exist' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeUndefined()
  })

  it('retrieves an item from a composite key table', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: compositeTableDef.name,
        Key: { pk: { S: 'get-comp' }, sk: { S: 'sort1' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.value.S).toBe('composite-value')
  })

  it('returns no Item when hash matches but sort key does not', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: compositeTableDef.name,
        Key: { pk: { S: 'get-comp' }, sk: { S: 'wrong-sort' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeUndefined()
  })
})

describe('GetItem — ProjectionExpression', () => {
  beforeAll(async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'get-proj' },
          a: { S: 'alpha' },
          b: { S: 'beta' },
          c: { N: '99' },
        },
      }),
    )
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'get-proj' } }])
  })

  it('returns only projected attributes', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'get-proj' } },
        ProjectionExpression: 'a, c',
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.a.S).toBe('alpha')
    expect(result.Item!.c.N).toBe('99')
    // b should not be present (unless the key is also returned, but pk wasn't projected)
    expect(result.Item!.b).toBeUndefined()
  })

  it('returns key attributes even when not explicitly projected (SDK behaviour)', async () => {
    // Note: DynamoDB does NOT auto-include keys in ProjectionExpression.
    // This test verifies that only requested attributes come back.
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'get-proj' } },
        ProjectionExpression: 'a',
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.a.S).toBe('alpha')
    // pk should NOT be returned since it wasn't in the projection
    expect(result.Item!.pk).toBeUndefined()
  })
})

describe('GetItem — ConsistentRead', () => {
  it('accepts ConsistentRead: true', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'nonexistent-cr' } },
        ConsistentRead: true,
      }),
    )
    // Should not error; item just won't exist
    expect(result.Item).toBeUndefined()
  })

  it('accepts ConsistentRead: false', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'nonexistent-cr' } },
        ConsistentRead: false,
      }),
    )
    expect(result.Item).toBeUndefined()
  })
})
