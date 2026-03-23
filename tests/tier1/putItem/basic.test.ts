import {
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('PutItem — basic', () => {
  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [
      { pk: { S: 'put-basic-1' } },
    ])
    await cleanupItems(compositeTableDef.name, [
      { pk: { S: 'put-comp-1' }, sk: { S: 'sort-1' } },
    ])
  })

  it('puts and retrieves a simple item with hash key', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'put-basic-1' },
          data: { S: 'hello' },
          num: { N: '42' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'put-basic-1' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.pk.S).toBe('put-basic-1')
    expect(result.Item!.data.S).toBe('hello')
    expect(result.Item!.num.N).toBe('42')
  })

  it('puts an item with composite key', async () => {
    const key = { pk: { S: 'put-comp-1' }, sk: { S: 'sort-1' } }

    await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: { ...key, value: { S: 'test' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: compositeTableDef.name,
        Key: key,
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.value.S).toBe('test')
  })

  it('overwrites an existing item with the same key', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'put-basic-1' }, data: { S: 'first' } },
      }),
    )

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'put-basic-1' }, data: { S: 'second' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'put-basic-1' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item!.data.S).toBe('second')
  })
})

describe('PutItem — all data types', () => {
  const pk = 'put-types-1'

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])
  })

  it('stores and retrieves all DynamoDB attribute types', async () => {
    const item = {
      pk: { S: pk },
      stringAttr: { S: 'hello' },
      numberAttr: { N: '123.456' },
      binaryAttr: { B: new Uint8Array([1, 2, 3]) },
      boolTrue: { BOOL: true },
      boolFalse: { BOOL: false },
      nullAttr: { NULL: true },
      stringSet: { SS: ['a', 'b', 'c'] },
      numberSet: { NS: ['1', '2', '3'] },
      binarySet: {
        BS: [new Uint8Array([1]), new Uint8Array([2])],
      },
      listAttr: {
        L: [{ S: 'item1' }, { N: '99' }, { BOOL: true }],
      },
      mapAttr: {
        M: {
          nested: { S: 'value' },
          num: { N: '7' },
        },
      },
    }

    await ddb.send(
      new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )

    const got = result.Item!
    expect(got.stringAttr.S).toBe('hello')
    expect(got.numberAttr.N).toBe('123.456')
    expect(got.boolTrue.BOOL).toBe(true)
    expect(got.boolFalse.BOOL).toBe(false)
    expect(got.nullAttr.NULL).toBe(true)
    expect(got.stringSet.SS).toEqual(expect.arrayContaining(['a', 'b', 'c']))
    expect(got.numberSet.NS).toEqual(expect.arrayContaining(['1', '2', '3']))
    expect(got.listAttr.L).toHaveLength(3)
    expect(got.mapAttr.M!.nested.S).toBe('value')
  })
})

describe('PutItem — return values', () => {
  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [
      { pk: { S: 'put-return-1' } },
      { pk: { S: 'put-return-nonexist' } },
    ])
  })

  it('returns ALL_OLD when replacing an item', async () => {
    const key = { pk: { S: 'put-return-1' } }

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { ...key, old: { S: 'data' } },
      }),
    )

    const result = await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { ...key, new: { S: 'data' } },
        ReturnValues: 'ALL_OLD',
      }),
    )

    expect(result.Attributes).toBeDefined()
    expect(result.Attributes!.old.S).toBe('data')
  })

  it('returns nothing with ALL_OLD when item does not exist', async () => {
    const result = await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'put-return-nonexist' } },
        ReturnValues: 'ALL_OLD',
      }),
    )

    expect(result.Attributes).toBeUndefined()
  })
})
