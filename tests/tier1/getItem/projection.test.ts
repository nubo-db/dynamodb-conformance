import {
  PutItemCommand,
  GetItemCommand,
  QueryCommand,
  BatchGetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Nested attribute projection', { tags: ['get-item', 'data-plane'] }, () => {
  const hashPk = 'proj-nested'
  const compositePk = 'proj-nested-q'

  beforeAll(async () => {
    await Promise.all([
      ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: {
            pk: { S: hashPk },
            mymap: { M: { nested: { S: 'deep' }, other: { N: '42' } } },
            mylist: { L: [{ S: 'zero' }, { S: 'one' }, { S: 'two' }] },
          },
        }),
      ),
      ddb.send(
        new PutItemCommand({
          TableName: compositeTableDef.name,
          Item: {
            pk: { S: compositePk },
            sk: { S: 'a' },
            mymap: { M: { nested: { S: 'deep' }, other: { N: '42' } } },
            mylist: { L: [{ S: 'zero' }, { S: 'one' }, { S: 'two' }] },
          },
        }),
      ),
    ])
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [{ pk: { S: hashPk } }])
    await cleanupItems(compositeTableDef.name, [
      { pk: { S: compositePk }, sk: { S: 'a' } },
    ])
  })

  it('GetItem ProjectionExpression with nested map path returns only the nested value', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: hashPk } },
        ProjectionExpression: '#m.#n',
        ExpressionAttributeNames: { '#m': 'mymap', '#n': 'nested' },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.mymap).toBeDefined()
    expect(result.Item!.mymap.M!.nested.S).toBe('deep')
    // "other" should not be returned
    expect(result.Item!.mymap.M!.other).toBeUndefined()
  })

  it('GetItem ProjectionExpression with list index returns the element', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: hashPk } },
        ProjectionExpression: '#l[0]',
        ExpressionAttributeNames: { '#l': 'mylist' },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.mylist).toBeDefined()
    expect(result.Item!.mylist.L).toHaveLength(1)
    expect(result.Item!.mylist.L![0].S).toBe('zero')
  })

  it('Query ProjectionExpression with nested path and list index', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        ProjectionExpression: '#m.#n, #l[1]',
        ExpressionAttributeNames: {
          '#pk': 'pk',
          '#m': 'mymap',
          '#n': 'nested',
          '#l': 'mylist',
        },
        ExpressionAttributeValues: { ':pk': { S: compositePk } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    const item = result.Items![0]
    expect(item.mymap.M!.nested.S).toBe('deep')
    expect(item.mymap.M!.other).toBeUndefined()
    expect(item.mylist.L).toHaveLength(1)
    expect(item.mylist.L![0].S).toBe('one')
  })

  it('BatchGetItem ProjectionExpression with nested path', async () => {
    const result = await ddb.send(
      new BatchGetItemCommand({
        RequestItems: {
          [hashTableDef.name]: {
            Keys: [{ pk: { S: hashPk } }],
            ProjectionExpression: '#m.#n',
            ExpressionAttributeNames: { '#m': 'mymap', '#n': 'nested' },
            ConsistentRead: true,
          },
        },
      }),
    )

    const items = result.Responses![hashTableDef.name]
    expect(items).toHaveLength(1)
    expect(items[0].mymap.M!.nested.S).toBe('deep')
    expect(items[0].mymap.M!.other).toBeUndefined()
  })

  it('GetItem ProjectionExpression with multiple sibling paths in one map keeps all of them', async () => {
    const k = 'proj-multi'
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: k },
          m: { M: { a: { S: 'A' }, b: { S: 'B' }, c: { S: 'C' } } },
        },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: k } },
        ProjectionExpression: '#m.#a, #m.#b',
        ExpressionAttributeNames: { '#m': 'm', '#a': 'a', '#b': 'b' },
        ConsistentRead: true,
      }),
    )
    // Both projected siblings survive (not just the last), and the structure is
    // preserved; the unprojected sibling is dropped.
    expect(result.Item!.m.M!.a.S).toBe('A')
    expect(result.Item!.m.M!.b.S).toBe('B')
    expect(result.Item!.m.M!.c).toBeUndefined()
    await cleanupItems(hashTableDef.name, [{ pk: { S: k } }])
  })
})

describe('GetItem — projection matching nothing', { tags: ['get-item', 'data-plane'] }, () => {
  const pk = 'get-emptyproj'

  beforeAll(async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: pk }, name: { S: 'Alice' } },
      }),
    )
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])
  })

  it('returns an empty Item when ProjectionExpression matches no attribute on a present item', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ProjectionExpression: 'nonexistent',
        ConsistentRead: true,
      }),
    )

    // The item exists but the projection selects nothing. Unlike TransactGetItems
    // (which omits Item entirely), GetItem returns Item as an empty {} object.
    expect(result.Item).toBeDefined()
    expect(Object.keys(result.Item!)).toHaveLength(0)
  })

  it('returns an empty Item when legacy AttributesToGet matches no attribute on a present item', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        AttributesToGet: ['nonexistent'],
        ConsistentRead: true,
      }),
    )

    // Legacy AttributesToGet behaves identically to ProjectionExpression here.
    expect(result.Item).toBeDefined()
    expect(Object.keys(result.Item!)).toHaveLength(0)
  })

  it('still returns Item when projecting an attribute that exists', async () => {
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ProjectionExpression: '#n',
        ExpressionAttributeNames: { '#n': 'name' },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.name.S).toBe('Alice')
  })

  it('still returns Item when projecting the key attribute pk explicitly', async () => {
    // GetItem does not auto-include keys, but an explicitly projected key resolves
    // and so Item is returned (it is not the empty-projection case).
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ProjectionExpression: 'pk',
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.pk.S).toBe(pk)
  })
})
