import {
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems } from '../../../src/helpers.js'

describe('UpdateItem — SET', () => {
  afterEach(async () => {
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-set' } }])
  })

  it('creates an item when it does not exist (upsert)', async () => {
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        UpdateExpression: 'SET #d = :v',
        ExpressionAttributeNames: { '#d': 'data' },
        ExpressionAttributeValues: { ':v': { S: 'created-via-update' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.data.S).toBe('created-via-update')
  })

  it('updates an existing attribute', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-set' }, data: { S: 'original' } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        UpdateExpression: 'SET #d = :v',
        ExpressionAttributeNames: { '#d': 'data' },
        ExpressionAttributeValues: { ':v': { S: 'modified' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.data.S).toBe('modified')
  })

  it('adds a new attribute to an existing item', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-set' }, existing: { S: 'keep' } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        UpdateExpression: 'SET newAttr = :v',
        ExpressionAttributeValues: { ':v': { N: '100' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.existing.S).toBe('keep')
    expect(result.Item!.newAttr.N).toBe('100')
  })

  it('supports SET with arithmetic (SET x = x + :val)', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-set' }, counter: { N: '10' } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        UpdateExpression: 'SET #c = #c + :inc',
        ExpressionAttributeNames: { '#c': 'counter' },
        ExpressionAttributeValues: { ':inc': { N: '5' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.counter.N).toBe('15')
  })

  it('supports SET with if_not_exists', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-set' }, existing: { S: 'keep-me' } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        UpdateExpression:
          'SET existing = if_not_exists(existing, :def), newone = if_not_exists(newone, :def2)',
        ExpressionAttributeValues: {
          ':def': { S: 'default' },
          ':def2': { S: 'created' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.existing.S).toBe('keep-me') // unchanged
    expect(result.Item!.newone.S).toBe('created') // set to default
  })

  it('if_not_exists with cross-attribute reference', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-set' }, myval: { S: 'original' } },
      }),
    )

    // backup doesn't exist, so if_not_exists(backup, myval) should resolve to myval's value
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        UpdateExpression: 'SET #bk = if_not_exists(#bk, #mv)',
        ExpressionAttributeNames: { '#bk': 'backup', '#mv': 'myval' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.backup.S).toBe('original')
  })

  it('supports SET with list_append', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'upd-set' },
          vals: { L: [{ S: 'a' }, { S: 'b' }] },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        UpdateExpression: 'SET vals = list_append(vals, :newItems)',
        ExpressionAttributeValues: {
          ':newItems': { L: [{ S: 'c' }, { S: 'd' }] },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-set' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.vals.L).toHaveLength(4)
    expect(result.Item!.vals.L![0].S).toBe('a')
    expect(result.Item!.vals.L![3].S).toBe('d')
  })
})

describe('UpdateItem — REMOVE', () => {
  it('removes an attribute from an item', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'upd-rem' },
          keep: { S: 'yes' },
          drop: { S: 'no' },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-rem' } },
        UpdateExpression: 'REMOVE #d',
        ExpressionAttributeNames: { '#d': 'drop' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-rem' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.keep.S).toBe('yes')
    expect(result.Item!.drop).toBeUndefined()

    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-rem' } }])
  })

  it('REMOVE on non-existent attribute succeeds silently', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-rem-ghost' }, existing: { S: 'hello' } },
      }),
    )

    // REMOVE an attribute that does not exist — should not throw
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-rem-ghost' } },
        UpdateExpression: 'REMOVE #g',
        ExpressionAttributeNames: { '#g': 'ghostAttr' },
      }),
    )

    // Verify item is unchanged
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-rem-ghost' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.existing.S).toBe('hello')
    expect(result.Item!.ghostAttr).toBeUndefined()

    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-rem-ghost' } }])
  })

  it('REMOVE on non-existent nested path succeeds silently', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'upd-rem-nested' },
          mapAttr: { M: { realKey: { S: 'value' } } },
        },
      }),
    )

    // REMOVE a nested path that does not exist — should not throw
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-rem-nested' } },
        UpdateExpression: 'REMOVE #m.#n',
        ExpressionAttributeNames: { '#m': 'mapAttr', '#n': 'nonexistent' },
      }),
    )

    // Verify item is unchanged
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-rem-nested' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.mapAttr.M!.realKey.S).toBe('value')
    expect(result.Item!.mapAttr.M!.nonexistent).toBeUndefined()

    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-rem-nested' } }])
  })
})

describe('UpdateItem — ADD', () => {
  it('adds a number to an existing numeric attribute', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-add' }, count: { N: '10' } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-add' } },
        UpdateExpression: 'ADD #c :v',
        ExpressionAttributeNames: { '#c': 'count' },
        ExpressionAttributeValues: { ':v': { N: '3' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-add' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.count.N).toBe('13')

    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-add' } }])
  })

  it('ADD on non-existent numeric attribute creates it', async () => {
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'upd-add-new' } }, // no counter attribute
    }))
    await ddb.send(new UpdateItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'upd-add-new' } },
      UpdateExpression: 'ADD #c :v',
      ExpressionAttributeNames: { '#c': 'counter' },
      ExpressionAttributeValues: { ':v': { N: '10' } },
    }))
    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'upd-add-new' } },
      ConsistentRead: true,
    }))
    expect(result.Item!.counter.N).toBe('10')
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-add-new' } }])
  })

  it('adds elements to a string set', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-add-ss' }, tags: { SS: ['a', 'b'] } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-add-ss' } },
        UpdateExpression: 'ADD tags :v',
        ExpressionAttributeValues: { ':v': { SS: ['c', 'd'] } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-add-ss' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.tags.SS).toEqual(
      expect.arrayContaining(['a', 'b', 'c', 'd']),
    )

    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-add-ss' } }])
  })
})

describe('UpdateItem — DELETE', () => {
  it('DELETE all elements from set removes the attribute entirely', async () => {
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'upd-del-all' }, tags: { SS: ['a', 'b'] } },
    }))
    await ddb.send(new UpdateItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'upd-del-all' } },
      UpdateExpression: 'DELETE #t :v',
      ExpressionAttributeNames: { '#t': 'tags' },
      ExpressionAttributeValues: { ':v': { SS: ['a', 'b'] } },
    }))
    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'upd-del-all' } },
      ConsistentRead: true,
    }))
    // Attribute should be gone entirely, not an empty set
    expect(result.Item!.tags).toBeUndefined()
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-del-all' } }])
  })

  it('DELETE partial elements from set keeps remaining', async () => {
    await ddb.send(new PutItemCommand({
      TableName: hashTableDef.name,
      Item: { pk: { S: 'upd-del-partial' }, tags: { SS: ['a', 'b', 'c'] } },
    }))
    await ddb.send(new UpdateItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'upd-del-partial' } },
      UpdateExpression: 'DELETE #t :v',
      ExpressionAttributeNames: { '#t': 'tags' },
      ExpressionAttributeValues: { ':v': { SS: ['b'] } },
    }))
    const result = await ddb.send(new GetItemCommand({
      TableName: hashTableDef.name,
      Key: { pk: { S: 'upd-del-partial' } },
      ConsistentRead: true,
    }))
    expect(result.Item!.tags.SS).toEqual(expect.arrayContaining(['a', 'c']))
    expect(result.Item!.tags.SS).not.toContain('b')
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-del-partial' } }])
  })

  it('removes elements from a string set', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-del-ss' }, tags: { SS: ['a', 'b', 'c'] } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-del-ss' } },
        UpdateExpression: 'DELETE tags :v',
        ExpressionAttributeValues: { ':v': { SS: ['b'] } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-del-ss' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.tags.SS).toEqual(expect.arrayContaining(['a', 'c']))
    expect(result.Item!.tags.SS).not.toContain('b')

    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-del-ss' } }])
  })
})

describe('UpdateItem — return values', () => {
  it('returns ALL_NEW after update', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-ret' }, x: { N: '1' } },
      }),
    )

    const result = await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-ret' } },
        UpdateExpression: 'SET x = :v',
        ExpressionAttributeValues: { ':v': { N: '2' } },
        ReturnValues: 'ALL_NEW',
      }),
    )

    expect(result.Attributes!.x.N).toBe('2')
    expect(result.Attributes!.pk.S).toBe('upd-ret')

    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-ret' } }])
  })

  it('returns UPDATED_OLD for changed attributes', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-ret2' }, x: { N: '1' }, y: { S: 'keep' } },
      }),
    )

    const result = await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-ret2' } },
        UpdateExpression: 'SET x = :v',
        ExpressionAttributeValues: { ':v': { N: '2' } },
        ReturnValues: 'UPDATED_OLD',
      }),
    )

    expect(result.Attributes!.x.N).toBe('1')
    // y was not updated, so it should not appear in UPDATED_OLD
    expect(result.Attributes!.y).toBeUndefined()

    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-ret2' } }])
  })

  it('returns UPDATED_NEW for changed attributes', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-ret3' }, x: { N: '1' }, y: { S: 'keep' } },
      }),
    )

    const result = await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-ret3' } },
        UpdateExpression: 'SET x = :v',
        ExpressionAttributeValues: { ':v': { N: '2' } },
        ReturnValues: 'UPDATED_NEW',
      }),
    )

    expect(result.Attributes!.x.N).toBe('2')
    expect(result.Attributes!.y).toBeUndefined()

    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-ret3' } }])
  })
})
