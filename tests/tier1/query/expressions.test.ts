import {
  PutItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Query — FilterExpression functions and operators', () => {
  const pk = 'query-expr'
  const items = [
    {
      pk: { S: pk },
      sk: { S: '1' },
      name: { S: 'alice' },
      age: { N: '30' },
      tags: { SS: ['admin', 'user'] },
      mapAttr: { M: { nested: { S: 'deep-value' } } },
      listAttr: { L: [{ S: 'first' }, { S: 'second' }] },
    },
    {
      pk: { S: pk },
      sk: { S: '2' },
      name: { S: 'bob' },
      age: { N: '25' },
      tags: { SS: ['user'] },
      mapAttr: { M: { nested: { S: 'other-value' } } },
      listAttr: { L: [{ S: 'alpha' }, { S: 'beta' }] },
    },
    {
      pk: { S: pk },
      sk: { S: '3' },
      name: { S: 'carol' },
      age: { N: '35' },
      tags: { SS: ['admin', 'manager'] },
      mapAttr: { M: { nested: { S: 'deep-value' } } },
      listAttr: { L: [{ S: 'first' }, { S: 'gamma' }] },
    },
  ]

  const item4 = {
    pk: { S: pk },
    sk: { S: '4' },
    name: { S: 'dave' },
    age: { N: '28' },
    tags: { SS: ['viewer'] },
    mapAttr: { M: { nested: { S: 'v0' }, k1: { S: 'v1' }, k2: { S: 'v2' }, k3: { S: 'v3' } } },
    listAttr: { L: [{ S: 'x' }, { S: 'y' }, { S: 'z' }, { S: 'w' }] },
    binAttr: { B: new Uint8Array([0x01, 0x02, 0x03, 0x04, 0x05]) },
  }

  beforeAll(async () => {
    await Promise.all([
      ...items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: compositeTableDef.name, Item: item }),
        ),
      ),
      ddb.send(
        new PutItemCommand({ TableName: compositeTableDef.name, Item: item4 }),
      ),
    ])
  })

  afterAll(async () => {
    await cleanupItems(
      compositeTableDef.name,
      [...items, item4].map((item) => ({ pk: item.pk, sk: item.sk })),
    )
  })

  it('filters by size(attr) > :val', async () => {
    // "alice" has length 5, "bob" has length 3, "carol" has length 5
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: 'size(#n) > :len',
        ExpressionAttributeNames: { '#n': 'name' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':len': { N: '3' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(3)
    const names = result.Items!.map((i) => i.name.S).sort()
    expect(names).toEqual(['alice', 'carol', 'dave'])
  })

  it('filters by contains(stringAttr, :substr) — substring match', async () => {
    // "alice" contains "lic", others do not
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: 'contains(#n, :substr)',
        ExpressionAttributeNames: { '#n': 'name' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':substr': { S: 'lic' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].name.S).toBe('alice')
  })

  it('filters by contains(setAttr, :element) — set membership', async () => {
    // items 1 and 3 have "admin" in tags
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: 'contains(tags, :elem)',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':elem': { S: 'admin' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(2)
    const sks = result.Items!.map((i) => i.sk.S).sort()
    expect(sks).toEqual(['1', '3'])
  })

  it('filters by attribute_type(attr, :type)', async () => {
    // name is of type S on all items
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: 'attribute_type(#n, :t)',
        ExpressionAttributeNames: { '#n': 'name' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':t': { S: 'S' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(4)
  })

  it('filters by begins_with(attr, :prefix) on non-key attribute', async () => {
    // "carol" begins with "car"
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: 'begins_with(#n, :prefix)',
        ExpressionAttributeNames: { '#n': 'name' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':prefix': { S: 'car' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].name.S).toBe('carol')
  })

  it('filters by attr IN (:v1, :v2, :v3)', async () => {
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: '#n IN (:v1, :v2)',
        ExpressionAttributeNames: { '#n': 'name' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':v1': { S: 'alice' },
          ':v2': { S: 'carol' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(2)
    const names = result.Items!.map((i) => i.name.S).sort()
    expect(names).toEqual(['alice', 'carol'])
  })

  it('filters by NOT contains(attr, :val) — negation', async () => {
    // items without "admin" in tags: only bob (sk=2)
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: 'NOT contains(tags, :elem)',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':elem': { S: 'admin' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(2)
    const nonAdminNames = result.Items!.map((i) => i.name.S).sort()
    expect(nonAdminNames).toEqual(['bob', 'dave'])
  })

  it('filters by compound AND condition', async () => {
    // name = "alice" AND age > 20 — only alice
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: '#n = :name AND age > :minAge',
        ExpressionAttributeNames: { '#n': 'name' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':name': { S: 'alice' },
          ':minAge': { N: '20' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].name.S).toBe('alice')
  })

  it('filters by compound OR condition', async () => {
    // name = "alice" OR name = "bob"
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: '#n = :v1 OR #n = :v2',
        ExpressionAttributeNames: { '#n': 'name' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':v1': { S: 'alice' },
          ':v2': { S: 'bob' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(2)
    const names = result.Items!.map((i) => i.name.S).sort()
    expect(names).toEqual(['alice', 'bob'])
  })

  it('filters by BETWEEN on non-key attribute', async () => {
    // age BETWEEN 26 AND 34 — alice (30) and dave (28)
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: 'age BETWEEN :lo AND :hi',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':lo': { N: '26' },
          ':hi': { N: '34' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(2)
    const betweenNames = result.Items!.map((i) => i.name.S).sort()
    expect(betweenNames).toEqual(['alice', 'dave'])
  })

  it('filters by nested map path: mapAttr.nested = :val', async () => {
    // items 1 and 3 have mapAttr.nested = "deep-value"
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: 'mapAttr.nested = :val',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':val': { S: 'deep-value' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(2)
    const sks = result.Items!.map((i) => i.sk.S).sort()
    expect(sks).toEqual(['1', '3'])
  })

  it('filters by list index: listAttr[0] = :val', async () => {
    // items 1 and 3 have listAttr[0] = "first"
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        FilterExpression: 'listAttr[0] = :val',
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':val': { S: 'first' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(2)
    const sks = result.Items!.map((i) => i.sk.S).sort()
    expect(sks).toEqual(['1', '3'])
  })

  it('size() on list returns element count', async () => {
    // item 4 (dave) has listAttr with 4 elements
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        FilterExpression: 'size(#la) = :sz',
        ExpressionAttributeNames: { '#pk': 'pk', '#la': 'listAttr' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':sz': { N: '4' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].sk.S).toBe('4')
  })

  it('size() on string set returns element count', async () => {
    // item 4 (dave) has tags SS with 1 element ["viewer"]
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        FilterExpression: 'size(#t) = :sz',
        ExpressionAttributeNames: { '#pk': 'pk', '#t': 'tags' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':sz': { N: '1' },
        },
        ConsistentRead: true,
      }),
    )

    // items with exactly 1 tag element: item 2 (bob, ["user"]) and item 4 (dave, ["viewer"])
    expect(result.Items!.length).toBeGreaterThanOrEqual(1)
    const sks = result.Items!.map((i) => i.sk.S).sort()
    expect(sks).toContain('4')
  })

  it('size() on map returns key count', async () => {
    // item 4 (dave) has mapAttr with 3 keys {k1, k2, k3}
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        FilterExpression: 'size(#ma) > :sz',
        ExpressionAttributeNames: { '#pk': 'pk', '#ma': 'mapAttr' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':sz': { N: '2' },
        },
        ConsistentRead: true,
      }),
    )

    // Only item 4 has a map with more than 2 keys
    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].sk.S).toBe('4')
  })

  it('size() on binary returns byte length', async () => {
    // item 4 (dave) has binAttr with 5 bytes
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeTableDef.name,
        KeyConditionExpression: '#pk = :pk',
        FilterExpression: 'size(#ba) > :sz',
        ExpressionAttributeNames: { '#pk': 'pk', '#ba': 'binAttr' },
        ExpressionAttributeValues: {
          ':pk': { S: pk },
          ':sz': { N: '3' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toHaveLength(1)
    expect(result.Items![0].sk.S).toBe('4')
  })
})
