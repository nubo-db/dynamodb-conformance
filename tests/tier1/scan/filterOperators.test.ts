import {
  PutItemCommand,
  ScanCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Scan — filter operators on different types', () => {
  const items = [
    {
      pk: { S: 'fo-1' },
      strVal: { S: 'apple' },
      numVal: { N: '10' },
      binVal: { B: new Uint8Array([1, 2]) },
      setVal: { SS: ['a', 'b'] },
    },
    {
      pk: { S: 'fo-2' },
      strVal: { S: 'banana' },
      numVal: { N: '20' },
      binVal: { B: new Uint8Array([3, 4]) },
      setVal: { SS: ['b', 'c'] },
    },
    {
      pk: { S: 'fo-3' },
      strVal: { S: 'cherry' },
      numVal: { N: '30' },
      binVal: { B: new Uint8Array([5, 6]) },
      setVal: { SS: ['a', 'c'] },
    },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      items.map((item) => ({ pk: item.pk })),
    )
  })

  it('EQ on string — exact match', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#sv = :val',
        ExpressionAttributeNames: { '#sv': 'strVal' },
        ExpressionAttributeValues: { ':val': { S: 'banana' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(1)
    expect(result.Items![0].pk.S).toBe('fo-2')
  })

  it('NE on number — not equal', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#nv <> :val AND begins_with(#pk, :prefix)',
        ExpressionAttributeNames: { '#nv': 'numVal', '#pk': 'pk' },
        ExpressionAttributeValues: {
          ':val': { N: '20' },
          ':prefix': { S: 'fo-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(2)
    const pks = result.Items!.map((i) => i.pk.S).sort()
    expect(pks).toEqual(['fo-1', 'fo-3'])
  })

  it('GT on number — greater than', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#nv > :val AND begins_with(#pk, :prefix)',
        ExpressionAttributeNames: { '#nv': 'numVal', '#pk': 'pk' },
        ExpressionAttributeValues: {
          ':val': { N: '15' },
          ':prefix': { S: 'fo-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(2)
    const pks = result.Items!.map((i) => i.pk.S).sort()
    expect(pks).toEqual(['fo-2', 'fo-3'])
  })

  it('LE on string — lexicographic less-than-or-equal', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#sv <= :val AND begins_with(#pk, :prefix)',
        ExpressionAttributeNames: { '#sv': 'strVal', '#pk': 'pk' },
        ExpressionAttributeValues: {
          ':val': { S: 'banana' },
          ':prefix': { S: 'fo-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(2)
    const pks = result.Items!.map((i) => i.pk.S).sort()
    expect(pks).toEqual(['fo-1', 'fo-2'])
  })

  it('CONTAINS on string — substring match', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: 'contains(#sv, :sub) AND begins_with(#pk, :prefix)',
        ExpressionAttributeNames: { '#sv': 'strVal', '#pk': 'pk' },
        ExpressionAttributeValues: {
          ':sub': { S: 'an' },
          ':prefix': { S: 'fo-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    // 'banana' contains 'an'
    expect(result.Items!.length).toBe(1)
    expect(result.Items![0].pk.S).toBe('fo-2')
  })

  it('attribute_exists — verify attribute present', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: 'attribute_exists(#sv) AND begins_with(#pk, :prefix)',
        ExpressionAttributeNames: { '#sv': 'strVal', '#pk': 'pk' },
        ExpressionAttributeValues: {
          ':prefix': { S: 'fo-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(3)
  })
})

describe('Scan — binary attribute comparisons', () => {
  const items = [
    { pk: { S: 'fo-1' }, binVal: { B: new Uint8Array([1, 2]) } },
    { pk: { S: 'fo-2' }, binVal: { B: new Uint8Array([3, 4]) } },
    { pk: { S: 'fo-3' }, binVal: { B: new Uint8Array([5, 6]) } },
  ]

  // Items already inserted by outer describe; these tests reuse them.
  // We re-put to ensure they exist if run independently.
  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      items.map((item) => ({ pk: item.pk })),
    )
  })

  it('Binary GT comparison (byte-wise)', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#bv > :val AND begins_with(#pk, :prefix)',
        ExpressionAttributeNames: { '#bv': 'binVal', '#pk': 'pk' },
        ExpressionAttributeValues: {
          ':val': { B: new Uint8Array([3, 4]) },
          ':prefix': { S: 'fo-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(1)
    expect(result.Items![0].pk.S).toBe('fo-3')
  })

  it('Binary BEGINS_WITH', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: 'begins_with(#bv, :prefix) AND begins_with(#pk, :pkPrefix)',
        ExpressionAttributeNames: { '#bv': 'binVal', '#pk': 'pk' },
        ExpressionAttributeValues: {
          ':prefix': { B: new Uint8Array([3]) },
          ':pkPrefix': { S: 'fo-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(1)
    expect(result.Items![0].pk.S).toBe('fo-2')
  })

  it('Binary BETWEEN', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#bv BETWEEN :lo AND :hi AND begins_with(#pk, :prefix)',
        ExpressionAttributeNames: { '#bv': 'binVal', '#pk': 'pk' },
        ExpressionAttributeValues: {
          ':lo': { B: new Uint8Array([1, 2]) },
          ':hi': { B: new Uint8Array([3, 4]) },
          ':prefix': { S: 'fo-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(2)
    const pks = result.Items!.map((i) => i.pk.S).sort()
    expect(pks).toEqual(['fo-1', 'fo-2'])
  })
})

describe('Scan — set equality', () => {
  const items = [
    { pk: { S: 'fo-1' }, setVal: { SS: ['a', 'b'] } },
    { pk: { S: 'fo-2' }, setVal: { SS: ['b', 'c'] } },
    { pk: { S: 'fo-3' }, setVal: { SS: ['a', 'c'] } },
  ]

  beforeAll(async () => {
    await Promise.all(
      items.map((item) =>
        ddb.send(
          new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
        ),
      ),
    )
  })

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      items.map((item) => ({ pk: item.pk })),
    )
  })

  it('Set EQ — matches regardless of element order', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#sv = :expected AND begins_with(#pk, :prefix)',
        ExpressionAttributeNames: { '#sv': 'setVal', '#pk': 'pk' },
        ExpressionAttributeValues: {
          // Provide elements in different order than inserted ('a', 'b')
          ':expected': { SS: ['b', 'a'] },
          ':prefix': { S: 'fo-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(1)
    expect(result.Items![0].pk.S).toBe('fo-1')
  })

  it('Set NE — verify inequality', async () => {
    const result = await ddb.send(
      new ScanCommand({
        TableName: hashTableDef.name,
        FilterExpression: '#sv <> :expected AND begins_with(#pk, :prefix)',
        ExpressionAttributeNames: { '#sv': 'setVal', '#pk': 'pk' },
        ExpressionAttributeValues: {
          ':expected': { SS: ['a', 'b'] },
          ':prefix': { S: 'fo-' },
        },
        ConsistentRead: true,
      }),
    )

    expect(result.Items).toBeDefined()
    expect(result.Items!.length).toBe(2)
    const pks = result.Items!.map((i) => i.pk.S).sort()
    expect(pks).toEqual(['fo-2', 'fo-3'])
  })
})
