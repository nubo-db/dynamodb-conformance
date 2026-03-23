import {
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems } from '../../../src/helpers.js'

describe('PutItem — data types depth and edge cases', () => {
  const keys = [
    { pk: { S: 'dt-nested-map' } },
    { pk: { S: 'dt-mixed-list' } },
    { pk: { S: 'dt-map-list-map' } },
    { pk: { S: 'dt-empty-string' } },
    { pk: { S: 'dt-large-string' } },
    { pk: { S: 'dt-num-38digits' } },
    { pk: { S: 'dt-num-pos-max' } },
    { pk: { S: 'dt-num-neg-max' } },
  ]

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, keys)
  })

  it('deeply nested map (3 levels) round-trips correctly', async () => {
    const item = {
      pk: { S: 'dt-nested-map' },
      nested: {
        M: {
          a: {
            M: {
              b: {
                M: {
                  c: { S: 'deep' },
                },
              },
            },
          },
        },
      },
    }

    await ddb.send(
      new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'dt-nested-map' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item!.nested.M!.a.M!.b.M!.c.S).toBe('deep')
  })

  it('list containing mixed types round-trips correctly', async () => {
    const item = {
      pk: { S: 'dt-mixed-list' },
      mixed: {
        L: [
          { S: 'str' },
          { N: '1' },
          { BOOL: true },
          { NULL: true },
          { M: { x: { S: 'y' } } },
        ],
      },
    }

    await ddb.send(
      new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'dt-mixed-list' } },
        ConsistentRead: true,
      }),
    )

    const list = result.Item!.mixed.L!
    expect(list).toHaveLength(5)
    expect(list[0].S).toBe('str')
    expect(list[1].N).toBe('1')
    expect(list[2].BOOL).toBe(true)
    expect(list[3].NULL).toBe(true)
    expect(list[4].M!.x.S).toBe('y')
  })

  it('map containing a list containing a map round-trips correctly', async () => {
    const item = {
      pk: { S: 'dt-map-list-map' },
      outer: {
        M: {
          items: {
            L: [
              {
                M: {
                  name: { S: 'first' },
                  value: { N: '100' },
                },
              },
              {
                M: {
                  name: { S: 'second' },
                  value: { N: '200' },
                },
              },
            ],
          },
        },
      },
    }

    await ddb.send(
      new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'dt-map-list-map' } },
        ConsistentRead: true,
      }),
    )

    const items = result.Item!.outer.M!.items.L!
    expect(items).toHaveLength(2)
    expect(items[0].M!.name.S).toBe('first')
    expect(items[0].M!.value.N).toBe('100')
    expect(items[1].M!.name.S).toBe('second')
    expect(items[1].M!.value.N).toBe('200')
  })

  it('empty string value in non-key attribute succeeds', async () => {
    const item = {
      pk: { S: 'dt-empty-string' },
      emptyStr: { S: '' },
    }

    await ddb.send(
      new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'dt-empty-string' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item!.emptyStr.S).toBe('')
  })

  it('large string value (100KB) succeeds', async () => {
    const largeString = 'x'.repeat(100 * 1024)
    const item = {
      pk: { S: 'dt-large-string' },
      big: { S: largeString },
    }

    await ddb.send(
      new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'dt-large-string' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item!.big.S).toBe(largeString)
  })

  it('number with 38 significant digits (max precision)', async () => {
    const preciseNumber = '12345678901234567890123456789012345678'
    const item = {
      pk: { S: 'dt-num-38digits' },
      precise: { N: preciseNumber },
    }

    await ddb.send(
      new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'dt-num-38digits' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item!.precise.N).toBe(preciseNumber)
  })

  it('number at positive magnitude boundary: 9.9999999999999999999999999999999999999E+125', async () => {
    const maxPositive = '9.9999999999999999999999999999999999999E+125'
    const item = {
      pk: { S: 'dt-num-pos-max' },
      maxNum: { N: maxPositive },
    }

    await ddb.send(
      new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'dt-num-pos-max' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item!.maxNum.N).toBeDefined()
    // DynamoDB may normalize the representation, so parse and compare
    expect(Number.parseFloat(result.Item!.maxNum.N!)).toBeCloseTo(
      Number.parseFloat(maxPositive),
      -120,
    )
  })

  it('number at negative magnitude boundary: -9.9999999999999999999999999999999999999E+125', async () => {
    const maxNegative = '-9.9999999999999999999999999999999999999E+125'
    const item = {
      pk: { S: 'dt-num-neg-max' },
      minNum: { N: maxNegative },
    }

    await ddb.send(
      new PutItemCommand({ TableName: hashTableDef.name, Item: item }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'dt-num-neg-max' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item!.minNum.N).toBeDefined()
    expect(Number.parseFloat(result.Item!.minNum.N!)).toBeCloseTo(
      Number.parseFloat(maxNegative),
      -120,
    )
  })
})
