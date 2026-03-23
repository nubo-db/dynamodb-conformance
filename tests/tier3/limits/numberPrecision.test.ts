import {
  PutItemCommand,
  GetItemCommand,
  QueryCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeNTableDef,
  cleanupItems,
  expectDynamoError,
} from '../../../src/helpers.js'

describe('Number precision — DynamoDB number limits and edge cases', () => {
  const hashKeys = [
    { pk: { S: 'np-38digits' } },
    { pk: { S: 'np-max-pos' } },
    { pk: { S: 'np-max-neg' } },
    { pk: { S: 'np-min-pos' } },
    { pk: { S: 'np-leading-zeros' } },
    { pk: { S: 'np-sci-notation' } },
    { pk: { S: 'np-neg-zero' } },
    { pk: { S: 'np-precise-decimal' } },
    { pk: { S: 'np-trailing-zeros-a' } },
    { pk: { S: 'np-trailing-zeros-b' } },
    { pk: { S: 'np-ns-extreme' } },
    { pk: { S: 'np-norm-leading' } },
    { pk: { S: 'np-norm-trailing-1' } },
    { pk: { S: 'np-norm-trailing-2' } },
    { pk: { S: 'np-norm-sci' } },
  ]
  const compositeKeys = [
    { pk: { S: 'np-sortkey' }, sk: { N: '99999999999999999999999999999999999999' } },
    { pk: { S: 'np-sortkey' }, sk: { N: '1E-130' } },
    { pk: { S: 'np-sortkey' }, sk: { N: '0' } },
  ]

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, hashKeys)
    await cleanupItems(compositeNTableDef.name, compositeKeys)
  })

  it('accepts a number with 38 significant digits', async () => {
    const num = '12345678901234567890123456789012345678'
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-38digits' }, val: { N: num } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-38digits' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.val.N).toBe(num)
  })

  it('rejects a number with 39 significant digits', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'np-39digits' }, val: { N: '123456789012345678901234567890123456789' } },
          }),
        ),
      'ValidationException',
    )
  })

  it('accepts maximum positive number: 9.9999999999999999999999999999999999999E+125', async () => {
    const num = '9.9999999999999999999999999999999999999E+125'
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-max-pos' }, val: { N: num } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-max-pos' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.val.N).toBeDefined()
  })

  it('rejects number just over maximum positive: 1E+126', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'np-over-max-pos' }, val: { N: '1E+126' } },
          }),
        ),
      'ValidationException',
    )
  })

  it('accepts maximum negative number: -9.9999999999999999999999999999999999999E+125', async () => {
    const num = '-9.9999999999999999999999999999999999999E+125'
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-max-neg' }, val: { N: num } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-max-neg' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.val.N).toBeDefined()
  })

  it('rejects number just over maximum negative: -1E+126', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'np-over-max-neg' }, val: { N: '-1E+126' } },
          }),
        ),
      'ValidationException',
    )
  })

  it('accepts minimum positive number: 1E-130', async () => {
    const num = '1E-130'
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-min-pos' }, val: { N: num } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-min-pos' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.val.N).toBeDefined()
  })

  it('rejects number below minimum positive: 1E-131', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'np-below-min' }, val: { N: '1E-131' } },
          }),
        ),
      'ValidationException',
    )
  })

  it('leading zeros are not counted as significant digits: 00042 treated as 42', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-leading-zeros' }, val: { N: '00042' } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-leading-zeros' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.val.N).toBe('42')
  })

  it('scientific notation round-trips correctly: 1.5E2', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-sci-notation' }, val: { N: '1.5E2' } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-sci-notation' } },
        ConsistentRead: true,
      }),
    )
    // DynamoDB may normalize to '150'
    const returned = result.Item!.val.N!
    expect(parseFloat(returned)).toBe(150)
  })

  it('negative zero: -0 behavior', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-neg-zero' }, val: { N: '-0' } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-neg-zero' } },
        ConsistentRead: true,
      }),
    )
    // DynamoDB normalizes -0 to 0
    expect(result.Item!.val.N).toBe('0')
  })

  it('very precise decimal: 0.00000000000000000000000000000000000001 (1E-38)', async () => {
    const num = '0.00000000000000000000000000000000000001'
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-precise-decimal' }, val: { N: num } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-precise-decimal' } },
        ConsistentRead: true,
      }),
    )
    const returned = result.Item!.val.N!
    // Value should represent 1E-38 regardless of notation
    expect(parseFloat(returned)).toBeCloseTo(1e-38, 45)
  })

  it('number with trailing zeros: 100 vs 1E2 represent the same value', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-trailing-zeros-a' }, val: { N: '100' } },
      }),
    )
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-trailing-zeros-b' }, val: { N: '1E2' } },
      }),
    )
    const resultA = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-trailing-zeros-a' } },
        ConsistentRead: true,
      }),
    )
    const resultB = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-trailing-zeros-b' } },
        ConsistentRead: true,
      }),
    )
    expect(parseFloat(resultA.Item!.val.N!)).toBe(parseFloat(resultB.Item!.val.N!))
  })

  it('number set with extreme values accepted', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'np-ns-extreme' },
          nums: {
            NS: [
              '9.9999999999999999999999999999999999999E+125',
              '-9.9999999999999999999999999999999999999E+125',
              '1E-130',
              '0',
            ],
          },
        },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-ns-extreme' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.nums.NS).toBeDefined()
    expect(result.Item!.nums.NS!.length).toBe(4)
  })

  it('leading zeros are stripped on round-trip', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-norm-leading' }, val: { N: '00042' } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-norm-leading' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.val.N).toBe('42')
  })

  it('trailing decimal zeros are stripped on round-trip', async () => {
    // Put N: '1.0', Get back — should return '1'
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-norm-trailing-1' }, val: { N: '1.0' } },
      }),
    )
    const result1 = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-norm-trailing-1' } },
        ConsistentRead: true,
      }),
    )
    expect(result1.Item!.val.N).toBe('1')

    // Put N: '3.1400', Get back — should return '3.14'
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-norm-trailing-2' }, val: { N: '3.1400' } },
      }),
    )
    const result2 = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-norm-trailing-2' } },
        ConsistentRead: true,
      }),
    )
    expect(result2.Item!.val.N).toBe('3.14')
  })

  it('scientific notation is normalized on round-trip', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'np-norm-sci' }, val: { N: '1.5E2' } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'np-norm-sci' } },
        ConsistentRead: true,
      }),
    )
    // DynamoDB normalizes scientific notation to decimal form
    expect(result.Item!.val.N).toBe('150')
  })

  it('numeric sort key preserves ordering at precision boundaries', async () => {
    const items = [
      { pk: { S: 'np-sortkey' }, sk: { N: '0' } },
      { pk: { S: 'np-sortkey' }, sk: { N: '1E-130' } },
      { pk: { S: 'np-sortkey' }, sk: { N: '99999999999999999999999999999999999999' } },
    ]
    for (const item of items) {
      await ddb.send(
        new PutItemCommand({ TableName: compositeNTableDef.name, Item: item }),
      )
    }
    const result = await ddb.send(
      new QueryCommand({
        TableName: compositeNTableDef.name,
        KeyConditionExpression: 'pk = :pk',
        ExpressionAttributeValues: { ':pk': { S: 'np-sortkey' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Items!.length).toBe(3)
    const sortKeys = result.Items!.map((i) => parseFloat(i.sk.N!))
    expect(sortKeys[0]).toBeLessThan(sortKeys[1])
    expect(sortKeys[1]).toBeLessThan(sortKeys[2])
  })
})
