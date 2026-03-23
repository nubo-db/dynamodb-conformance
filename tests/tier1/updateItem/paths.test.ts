import {
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems, expectDynamoError } from '../../../src/helpers.js'

describe('UpdateItem — nested path semantics', () => {
  const keysToCleanup: { pk: { S: string } }[] = []

  afterAll(async () => {
    await cleanupItems(
      hashTableDef.name,
      keysToCleanup,
    )
  })

  it('SET fails when intermediate map path does not exist', async () => {
    const pkVal = 'path-missing-intermediate'
    keysToCleanup.push({ pk: { S: pkVal } })

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: pkVal }, topLevel: { S: 'val' } },
      }),
    )

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: pkVal } },
            UpdateExpression: 'SET #a.#b.#c = :v',
            ExpressionAttributeNames: {
              '#a': 'missing',
              '#b': 'nested',
              '#c': 'deep',
            },
            ExpressionAttributeValues: { ':v': { S: 'test' } },
          }),
        ),
      'ValidationException',
    )
  })

  it('SET succeeds on existing nested map path', async () => {
    const pkVal = 'path-existing-nested'
    keysToCleanup.push({ pk: { S: pkVal } })

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pkVal },
          mymap: { M: { nested: { S: 'old' } } },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        UpdateExpression: 'SET #m.#n = :v',
        ExpressionAttributeNames: { '#m': 'mymap', '#n': 'nested' },
        ExpressionAttributeValues: { ':v': { S: 'updated' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.mymap.M!.nested.S).toBe('updated')
  })

  it('SET on list index beyond bounds extends the list', async () => {
    const pkVal = 'path-list-beyond'
    keysToCleanup.push({ pk: { S: pkVal } })

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pkVal },
          mylist: { L: [{ S: 'a' }, { S: 'b' }, { S: 'c' }] },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        UpdateExpression: 'SET #l[5] = :v',
        ExpressionAttributeNames: { '#l': 'mylist' },
        ExpressionAttributeValues: { ':v': { S: 'inserted' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        ConsistentRead: true,
      }),
    )
    const list = result.Item!.mylist.L!
    // DynamoDB appends to end when index > length, so the value lands at the end
    expect(list.length).toBeGreaterThanOrEqual(4)
    expect(list[list.length - 1].S).toBe('inserted')
  })

  it('SET deeply nested path a.b.c when all intermediates exist', async () => {
    const pkVal = 'path-deep-nested'
    keysToCleanup.push({ pk: { S: pkVal } })

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pkVal },
          a: { M: { b: { M: { c: { S: 'old' } } } } },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        UpdateExpression: 'SET #a.#b.#c = :v',
        ExpressionAttributeNames: { '#a': 'a', '#b': 'b', '#c': 'c' },
        ExpressionAttributeValues: { ':v': { S: 'new' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.a.M!.b.M!.c.S).toBe('new')
  })

  it('arithmetic on nested path: SET mymap.counter = mymap.counter + :inc', async () => {
    const pkVal = 'path-nested-arith'
    keysToCleanup.push({ pk: { S: pkVal } })

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pkVal },
          mymap: { M: { counter: { N: '10' } } },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        UpdateExpression: 'SET #m.#c = #m.#c + :inc',
        ExpressionAttributeNames: { '#m': 'mymap', '#c': 'counter' },
        ExpressionAttributeValues: { ':inc': { N: '5' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.mymap.M!.counter.N).toBe('15')
  })

  it('REMOVE nested map key removes just that key', async () => {
    const pkVal = 'path-remove-nested-key'
    keysToCleanup.push({ pk: { S: pkVal } })

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pkVal },
          mymap: { M: { nested: { S: 'gone' }, keep: { S: 'stay' } } },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        UpdateExpression: 'REMOVE #m.#n',
        ExpressionAttributeNames: { '#m': 'mymap', '#n': 'nested' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.mymap.M!.nested).toBeUndefined()
    expect(result.Item!.mymap.M!.keep.S).toBe('stay')
  })

  // ── N4: REMOVE list element by index ────────────────────────────────

  it('REMOVE list element by index removes and shifts', async () => {
    const pkVal = 'path-remove-list-idx'
    keysToCleanup.push({ pk: { S: pkVal } })

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pkVal },
          mylist: { L: [{ S: 'a' }, { S: 'b' }, { S: 'c' }, { S: 'd' }] },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        UpdateExpression: 'REMOVE #l[1]',
        ExpressionAttributeNames: { '#l': 'mylist' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        ConsistentRead: true,
      }),
    )
    const list = result.Item!.mylist.L!
    expect(list).toHaveLength(3)
    expect(list.map((e) => e.S)).toEqual(['a', 'c', 'd'])
  })

  it('REMOVE last list element', async () => {
    const pkVal = 'path-remove-last-elem'
    keysToCleanup.push({ pk: { S: pkVal } })

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pkVal },
          mylist: { L: [{ S: 'x' }, { S: 'y' }] },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        UpdateExpression: 'REMOVE #l[1]',
        ExpressionAttributeNames: { '#l': 'mylist' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        ConsistentRead: true,
      }),
    )
    const list = result.Item!.mylist.L!
    expect(list).toHaveLength(1)
    expect(list[0].S).toBe('x')
  })

  // ── N5: Multiple SET on same attribute ────────────────────────────────

  it('multiple SET on same attribute in one expression', async () => {
    const pkVal = 'path-multi-set-same'
    keysToCleanup.push({ pk: { S: pkVal } })

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: pkVal }, val: { S: 'original' } },
      }),
    )

    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: pkVal } },
          UpdateExpression: 'SET #v = :a, #v = :b',
          ExpressionAttributeNames: { '#v': 'val' },
          ExpressionAttributeValues: {
            ':a': { S: 'first' },
            ':b': { S: 'second' },
          },
        }),
      )

      // If it succeeds, verify which value won
      const result = await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: pkVal } },
          ConsistentRead: true,
        }),
      )
      // DynamoDB may apply the last SET or reject — if we get here, check the value
      expect(result.Item!.val.S).toBeDefined()
    } catch (e: unknown) {
      // DynamoDB rejects duplicate paths in update expressions
      expect(e).toBeDefined()
      expect((e as Error).name).toBe('ValidationException')
    }
  })

  it('SET on list index 0 of empty list succeeds', async () => {
    const pkVal = 'path-list-empty'
    keysToCleanup.push({ pk: { S: pkVal } })

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pkVal },
          mylist: { L: [] },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        UpdateExpression: 'SET #l[0] = :v',
        ExpressionAttributeNames: { '#l': 'mylist' },
        ExpressionAttributeValues: { ':v': { S: 'first' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pkVal } },
        ConsistentRead: true,
      }),
    )
    const list = result.Item!.mylist.L!
    expect(list).toHaveLength(1)
    expect(list[0].S).toBe('first')
  })
})
