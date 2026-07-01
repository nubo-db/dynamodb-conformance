import {
  BatchGetItemCommand,
  PutItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('BatchGetItem — basic', { tags: ['batch', 'data-plane'] }, () => {
  const items = Array.from({ length: 5 }, (_, i) => ({
    pk: { S: `bg-${i}` },
    val: { N: String(i * 10) },
  }))

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

  it('retrieves multiple items in a single batch', async () => {
    const result = await ddb.send(
      new BatchGetItemCommand({
        RequestItems: {
          [hashTableDef.name]: {
            Keys: items.map((i) => ({ pk: i.pk })),
            ConsistentRead: true,
          },
        },
      }),
    )

    const returned = result.Responses![hashTableDef.name]
    expect(returned).toHaveLength(5)
  })

  it('handles a mix of existing and non-existing keys', async () => {
    const result = await ddb.send(
      new BatchGetItemCommand({
        RequestItems: {
          [hashTableDef.name]: {
            Keys: [
              { pk: { S: 'bg-0' } },
              { pk: { S: 'does-not-exist-123' } },
              { pk: { S: 'bg-1' } },
            ],
            ConsistentRead: true,
          },
        },
      }),
    )

    const returned = result.Responses![hashTableDef.name]
    expect(returned).toHaveLength(2)
  })

  it('returns no UnprocessedKeys for small batches', async () => {
    const result = await ddb.send(
      new BatchGetItemCommand({
        RequestItems: {
          [hashTableDef.name]: {
            Keys: [{ pk: { S: 'bg-0' } }],
            ConsistentRead: true,
          },
        },
      }),
    )

    const unprocessed = result.UnprocessedKeys?.[hashTableDef.name]
    expect(unprocessed).toBeUndefined()
  })
})

describe('BatchGetItem — ProjectionExpression', { tags: ['batch', 'data-plane'] }, () => {
  beforeAll(async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'bg-proj' },
          a: { S: 'alpha' },
          b: { S: 'beta' },
          c: { N: '99' },
        },
      }),
    )
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'bg-proj' } }])
  })

  it('returns only projected attributes', async () => {
    const result = await ddb.send(
      new BatchGetItemCommand({
        RequestItems: {
          [hashTableDef.name]: {
            Keys: [{ pk: { S: 'bg-proj' } }],
            ProjectionExpression: 'a, c',
            ConsistentRead: true,
          },
        },
      }),
    )

    const item = result.Responses![hashTableDef.name][0]
    expect(item.a.S).toBe('alpha')
    expect(item.c.N).toBe('99')
    expect(item.b).toBeUndefined()
    // BatchGetItem does NOT auto-include key attributes in projection
    expect(item.pk).toBeUndefined()
  })
})

describe('BatchGetItem — projection matching nothing', { tags: ['batch', 'data-plane'] }, () => {
  const pk = 'bg-emptyproj'

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

  it('keeps an empty {} entry when the projection matches no attribute on a present item', async () => {
    const result = await ddb.send(
      new BatchGetItemCommand({
        RequestItems: {
          [hashTableDef.name]: {
            Keys: [{ pk: { S: pk } }],
            ProjectionExpression: 'nonexistent',
            ConsistentRead: true,
          },
        },
      }),
    )

    // The item exists but the projection selects nothing. BatchGetItem does NOT
    // drop the item the way it drops non-existent keys; it keeps an empty {}
    // entry in the Responses array.
    const items = result.Responses![hashTableDef.name]
    expect(items).toHaveLength(1)
    expect(Object.keys(items[0])).toHaveLength(0)
  })
})

describe('BatchGetItem — multiple tables', { tags: ['batch', 'data-plane'] }, () => {
  const hashKey = { pk: { S: 'bg-multi-hash' } }
  const compositeKey = { pk: { S: 'bg-multi-comp' }, sk: { S: 'sk1' } }

  beforeAll(async () => {
    await Promise.all([
      ddb.send(new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { ...hashKey, val: { S: 'from-hash' } },
      })),
      ddb.send(new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: { ...compositeKey, val: { S: 'from-composite' } },
      })),
    ])
  })

  afterAll(async () => {
    await Promise.all([
      cleanupItems(hashTableDef.name, [hashKey]),
      cleanupItems(compositeTableDef.name, [compositeKey]),
    ])
  })

  it('retrieves items from multiple tables in one batch', async () => {
    const result = await ddb.send(new BatchGetItemCommand({
      RequestItems: {
        [hashTableDef.name]: {
          Keys: [hashKey],
          ConsistentRead: true,
        },
        [compositeTableDef.name]: {
          Keys: [compositeKey],
          ConsistentRead: true,
        },
      },
    }))

    const hashItems = result.Responses![hashTableDef.name]
    expect(hashItems).toHaveLength(1)
    expect(hashItems[0].val.S).toBe('from-hash')

    const compositeItems = result.Responses![compositeTableDef.name]
    expect(compositeItems).toHaveLength(1)
    expect(compositeItems[0].val.S).toBe('from-composite')
  })

  it('handles mix of found and not-found across multiple tables', async () => {
    const result = await ddb.send(new BatchGetItemCommand({
      RequestItems: {
        [hashTableDef.name]: {
          Keys: [hashKey],
          ConsistentRead: true,
        },
        [compositeTableDef.name]: {
          Keys: [{ pk: { S: 'bg-multi-missing' }, sk: { S: 'nope' } }],
          ConsistentRead: true,
        },
      },
    }))

    const hashItems = result.Responses![hashTableDef.name]
    expect(hashItems).toHaveLength(1)
    expect(hashItems[0].val.S).toBe('from-hash')

    const compositeItems = result.Responses![compositeTableDef.name]
    expect(compositeItems).toHaveLength(0)
  })

  it('returns UnprocessedKeys as an empty object on full success', async () => {
    // Real AWS always includes UnprocessedKeys; on a fully-successful batch it
    // is an empty object, not omitted.
    const result = await ddb.send(
      new BatchGetItemCommand({
        RequestItems: {
          [hashTableDef.name]: { Keys: [{ pk: { S: 'bg-0' } }], ConsistentRead: true },
        },
      }),
    )
    expect(result.UnprocessedKeys).toEqual({})
  })
})
