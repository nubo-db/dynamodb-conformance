import {
  PutItemCommand,
  GetItemCommand,
  QueryCommand,
  BatchGetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, compositeTableDef, cleanupItems } from '../../../src/helpers.js'

describe('Nested attribute projection', () => {
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
})
