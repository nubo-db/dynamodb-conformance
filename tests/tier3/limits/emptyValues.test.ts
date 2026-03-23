import {
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  cleanupItems,
  expectDynamoError,
} from '../../../src/helpers.js'

describe('Empty values — strings, binary, and sets', () => {
  const hashKeys = [
    { pk: { S: 'ev-empty-str' } },
    { pk: { S: 'ev-empty-str-list' } },
    { pk: { S: 'ev-empty-bin' } },
  ]
  const compositeKeys = [
    { pk: { S: 'ev-composite' }, sk: { S: 'placeholder' } },
  ]

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, hashKeys)
    await cleanupItems(compositeTableDef.name, compositeKeys)
  })

  it('empty string in non-key S attribute is accepted', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'ev-empty-str' }, attr: { S: '' } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'ev-empty-str' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.attr.S).toBe('')
  })

  it('empty string as hash key value is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: '' } },
          }),
        ),
      'ValidationException',
    )
  })

  it('empty string as sort key value is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: compositeTableDef.name,
            Item: { pk: { S: 'ev-composite' }, sk: { S: '' } },
          }),
        ),
      'ValidationException',
    )
  })

  it('empty binary (B: empty Uint8Array) in non-key attribute is accepted', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'ev-empty-bin' }, attr: { B: new Uint8Array([]) } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'ev-empty-bin' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item).toBeDefined()
    expect(result.Item!.attr.B).toBeDefined()
  })

  it('empty string set (SS) is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'ev-empty-ss' }, attr: { SS: [] } },
          }),
        ),
      'ValidationException',
      /An string set {2}may not be empty/,
    )
  })

  it('empty number set (NS) is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'ev-empty-ns' }, attr: { NS: [] } },
          }),
        ),
      'ValidationException',
      /An number set {2}may not be empty/,
    )
  })

  it('empty binary set (BS) is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'ev-empty-bs' }, attr: { BS: [] } },
          }),
        ),
      'ValidationException',
      'Binary sets should not be empty',
    )
  })

  it('empty set nested inside a Map is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: {
              pk: { S: 'ev-map-empty-set' },
              outer: { M: { inner: { SS: [] } } },
            },
          }),
        ),
      'ValidationException',
    )
  })

  it('empty set nested inside a List is rejected', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: {
              pk: { S: 'ev-list-empty-set' },
              items: { L: [{ SS: [] }] },
            },
          }),
        ),
      'ValidationException',
    )
  })

  it('empty string inside a List element is accepted', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'ev-empty-str-list' },
          items: { L: [{ S: '' }, { S: 'hello' }] },
        },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'ev-empty-str-list' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.items.L![0].S).toBe('')
    expect(result.Item!.items.L![1].S).toBe('hello')
  })
})
