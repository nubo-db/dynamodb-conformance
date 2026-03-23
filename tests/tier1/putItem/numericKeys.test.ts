import {
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashNTableDef, compositeNTableDef, cleanupItems } from '../../../src/helpers.js'

describe('PutItem — numeric hash key', () => {
  afterAll(async () => {
    await cleanupItems(hashNTableDef.name, [
      { pk: { N: '42' } },
      { pk: { N: '3.14' } },
      { pk: { N: '-100' } },
    ])
    await cleanupItems(compositeNTableDef.name, [
      { pk: { S: 'numput-1' }, sk: { N: '99' } },
    ])
  })

  it('puts and retrieves an item with numeric hash key', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashNTableDef.name,
        Item: {
          pk: { N: '42' },
          data: { S: 'numeric-key-item' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashNTableDef.name,
        Key: { pk: { N: '42' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.pk.N).toBe('42')
    expect(result.Item!.data.S).toBe('numeric-key-item')
  })

  it('puts and retrieves an item with numeric sort key', async () => {
    const key = { pk: { S: 'numput-1' }, sk: { N: '99' } }

    await ddb.send(
      new PutItemCommand({
        TableName: compositeNTableDef.name,
        Item: { ...key, value: { S: 'numeric-sort' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: compositeNTableDef.name,
        Key: key,
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.sk.N).toBe('99')
    expect(result.Item!.value.S).toBe('numeric-sort')
  })

  it('supports decimal value as numeric hash key', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashNTableDef.name,
        Item: {
          pk: { N: '3.14' },
          data: { S: 'decimal-key' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashNTableDef.name,
        Key: { pk: { N: '3.14' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.pk.N).toBe('3.14')
    expect(result.Item!.data.S).toBe('decimal-key')
  })

  it('supports negative value as numeric hash key', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashNTableDef.name,
        Item: {
          pk: { N: '-100' },
          data: { S: 'negative-key' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashNTableDef.name,
        Key: { pk: { N: '-100' } },
        ConsistentRead: true,
      }),
    )

    expect(result.Item).toBeDefined()
    expect(result.Item!.pk.N).toBe('-100')
    expect(result.Item!.data.S).toBe('negative-key')
  })
})
