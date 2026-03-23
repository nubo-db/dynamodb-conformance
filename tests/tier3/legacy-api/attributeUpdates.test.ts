import {
  PutItemCommand,
  UpdateItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems, expectDynamoError } from '../../../src/helpers.js'

describe('Legacy API — AttributeUpdates (legacy UpdateExpression)', () => {
  const keys = [
    { pk: { S: 'attrupd-1' } },
    { pk: { S: 'attrupd-2' } },
    { pk: { S: 'attrupd-3' } },
    { pk: { S: 'attrupd-4' } },
    { pk: { S: 'attrupd-5' } },
    { pk: { S: 'attrupd-6' } },
  ]

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, keys)
  })

  it('AttributeUpdates with Action PUT sets attribute value', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'attrupd-1' } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-1' } },
        AttributeUpdates: {
          data: {
            Action: 'PUT',
            Value: { S: 'new-value' },
          },
        },
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-1' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item!.data.S).toBe('new-value')
  })

  it('AttributeUpdates with Action DELETE on a set removes elements', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'attrupd-2' }, tags: { SS: ['a', 'b', 'c'] } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-2' } },
        AttributeUpdates: {
          tags: {
            Action: 'DELETE',
            Value: { SS: ['b'] },
          },
        },
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-2' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item!.tags.SS).toEqual(expect.arrayContaining(['a', 'c']))
    expect(got.Item!.tags.SS).not.toContain('b')
  })

  it('AttributeUpdates with Action ADD on a number increments', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'attrupd-3' }, counter: { N: '10' } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-3' } },
        AttributeUpdates: {
          counter: {
            Action: 'ADD',
            Value: { N: '5' },
          },
        },
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-3' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item!.counter.N).toBe('15')
  })

  it('AttributeUpdates with Action ADD on a set adds elements', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'attrupd-4' }, tags: { SS: ['a', 'b'] } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-4' } },
        AttributeUpdates: {
          tags: {
            Action: 'ADD',
            Value: { SS: ['c', 'd'] },
          },
        },
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-4' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item!.tags.SS).toEqual(expect.arrayContaining(['a', 'b', 'c', 'd']))
  })

  it('Multiple attribute updates in one call', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'attrupd-5' },
          name: { S: 'old-name' },
          count: { N: '1' },
          extra: { S: 'remove-me' },
        },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-5' } },
        AttributeUpdates: {
          name: {
            Action: 'PUT',
            Value: { S: 'new-name' },
          },
          count: {
            Action: 'ADD',
            Value: { N: '9' },
          },
          extra: {
            Action: 'DELETE',
          },
        },
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-5' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item!.name.S).toBe('new-name')
    expect(got.Item!.count.N).toBe('10')
    expect(got.Item!.extra).toBeUndefined()
  })

  it('Verify updated item has correct values after PUT action', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'attrupd-6' }, val: { S: 'original' } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-6' } },
        AttributeUpdates: {
          val: { Action: 'PUT', Value: { S: 'replaced' } },
          newAttr: { Action: 'PUT', Value: { N: '42' } },
        },
        ReturnValues: 'ALL_NEW',
      }),
    )

    const got = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'attrupd-6' } },
        ConsistentRead: true,
      }),
    )
    expect(got.Item!.pk.S).toBe('attrupd-6')
    expect(got.Item!.val.S).toBe('replaced')
    expect(got.Item!.newAttr.N).toBe('42')
  })

  it('Mixing AttributeUpdates with UpdateExpression throws ValidationException', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'attrupd-1' } },
            AttributeUpdates: {
              data: { Action: 'PUT', Value: { S: 'test' } },
            },
            UpdateExpression: 'SET data = :v',
            ExpressionAttributeValues: { ':v': { S: 'test' } },
          }),
        ),
      'ValidationException',
    )
  })

  it('AttributeUpdates cannot modify key attributes', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'attrupd-1' } },
            AttributeUpdates: {
              pk: { Action: 'PUT', Value: { S: 'new-key' } },
            },
          }),
        ),
      'ValidationException',
    )
  })
})
