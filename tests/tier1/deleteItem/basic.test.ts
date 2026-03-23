import {
  PutItemCommand,
  GetItemCommand,
  DeleteItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  expectDynamoError,
  cleanupItems,
} from '../../../src/helpers.js'

describe('DeleteItem — basic', () => {
  it('deletes an existing item', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'del-1' }, data: { S: 'to-delete' } },
      }),
    )

    await ddb.send(
      new DeleteItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'del-1' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'del-1' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item).toBeUndefined()
  })

  it('succeeds silently when deleting a non-existent item', async () => {
    // DeleteItem on a non-existent key should NOT throw
    const result = await ddb.send(
      new DeleteItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'never-existed' } },
      }),
    )
    // No error, no Attributes returned
    expect(result.Attributes).toBeUndefined()
  })

  it('deletes an item from a composite key table', async () => {
    const key = { pk: { S: 'del-comp' }, sk: { S: 'sort1' } }

    await ddb.send(
      new PutItemCommand({
        TableName: compositeTableDef.name,
        Item: { ...key, value: { S: 'delete-me' } },
      }),
    )

    await ddb.send(
      new DeleteItemCommand({ TableName: compositeTableDef.name, Key: key }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: compositeTableDef.name,
        Key: key,
        ConsistentRead: true,
      }),
    )
    expect(result.Item).toBeUndefined()
  })
})

describe('DeleteItem — return values', () => {
  it('returns ALL_OLD when deleting an existing item', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'del-ret-1' }, data: { S: 'old-data' } },
      }),
    )

    const result = await ddb.send(
      new DeleteItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'del-ret-1' } },
        ReturnValues: 'ALL_OLD',
      }),
    )

    expect(result.Attributes).toBeDefined()
    expect(result.Attributes!.data.S).toBe('old-data')
  })

  it('returns no Attributes with ALL_OLD when item does not exist', async () => {
    const result = await ddb.send(
      new DeleteItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'del-ret-nonexist' } },
        ReturnValues: 'ALL_OLD',
      }),
    )

    expect(result.Attributes).toBeUndefined()
  })
})

describe('DeleteItem — ConditionExpression', () => {
  it('deletes when condition is met', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'del-cond-1' }, status: { S: 'pending' } },
      }),
    )

    await ddb.send(
      new DeleteItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'del-cond-1' } },
        ConditionExpression: '#s = :v',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':v': { S: 'pending' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'del-cond-1' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item).toBeUndefined()
  })

  it('fails with ConditionalCheckFailedException when condition is not met', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'del-cond-2' }, status: { S: 'active' } },
      }),
    )

    await expectDynamoError(
      () => ddb.send(
        new DeleteItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'del-cond-2' } },
          ConditionExpression: '#s = :v',
          ExpressionAttributeNames: { '#s': 'status' },
          ExpressionAttributeValues: { ':v': { S: 'pending' } },
        }),
      ),
      'ConditionalCheckFailedException',
    )

    // Item should still exist
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'del-cond-2' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item).toBeDefined()

    // cleanup
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'del-cond-2' } }])
  })
})
