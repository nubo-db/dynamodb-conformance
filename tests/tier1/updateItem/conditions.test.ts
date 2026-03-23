import {
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
  ConditionalCheckFailedException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, cleanupItems, expectDynamoError } from '../../../src/helpers.js'

describe('UpdateItem — ConditionExpression', () => {
  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [
      { pk: { S: 'upd-cond-pass' } },
      { pk: { S: 'upd-cond-fail' } },
      { pk: { S: 'upd-cond-ane' } },
      { pk: { S: 'upd-cond-cmp' } },
      { pk: { S: 'upd-cond-and' } },
      { pk: { S: 'upd-cond-rvcf' } },
    ])
  })

  it('succeeds when ConditionExpression passes', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-cond-pass' }, status: { S: 'active' } },
      }),
    )

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-cond-pass' } },
        UpdateExpression: 'SET #d = :v',
        ConditionExpression: '#s = :expected',
        ExpressionAttributeNames: { '#d': 'data', '#s': 'status' },
        ExpressionAttributeValues: {
          ':v': { S: 'updated' },
          ':expected': { S: 'active' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-cond-pass' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.data.S).toBe('updated')
  })

  it('throws ConditionalCheckFailedException when condition fails', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-cond-fail' }, status: { S: 'inactive' } },
      }),
    )

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'upd-cond-fail' } },
            UpdateExpression: 'SET #d = :v',
            ConditionExpression: '#s = :expected',
            ExpressionAttributeNames: { '#d': 'data', '#s': 'status' },
            ExpressionAttributeValues: {
              ':v': { S: 'nope' },
              ':expected': { S: 'active' },
            },
          }),
        ),
      'ConditionalCheckFailedException',
    )
  })

  it('attribute_not_exists — update only if attribute is missing', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-cond-ane' }, x: { N: '1' } },
      }),
    )

    // Should succeed: 'locked' does not exist
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-cond-ane' } },
        UpdateExpression: 'SET locked = :v',
        ConditionExpression: 'attribute_not_exists(locked)',
        ExpressionAttributeValues: { ':v': { BOOL: true } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-cond-ane' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.locked.BOOL).toBe(true)

    // Should fail: 'locked' now exists
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'upd-cond-ane' } },
            UpdateExpression: 'SET locked = :v',
            ConditionExpression: 'attribute_not_exists(locked)',
            ExpressionAttributeValues: { ':v': { BOOL: true } },
          }),
        ),
      'ConditionalCheckFailedException',
    )
  })

  it('comparison operator — attr > :val', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'upd-cond-cmp' }, score: { N: '50' } },
      }),
    )

    // Should succeed: 50 > 10
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-cond-cmp' } },
        UpdateExpression: 'SET score = :newval',
        ConditionExpression: 'score > :threshold',
        ExpressionAttributeValues: {
          ':newval': { N: '100' },
          ':threshold': { N: '10' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-cond-cmp' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.score.N).toBe('100')

    // Should fail: 100 > 200 is false
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'upd-cond-cmp' } },
            UpdateExpression: 'SET score = :newval',
            ConditionExpression: 'score > :threshold',
            ExpressionAttributeValues: {
              ':newval': { N: '999' },
              ':threshold': { N: '200' },
            },
          }),
        ),
      'ConditionalCheckFailedException',
    )
  })

  it('AND — both conditions must be met', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'upd-cond-and' },
          status: { S: 'active' },
          score: { N: '75' },
        },
      }),
    )

    // Should succeed: status = active AND score > 50
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-cond-and' } },
        UpdateExpression: 'SET promoted = :v',
        ConditionExpression: '#s = :status AND score > :min',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: {
          ':v': { BOOL: true },
          ':status': { S: 'active' },
          ':min': { N: '50' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-cond-and' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.promoted.BOOL).toBe(true)

    // Should fail: status = active but score > 100 is false (score is 75)
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'upd-cond-and' } },
            UpdateExpression: 'SET extra = :v',
            ConditionExpression: '#s = :status AND score > :min',
            ExpressionAttributeNames: { '#s': 'status' },
            ExpressionAttributeValues: {
              ':v': { BOOL: true },
              ':status': { S: 'active' },
              ':min': { N: '100' },
            },
          }),
        ),
      'ConditionalCheckFailedException',
    )
  })

  it('ReturnValuesOnConditionCheckFailure ALL_OLD returns existing item on failure', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'upd-cond-rvcf' },
          status: { S: 'locked' },
          data: { S: 'important' },
        },
      }),
    )

    try {
      await ddb.send(
        new UpdateItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: 'upd-cond-rvcf' } },
          UpdateExpression: 'SET #d = :v',
          ConditionExpression: '#s = :expected',
          ExpressionAttributeNames: { '#d': 'data', '#s': 'status' },
          ExpressionAttributeValues: {
            ':v': { S: 'changed' },
            ':expected': { S: 'active' },
          },
          ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(ConditionalCheckFailedException)
      const err = e as ConditionalCheckFailedException
      expect(err.Item).toBeDefined()
      expect(err.Item!.pk.S).toBe('upd-cond-rvcf')
      expect(err.Item!.status.S).toBe('locked')
      expect(err.Item!.data.S).toBe('important')
    }
  })
})
