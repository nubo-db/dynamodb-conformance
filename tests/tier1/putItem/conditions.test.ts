import {
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, expectDynamoError, cleanupItems } from '../../../src/helpers.js'

describe('PutItem — ConditionExpression', () => {
  const pk = 'put-cond'

  beforeAll(async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: pk }, status: { S: 'active' }, count: { N: '5' } },
      }),
    )
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [
      { pk: { S: pk } },
      { pk: { S: 'put-cond-new' } },
    ])
  })

  it('succeeds when attribute_not_exists condition is met', async () => {
    const newPk = 'put-cond-new'
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: newPk }, data: { S: 'fresh' } },
        ConditionExpression: 'attribute_not_exists(pk)',
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: newPk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.data.S).toBe('fresh')
  })

  it('fails with ConditionalCheckFailedException when attribute_not_exists is not met', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: pk }, data: { S: 'should not overwrite' } },
          ConditionExpression: 'attribute_not_exists(pk)',
        }),
      ),
      'ConditionalCheckFailedException',
    )
  })

  it('succeeds when attribute_exists condition is met', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: pk }, status: { S: 'updated' }, count: { N: '5' } },
        ConditionExpression: 'attribute_exists(#s)',
        ExpressionAttributeNames: { '#s': 'status' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.status.S).toBe('updated')
  })

  it('supports comparison operators in conditions', async () => {
    // count = 5, so count > 3 should pass
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          status: { S: 'compared' },
          count: { N: '5' },
        },
        ConditionExpression: '#c > :min',
        ExpressionAttributeNames: { '#c': 'count' },
        ExpressionAttributeValues: { ':min': { N: '3' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.status.S).toBe('compared')
  })

  it('fails comparison when condition is not met', async () => {
    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: pk }, status: { S: 'nope' }, count: { N: '5' } },
          ConditionExpression: '#c > :min',
          ExpressionAttributeNames: { '#c': 'count' },
          ExpressionAttributeValues: { ':min': { N: '100' } },
        }),
      ),
      'ConditionalCheckFailedException',
    )
  })
})
