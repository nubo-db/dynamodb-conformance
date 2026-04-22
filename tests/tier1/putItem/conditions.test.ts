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
      { pk: { S: 'put-cond-ne-missing' } },
      { pk: { S: 'put-cond-ne-or-missing' } },
      { pk: { S: 'put-cond-eq-missing' } },
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

  it('not-equals on a missing attribute evaluates to true', async () => {
    // When the item does not exist, `status <> "working"` should be true
    // because a non-existent attribute is not equal to any value.
    const newPk = 'put-cond-ne-missing'
    await cleanupItems(hashTableDef.name, [{ pk: { S: newPk } }])

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: newPk }, status: { S: 'idle' } },
        ConditionExpression: '#s <> :v',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':v': { S: 'working' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: newPk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item).toBeDefined()
    expect(result.Item!.status.S).toBe('idle')
  })

  it('OR with not-equals and less-than on missing attributes succeeds', async () => {
    // `status <> "working" OR updatedAt < "2099-..."` on a non-existent item:
    // status is missing → <> returns true → OR short-circuits → condition passes
    const newPk = 'put-cond-ne-or-missing'
    await cleanupItems(hashTableDef.name, [{ pk: { S: newPk } }])

    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: newPk }, status: { S: 'new' } },
        ConditionExpression: '#s <> :v OR #u < :t',
        ExpressionAttributeNames: { '#s': 'status', '#u': 'updatedAt' },
        ExpressionAttributeValues: {
          ':v': { S: 'working' },
          ':t': { S: '2099-01-01T00:00:00Z' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: newPk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item).toBeDefined()
    expect(result.Item!.status.S).toBe('new')
  })

  it('equals on a missing attribute evaluates to false', async () => {
    // Sanity check: `status = "working"` on a non-existent item should fail
    const newPk = 'put-cond-eq-missing'
    await cleanupItems(hashTableDef.name, [{ pk: { S: newPk } }])

    await expectDynamoError(
      () => ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: newPk }, status: { S: 'idle' } },
          ConditionExpression: '#s = :v',
          ExpressionAttributeNames: { '#s': 'status' },
          ExpressionAttributeValues: { ':v': { S: 'working' } },
        }),
      ),
      'ConditionalCheckFailedException',
    )
  })
})
