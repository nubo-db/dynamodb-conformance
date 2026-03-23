import {
  PutItemCommand,
  GetItemCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import { hashTableDef, expectDynamoError, cleanupItems } from '../../../src/helpers.js'

describe('PutItem — ConditionExpression functions and operators', () => {
  const pk = 'put-expr'

  beforeAll(async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          name: { S: 'alice' },
          count: { N: '5' },
          status: { S: 'active' },
          mapAttr: { M: { nested: { S: 'deep-value' } } },
        },
      }),
    )
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [
      { pk: { S: pk } },
      { pk: { S: 'put-expr-missing' } },
    ])
  })

  it('succeeds when size(attr) < :val condition is met', async () => {
    // name is "alice" (length 5), so size < 10 should pass
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          name: { S: 'alice-updated' },
          count: { N: '5' },
          status: { S: 'active' },
          mapAttr: { M: { nested: { S: 'deep-value' } } },
        },
        ConditionExpression: 'size(#n) < :maxLen',
        ExpressionAttributeNames: { '#n': 'name' },
        ExpressionAttributeValues: { ':maxLen': { N: '10' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.name.S).toBe('alice-updated')
  })

  it('succeeds when contains(attr, :substr) condition is met', async () => {
    // name is now "alice-updated", contains "updated"
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          name: { S: 'alice-v2' },
          count: { N: '5' },
          status: { S: 'active' },
          mapAttr: { M: { nested: { S: 'deep-value' } } },
        },
        ConditionExpression: 'contains(#n, :substr)',
        ExpressionAttributeNames: { '#n': 'name' },
        ExpressionAttributeValues: { ':substr': { S: 'updated' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.name.S).toBe('alice-v2')
  })

  it('succeeds when attribute_type(attr, :type) condition is met', async () => {
    // status is of type S
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          name: { S: 'alice-v3' },
          count: { N: '5' },
          status: { S: 'active' },
          mapAttr: { M: { nested: { S: 'deep-value' } } },
        },
        ConditionExpression: 'attribute_type(#s, :t)',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':t': { S: 'S' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.name.S).toBe('alice-v3')
  })

  it('succeeds when NOT attribute_exists(attr) condition is met', async () => {
    // "put-expr-missing" does not exist yet, so NOT attribute_exists(pk) passes
    const newPk = 'put-expr-missing'
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: newPk }, data: { S: 'created' } },
        ConditionExpression: 'NOT attribute_exists(pk)',
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: newPk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.data.S).toBe('created')
  })

  it('succeeds when compound AND condition is met', async () => {
    // name = "alice-v3" AND count = 5
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          name: { S: 'alice-v4' },
          count: { N: '5' },
          status: { S: 'active' },
          mapAttr: { M: { nested: { S: 'deep-value' } } },
        },
        ConditionExpression: '#n = :name AND #c = :count',
        ExpressionAttributeNames: { '#n': 'name', '#c': 'count' },
        ExpressionAttributeValues: {
          ':name': { S: 'alice-v3' },
          ':count': { N: '5' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.name.S).toBe('alice-v4')
  })

  it('succeeds when compound OR condition is met (one side true)', async () => {
    // name = "wrong" OR count = 5 — second condition is true
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          name: { S: 'alice-v5' },
          count: { N: '5' },
          status: { S: 'active' },
          mapAttr: { M: { nested: { S: 'deep-value' } } },
        },
        ConditionExpression: '#n = :wrongName OR #c = :count',
        ExpressionAttributeNames: { '#n': 'name', '#c': 'count' },
        ExpressionAttributeValues: {
          ':wrongName': { S: 'nobody' },
          ':count': { N: '5' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.name.S).toBe('alice-v5')
  })

  it('succeeds when nested map path condition is met', async () => {
    // mapAttr.nested = "deep-value"
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          name: { S: 'alice-v6' },
          count: { N: '5' },
          status: { S: 'active' },
          mapAttr: { M: { nested: { S: 'deep-value' } } },
        },
        ConditionExpression: 'mapAttr.nested = :val',
        ExpressionAttributeValues: { ':val': { S: 'deep-value' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.name.S).toBe('alice-v6')
  })

  it('attribute_exists on nested path', async () => {
    // mapAttr.nested exists on the item
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          name: { S: 'alice-nested-exists' },
          count: { N: '5' },
          status: { S: 'active' },
          mapAttr: { M: { nested: { S: 'deep-value' } } },
        },
        ConditionExpression: 'attribute_exists(#m.#n)',
        ExpressionAttributeNames: { '#m': 'mapAttr', '#n': 'nested' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.name.S).toBe('alice-nested-exists')
  })

  it('attribute_not_exists on nested path', async () => {
    // mapAttr.missing does not exist on the item
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          name: { S: 'alice-nested-not-exists' },
          count: { N: '5' },
          status: { S: 'active' },
          mapAttr: { M: { nested: { S: 'deep-value' } } },
        },
        ConditionExpression: 'attribute_not_exists(#m.#missing)',
        ExpressionAttributeNames: { '#m': 'mapAttr', '#missing': 'nonexistent' },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.name.S).toBe('alice-nested-not-exists')
  })

  it('succeeds with reserved word using ExpressionAttributeNames', async () => {
    // "status" is a reserved word in DynamoDB
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: pk },
          name: { S: 'alice-v7' },
          count: { N: '5' },
          status: { S: 'active' },
          mapAttr: { M: { nested: { S: 'deep-value' } } },
        },
        ConditionExpression: '#status = :val',
        ExpressionAttributeNames: { '#status': 'status' },
        ExpressionAttributeValues: { ':val': { S: 'active' } },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.name.S).toBe('alice-v7')
  })
})
