import {
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
  QueryCommand,
  ScanCommand,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  cleanupItems,
  expectDynamoError,
} from '../../../src/helpers.js'

describe('Reserved words — ExpressionAttributeNames handling', () => {
  const keys = [
    { pk: { S: 'rw-status' } },
    { pk: { S: 'rw-name' } },
    { pk: { S: 'rw-data' } },
    { pk: { S: 'rw-count' } },
    { pk: { S: 'rw-items' } },
    { pk: { S: 'rw-cond-fail' } },
    { pk: { S: 'rw-upd-fail' } },
    { pk: { S: 'rw-filter-fail' } },
    { pk: { S: 'rw-proj-fail' } },
    { pk: { S: 'rw-multi' } },
  ]

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, keys)
  })

  // ── Positive cases: reserved words work with ExpressionAttributeNames ──

  it('"status" as attribute name works with #alias', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'rw-status' }, status: { S: 'active' } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'rw-status' } },
        ProjectionExpression: 'pk, #status',
        ExpressionAttributeNames: { '#status': 'status' },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.status.S).toBe('active')
  })

  it('"name" as attribute name works with #alias', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'rw-name' }, name: { S: 'Alice' } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'rw-name' } },
        ProjectionExpression: 'pk, #name',
        ExpressionAttributeNames: { '#name': 'name' },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.name.S).toBe('Alice')
  })

  it('"data" as attribute name works with #alias', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'rw-data' }, data: { S: 'payload' } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'rw-data' } },
        ProjectionExpression: 'pk, #data',
        ExpressionAttributeNames: { '#data': 'data' },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.data.S).toBe('payload')
  })

  it('"count" as attribute name works with #alias', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'rw-count' }, count: { N: '42' } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'rw-count' } },
        ProjectionExpression: 'pk, #count',
        ExpressionAttributeNames: { '#count': 'count' },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.count.N).toBe('42')
  })

  it('"items" as attribute name works with #alias', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'rw-items' }, items: { L: [{ S: 'a' }] } },
      }),
    )
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'rw-items' } },
        ProjectionExpression: 'pk, #items',
        ExpressionAttributeNames: { '#items': 'items' },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.items.L![0].S).toBe('a')
  })

  // ── Failure cases: reserved word without # in expressions ──

  it('reserved word without # in ConditionExpression fails', async () => {
    // Seed the item first
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'rw-cond-fail' }, status: { S: 'active' } },
      }),
    )
    await expectDynamoError(
      () =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: { pk: { S: 'rw-cond-fail' }, status: { S: 'inactive' } },
            ConditionExpression: 'status = :val',
            ExpressionAttributeValues: { ':val': { S: 'active' } },
          }),
        ),
      'ValidationException',
      /reserved keyword/i,
    )
  })

  it('reserved word without # in UpdateExpression fails', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'rw-upd-fail' }, status: { S: 'old' } },
      }),
    )
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'rw-upd-fail' } },
            UpdateExpression: 'SET status = :val',
            ExpressionAttributeValues: { ':val': { S: 'new' } },
          }),
        ),
      'ValidationException',
      /reserved keyword/i,
    )
  })

  it('reserved word without # in FilterExpression fails', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new ScanCommand({
            TableName: hashTableDef.name,
            FilterExpression: 'status = :val',
            ExpressionAttributeValues: { ':val': { S: 'active' } },
          }),
        ),
      'ValidationException',
      /reserved keyword/i,
    )
  })

  it('reserved word without # in ProjectionExpression fails', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new GetItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'rw-proj-fail' } },
            ProjectionExpression: 'pk, status',
            ConsistentRead: true,
          }),
        ),
      'ValidationException',
      /reserved keyword/i,
    )
  })

  it('multiple reserved words in same expression all need aliases', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: {
          pk: { S: 'rw-multi' },
          status: { S: 'active' },
          name: { S: 'test' },
          count: { N: '1' },
          data: { S: 'payload' },
        },
      }),
    )

    // Without aliases — should fail
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'rw-multi' } },
            UpdateExpression: 'SET status = :s, name = :n',
            ExpressionAttributeValues: {
              ':s': { S: 'inactive' },
              ':n': { S: 'updated' },
            },
          }),
        ),
      'ValidationException',
      /reserved keyword/i,
    )

    // With aliases — should succeed
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'rw-multi' } },
        UpdateExpression: 'SET #status = :s, #name = :n, #count = :c, #data = :d',
        ExpressionAttributeNames: {
          '#status': 'status',
          '#name': 'name',
          '#count': 'count',
          '#data': 'data',
        },
        ExpressionAttributeValues: {
          ':s': { S: 'inactive' },
          ':n': { S: 'updated' },
          ':c': { N: '2' },
          ':d': { S: 'new-payload' },
        },
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'rw-multi' } },
        ProjectionExpression: '#status, #name, #count, #data',
        ExpressionAttributeNames: {
          '#status': 'status',
          '#name': 'name',
          '#count': 'count',
          '#data': 'data',
        },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.status.S).toBe('inactive')
    expect(result.Item!.name.S).toBe('updated')
    expect(result.Item!.count.N).toBe('2')
    expect(result.Item!.data.S).toBe('new-payload')
  })
})
