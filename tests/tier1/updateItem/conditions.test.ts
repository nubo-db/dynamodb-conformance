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
      { pk: { S: 'upd-cond-noexist' } },
      { pk: { S: 'upd-cond-upsert' } },
      { pk: { S: 'upd-cond-cmp-noexist' } },
      { pk: { S: 'upd-cond-and-noexist' } },
      { pk: { S: 'upd-cond-upsert-allnew' } },
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

  it('attribute_exists rejects update on non-existent item (no upsert)', async () => {
    // attribute_exists(pk) on a key that does not exist must fail —
    // the update should NOT create a ghost item.
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'upd-cond-noexist' } }])

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: 'upd-cond-noexist' } },
            UpdateExpression: 'ADD hit_count :inc',
            ConditionExpression: 'attribute_exists(pk)',
            ExpressionAttributeValues: { ':inc': { N: '1' } },
          }),
        ),
      'ConditionalCheckFailedException',
    )

    // Verify no ghost item was created
    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'upd-cond-noexist' } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeUndefined()
  })

  it('attribute_not_exists upserts on non-existent key', async () => {
    // Canonical "create if absent" pattern. attribute_not_exists(pk) on a key
    // that does not exist must succeed, and the update should be applied.
    const pk = 'upd-cond-upsert'
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])

    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        UpdateExpression: 'SET #s = :new',
        ConditionExpression: 'attribute_not_exists(pk)',
        ExpressionAttributeNames: { '#s': 'status' },
        ExpressionAttributeValues: { ':new': { S: 'created' } },
      }),
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeDefined()
    expect(check.Item!.status.S).toBe('created')
  })

  it('comparison condition rejects update on non-existent key; no ghost item', async () => {
    // `score > :min` evaluates false because the attribute (and the item)
    // does not exist. Update must fail and must not create a ghost item.
    const pk = 'upd-cond-cmp-noexist'
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: pk } },
            UpdateExpression: 'SET #s = :new',
            ConditionExpression: '#sc > :min',
            ExpressionAttributeNames: { '#s': 'status', '#sc': 'score' },
            ExpressionAttributeValues: {
              ':new': { S: 'should-not-apply' },
              ':min': { N: '0' },
            },
          }),
        ),
      'ConditionalCheckFailedException',
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeUndefined()
  })

  it('combined attribute_exists + equality rejects update on non-existent key; no ghost item', async () => {
    const pk = 'upd-cond-and-noexist'
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])

    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: pk } },
            UpdateExpression: 'SET #s = :new',
            ConditionExpression: 'attribute_exists(pk) AND #s = :expected',
            ExpressionAttributeNames: { '#s': 'status' },
            ExpressionAttributeValues: {
              ':new': { S: 'should-not-apply' },
              ':expected': { S: 'active' },
            },
          }),
        ),
      'ConditionalCheckFailedException',
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeUndefined()
  })

  it('attribute_not_exists upsert with ReturnValues: ALL_NEW returns created attributes', async () => {
    const pk = 'upd-cond-upsert-allnew'
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])

    const result = await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        UpdateExpression: 'SET #s = :new, #sc = :score',
        ConditionExpression: 'attribute_not_exists(pk)',
        ExpressionAttributeNames: { '#s': 'status', '#sc': 'score' },
        ExpressionAttributeValues: {
          ':new': { S: 'fresh' },
          ':score': { N: '42' },
        },
        ReturnValues: 'ALL_NEW',
      }),
    )

    expect(result.Attributes).toBeDefined()
    expect(result.Attributes!.pk.S).toBe(pk)
    expect(result.Attributes!.status.S).toBe('fresh')
    expect(result.Attributes!.score.N).toBe('42')
  })
})

describe('UpdateItem — ConditionExpression parens', () => {
  const pk = 'upd-cep-seed'

  beforeAll(async () => {
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: pk }, status: { S: 'active' }, score: { N: '10' } },
      }),
    )
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])
  })

  it('accepts per-condition parens and updates', async () => {
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        UpdateExpression: 'SET #s = :next',
        ConditionExpression: '(#s = :cur) AND (#sc > :min)',
        ExpressionAttributeNames: { '#s': 'status', '#sc': 'score' },
        ExpressionAttributeValues: {
          ':cur': { S: 'active' },
          ':next': { S: 'step-1' },
          ':min': { N: '5' },
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
    expect(result.Item!.status.S).toBe('step-1')
  })

  it('accepts full-expression wrap and updates', async () => {
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        UpdateExpression: 'SET #s = :next',
        ConditionExpression: '(#s = :cur AND #sc > :min)',
        ExpressionAttributeNames: { '#s': 'status', '#sc': 'score' },
        ExpressionAttributeValues: {
          ':cur': { S: 'step-1' },
          ':next': { S: 'step-2' },
          ':min': { N: '5' },
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
    expect(result.Item!.status.S).toBe('step-2')
  })

  it('accepts non-redundant nested parens and updates', async () => {
    await ddb.send(
      new UpdateItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        UpdateExpression: 'SET #s = :next',
        ConditionExpression: '(#s = :cur AND (#sc > :min))',
        ExpressionAttributeNames: { '#s': 'status', '#sc': 'score' },
        ExpressionAttributeValues: {
          ':cur': { S: 'step-2' },
          ':next': { S: 'step-3' },
          ':min': { N: '5' },
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
    expect(result.Item!.status.S).toBe('step-3')
  })

  it('rejects UpdateItem when parenthesised condition fails; item unchanged', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new UpdateItemCommand({
            TableName: hashTableDef.name,
            Key: { pk: { S: pk } },
            UpdateExpression: 'SET #s = :next',
            ConditionExpression: '(#s = :wrong) AND (#sc > :min)',
            ExpressionAttributeNames: { '#s': 'status', '#sc': 'score' },
            ExpressionAttributeValues: {
              ':wrong': { S: 'inactive' },
              ':next': { S: 'should-not-apply' },
              ':min': { N: '5' },
            },
          }),
        ),
      'ConditionalCheckFailedException',
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.status.S).toBe('step-3')
  })
})
