import {
  PutItemCommand,
  GetItemCommand,
  UpdateItemCommand,
  DeleteItemCommand,
  TransactWriteItemsCommand,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  compositeTableDef,
  cleanupItems,
  expectDynamoError,
} from '../../../src/helpers.js'

const hashKeys = [
  { pk: { S: 'tw-basic-1' } },
  { pk: { S: 'tw-basic-2' } },
  { pk: { S: 'tw-basic-3' } },
  { pk: { S: 'tw-cond-put' } },
  { pk: { S: 'tw-cond-upd' } },
  { pk: { S: 'tw-cond-del' } },
  { pk: { S: 'tw-cc-check' } },
  { pk: { S: 'tw-cc-pass' } },
  { pk: { S: 'tw-cc-fail' } },
  { pk: { S: 'tw-cc-fail-put' } },
  { pk: { S: 'tw-cross-hash' } },
  { pk: { S: 'tw-idem-1' } },
  { pk: { S: 'tw-cancel-reasons' } },
  { pk: { S: 'tw-rvocf' } },
  { pk: { S: 'tw-multi-put-1' } },
  { pk: { S: 'tw-multi-put-2' } },
  { pk: { S: 'tw-multi-put-3' } },
  { pk: { S: 'tw-multi-upd-1' } },
  { pk: { S: 'tw-multi-upd-2' } },
  { pk: { S: 'tw-multi-upd-3' } },
  { pk: { S: 'tw-multi-del-1' } },
  { pk: { S: 'tw-multi-del-2' } },
  { pk: { S: 'tw-multi-del-3' } },
  { pk: { S: 'tw-dup-1' } },
  { pk: { S: 'tw-idem-mismatch' } },
  { pk: { S: 'tw-cond-noexist' } },
  { pk: { S: 'tw-cep-put' } },
  { pk: { S: 'tw-cep-upd' } },
  { pk: { S: 'tw-cep-del' } },
  { pk: { S: 'tw-cep-cc' } },
  { pk: { S: 'tw-cep-fail' } },
  { pk: { S: 'tw-upsert' } },
  { pk: { S: 'tw-cmp-noexist' } },
  { pk: { S: 'tw-and-noexist' } },
  { pk: { S: 'tw-mix-existing' } },
  { pk: { S: 'tw-mix-noexist' } },
]

const compositeKeys = [
  { pk: { S: 'tw-cross-comp' }, sk: { S: 'sk1' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, hashKeys)
  await cleanupItems(compositeTableDef.name, compositeKeys)
})

describe('TransactWriteItems - basic functionality', () => {
  it('executes Put + Update + Delete atomically', async () => {
    // Seed items for update and delete
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tw-basic-2' }, data: { S: 'before-update' } },
      }),
    )
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tw-basic-3' }, data: { S: 'to-delete' } },
      }),
    )

    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-basic-1' }, data: { S: 'created' } },
            },
          },
          {
            Update: {
              TableName: hashTableDef.name,
              Key: { pk: { S: 'tw-basic-2' } },
              UpdateExpression: 'SET #d = :v',
              ExpressionAttributeNames: { '#d': 'data' },
              ExpressionAttributeValues: { ':v': { S: 'after-update' } },
            },
          },
          {
            Delete: {
              TableName: hashTableDef.name,
              Key: { pk: { S: 'tw-basic-3' } },
            },
          },
        ],
      }),
    )

    const put = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-basic-1' } },
        ConsistentRead: true,
      }),
    )
    expect(put.Item).toBeDefined()
    expect(put.Item!.data.S).toBe('created')

    const upd = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-basic-2' } },
        ConsistentRead: true,
      }),
    )
    expect(upd.Item!.data.S).toBe('after-update')

    const del = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-basic-3' } },
        ConsistentRead: true,
      }),
    )
    expect(del.Item).toBeUndefined()
  })

  it('succeeds when ConditionCheck condition is met', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tw-cc-check' }, status: { S: 'active' } },
      }),
    )

    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: hashTableDef.name,
              Key: { pk: { S: 'tw-cc-check' } },
              ConditionExpression: '#s = :v',
              ExpressionAttributeNames: { '#s': 'status' },
              ExpressionAttributeValues: { ':v': { S: 'active' } },
            },
          },
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-cc-pass' }, status: { S: 'processed' } },
            },
          },
        ],
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cc-pass' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.status.S).toBe('processed')
  })

  it('rolls back entire transaction when ConditionCheck fails', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tw-cc-fail' }, status: { S: 'inactive' } },
      }),
    )

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              ConditionCheck: {
                TableName: hashTableDef.name,
                Key: { pk: { S: 'tw-cc-fail' } },
                ConditionExpression: '#s = :v',
                ExpressionAttributeNames: { '#s': 'status' },
                ExpressionAttributeValues: { ':v': { S: 'active' } },
              },
            },
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'tw-cc-fail-put' }, status: { S: 'should-not-exist' } },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
    }

    // Verify rollback: item unchanged
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cc-fail' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.status.S).toBe('inactive')
  })

  it('applies ConditionExpression on Put action', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tw-cond-put' }, data: { S: 'existing' } },
      }),
    )

    // Condition: attribute_not_exists should fail since item exists
    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'tw-cond-put' }, data: { S: 'replaced' } },
                ConditionExpression: 'attribute_not_exists(pk)',
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
    }

    // Item should remain unchanged
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cond-put' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.data.S).toBe('existing')
  })

  it('applies ConditionExpression on Update action', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tw-cond-upd' }, counter: { N: '5' } },
      }),
    )

    // Condition: counter > 10 should fail
    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: hashTableDef.name,
                Key: { pk: { S: 'tw-cond-upd' } },
                UpdateExpression: 'SET #c = :v',
                ConditionExpression: '#c > :min',
                ExpressionAttributeNames: { '#c': 'counter' },
                ExpressionAttributeValues: {
                  ':v': { N: '100' },
                  ':min': { N: '10' },
                },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
    }

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cond-upd' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.counter.N).toBe('5')
  })

  it('applies ConditionExpression on Delete action', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tw-cond-del' }, locked: { BOOL: true } },
      }),
    )

    // Condition: locked = false should fail
    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Delete: {
                TableName: hashTableDef.name,
                Key: { pk: { S: 'tw-cond-del' } },
                ConditionExpression: 'locked = :v',
                ExpressionAttributeValues: { ':v': { BOOL: false } },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
    }

    // Item should still exist
    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cond-del' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item).toBeDefined()
    expect(result.Item!.locked.BOOL).toBe(true)
  })

  it('executes cross-table transaction (hashTableDef + compositeTableDef)', async () => {
    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-cross-hash' }, data: { S: 'from-hash' } },
            },
          },
          {
            Put: {
              TableName: compositeTableDef.name,
              Item: {
                pk: { S: 'tw-cross-comp' },
                sk: { S: 'sk1' },
                data: { S: 'from-composite' },
              },
            },
          },
        ],
      }),
    )

    const hashResult = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cross-hash' } },
        ConsistentRead: true,
      }),
    )
    expect(hashResult.Item!.data.S).toBe('from-hash')

    const compResult = await ddb.send(
      new GetItemCommand({
        TableName: compositeTableDef.name,
        Key: { pk: { S: 'tw-cross-comp' }, sk: { S: 'sk1' } },
        ConsistentRead: true,
      }),
    )
    expect(compResult.Item!.data.S).toBe('from-composite')
  })

  it('supports idempotency via ClientRequestToken', async () => {
    const token = `idem-token-${Date.now()}`

    await ddb.send(
      new TransactWriteItemsCommand({
        ClientRequestToken: token,
        TransactItems: [
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-idem-1' }, attempt: { N: '1' } },
            },
          },
        ],
      }),
    )

    // Retry with same token should succeed (idempotent)
    await ddb.send(
      new TransactWriteItemsCommand({
        ClientRequestToken: token,
        TransactItems: [
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-idem-1' }, attempt: { N: '1' } },
            },
          },
        ],
      }),
    )

    const result = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-idem-1' } },
        ConsistentRead: true,
      }),
    )
    expect(result.Item!.attempt.N).toBe('1')
  })

  it('rejects same ClientRequestToken with different payload', async () => {
    const token = 'idempotent-test-token-' + Date.now()
    // First call succeeds
    await ddb.send(new TransactWriteItemsCommand({
      ClientRequestToken: token,
      TransactItems: [{
        Put: { TableName: hashTableDef.name, Item: { pk: { S: 'tw-idem-mismatch' }, val: { S: 'first' } } },
      }],
    }))
    // Same token, different payload — should throw IdempotentParameterMismatchException
    await expectDynamoError(
      () => ddb.send(new TransactWriteItemsCommand({
        ClientRequestToken: token,
        TransactItems: [{
          Put: { TableName: hashTableDef.name, Item: { pk: { S: 'tw-idem-mismatch' }, val: { S: 'different' } } },
        }],
      })),
      'IdempotentParameterMismatchException',
    )
  })

  it('includes CancellationReasons in error when condition fails', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tw-cancel-reasons' }, status: { S: 'locked' } },
      }),
    )

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              ConditionCheck: {
                TableName: hashTableDef.name,
                Key: { pk: { S: 'tw-cancel-reasons' } },
                ConditionExpression: '#s = :v',
                ExpressionAttributeNames: { '#s': 'status' },
                ExpressionAttributeValues: { ':v': { S: 'unlocked' } },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
      const err = e as TransactionCanceledException
      expect(err.CancellationReasons).toBeDefined()
      expect(err.CancellationReasons).toHaveLength(1)
      expect(err.CancellationReasons![0].Code).toBe('ConditionalCheckFailed')
    }
  })

  it('returns ALL_OLD item via ReturnValuesOnConditionCheckFailure', async () => {
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'tw-rvocf' }, data: { S: 'original' } },
      }),
    )

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'tw-rvocf' }, data: { S: 'new' } },
                ConditionExpression: 'attribute_not_exists(pk)',
                ReturnValuesOnConditionCheckFailure: 'ALL_OLD',
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
      const err = e as TransactionCanceledException
      expect(err.CancellationReasons).toBeDefined()
      expect(err.CancellationReasons).toHaveLength(1)
      const reason = err.CancellationReasons![0]
      expect(reason.Code).toBe('ConditionalCheckFailed')
      expect(reason.Item).toBeDefined()
      expect(reason.Item!.pk.S).toBe('tw-rvocf')
      expect(reason.Item!.data.S).toBe('original')
    }
  })
})

describe('TransactWriteItems - multiple items', () => {
  it('puts multiple items in one transaction', async () => {
    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-multi-put-1' }, idx: { N: '1' } },
            },
          },
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-multi-put-2' }, idx: { N: '2' } },
            },
          },
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-multi-put-3' }, idx: { N: '3' } },
            },
          },
        ],
      }),
    )

    for (const i of [1, 2, 3]) {
      const result = await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: `tw-multi-put-${i}` } },
          ConsistentRead: true,
        }),
      )
      expect(result.Item).toBeDefined()
      expect(result.Item!.idx.N).toBe(String(i))
    }
  })

  it('updates multiple items in one transaction', async () => {
    // Seed items
    for (const i of [1, 2, 3]) {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: `tw-multi-upd-${i}` }, val: { S: 'before' } },
        }),
      )
    }

    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [1, 2, 3].map((i) => ({
          Update: {
            TableName: hashTableDef.name,
            Key: { pk: { S: `tw-multi-upd-${i}` } },
            UpdateExpression: 'SET val = :v',
            ExpressionAttributeValues: { ':v': { S: `after-${i}` } },
          },
        })),
      }),
    )

    for (const i of [1, 2, 3]) {
      const result = await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: `tw-multi-upd-${i}` } },
          ConsistentRead: true,
        }),
      )
      expect(result.Item!.val.S).toBe(`after-${i}`)
    }
  })

  it('deletes multiple items in one transaction', async () => {
    // Seed items
    for (const i of [1, 2, 3]) {
      await ddb.send(
        new PutItemCommand({
          TableName: hashTableDef.name,
          Item: { pk: { S: `tw-multi-del-${i}` }, val: { S: 'exists' } },
        }),
      )
    }

    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [1, 2, 3].map((i) => ({
          Delete: {
            TableName: hashTableDef.name,
            Key: { pk: { S: `tw-multi-del-${i}` } },
          },
        })),
      }),
    )

    for (const i of [1, 2, 3]) {
      const result = await ddb.send(
        new GetItemCommand({
          TableName: hashTableDef.name,
          Key: { pk: { S: `tw-multi-del-${i}` } },
          ConsistentRead: true,
        }),
      )
      expect(result.Item).toBeUndefined()
    }
  })
})

describe('TransactWriteItems - validation', () => {
  it('rejects duplicate target items in same transaction', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Put: {
                  TableName: hashTableDef.name,
                  Item: { pk: { S: 'tw-dup-1' }, val: { S: 'first' } },
                },
              },
              {
                Put: {
                  TableName: hashTableDef.name,
                  Item: { pk: { S: 'tw-dup-1' }, val: { S: 'second' } },
                },
              },
            ],
          }),
        ),
      'ValidationException',
    )
  })

  it('rejects empty TransactItems', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new TransactWriteItemsCommand({
            TransactItems: [],
          }),
        ),
      'ValidationException',
    )
  })

  it('rejects transaction on non-existent table', async () => {
    await expectDynamoError(
      () =>
        ddb.send(
          new TransactWriteItemsCommand({
            TransactItems: [
              {
                Put: {
                  TableName: '_conformance_nonexistent_table',
                  Item: { pk: { S: 'x' } },
                },
              },
            ],
          }),
        ),
      'ResourceNotFoundException',
    )
  })

  it('Update with attribute_exists rejects non-existent item', async () => {
    // TransactWriteItems Update with attribute_exists(pk) on a key that does
    // not exist must cancel the transaction — not silently create the item.
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'tw-cond-noexist' } }])

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: hashTableDef.name,
                Key: { pk: { S: 'tw-cond-noexist' } },
                UpdateExpression: 'ADD hit_count :inc',
                ConditionExpression: 'attribute_exists(pk)',
                ExpressionAttributeValues: { ':inc': { N: '1' } },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown TransactionCanceledException')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
      const err = e as TransactionCanceledException
      expect(err.CancellationReasons).toBeDefined()
      expect(err.CancellationReasons![0].Code).toBe('ConditionalCheckFailed')
    }

    // Verify no ghost item was created
    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cond-noexist' } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeUndefined()
  })

  it('Update with attribute_not_exists upserts on non-existent key', async () => {
    // Canonical "create if absent" through TransactWriteItems. Mirror of the
    // UpdateItem upsert test, now through the transactional code path.
    const pk = 'tw-upsert'
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])

    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: hashTableDef.name,
              Key: { pk: { S: pk } },
              UpdateExpression: 'SET #s = :new',
              ConditionExpression: 'attribute_not_exists(pk)',
              ExpressionAttributeNames: { '#s': 'status' },
              ExpressionAttributeValues: { ':new': { S: 'created' } },
            },
          },
        ],
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

  it('Update with comparison condition cancels on non-existent key; no ghost item', async () => {
    const pk = 'tw-cmp-noexist'
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: hashTableDef.name,
                Key: { pk: { S: pk } },
                UpdateExpression: 'SET #s = :new',
                ConditionExpression: '#sc > :min',
                ExpressionAttributeNames: { '#s': 'status', '#sc': 'score' },
                ExpressionAttributeValues: {
                  ':new': { S: 'should-not-apply' },
                  ':min': { N: '0' },
                },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown TransactionCanceledException')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
      const err = e as TransactionCanceledException
      expect(err.CancellationReasons![0].Code).toBe('ConditionalCheckFailed')
    }

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeUndefined()
  })

  it('Update with combined attribute_exists + equality cancels on non-existent key; no ghost item', async () => {
    const pk = 'tw-and-noexist'
    await cleanupItems(hashTableDef.name, [{ pk: { S: pk } }])

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: hashTableDef.name,
                Key: { pk: { S: pk } },
                UpdateExpression: 'SET #s = :new',
                ConditionExpression: 'attribute_exists(pk) AND #s = :expected',
                ExpressionAttributeNames: { '#s': 'status' },
                ExpressionAttributeValues: {
                  ':new': { S: 'should-not-apply' },
                  ':expected': { S: 'active' },
                },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown TransactionCanceledException')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
      const err = e as TransactionCanceledException
      expect(err.CancellationReasons![0].Code).toBe('ConditionalCheckFailed')
    }

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: pk } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeUndefined()
  })

  it('mixed transaction: one passing, one failing on non-existent cancels everything', async () => {
    // Integration: two Updates in one transaction. One targets an existing
    // item (condition holds), the other targets a non-existent key with
    // attribute_exists(pk) (condition fails). The whole transaction must be
    // cancelled and the existing item must remain unmodified.
    const existingPk = 'tw-mix-existing'
    const noexistPk = 'tw-mix-noexist'
    await cleanupItems(hashTableDef.name, [
      { pk: { S: existingPk } },
      { pk: { S: noexistPk } },
    ])
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: existingPk }, status: { S: 'active' } },
      }),
    )

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: hashTableDef.name,
                Key: { pk: { S: existingPk } },
                UpdateExpression: 'SET #s = :new',
                ConditionExpression: 'attribute_exists(pk)',
                ExpressionAttributeNames: { '#s': 'status' },
                ExpressionAttributeValues: { ':new': { S: 'updated' } },
              },
            },
            {
              Update: {
                TableName: hashTableDef.name,
                Key: { pk: { S: noexistPk } },
                UpdateExpression: 'SET #s = :new',
                ConditionExpression: 'attribute_exists(pk)',
                ExpressionAttributeNames: { '#s': 'status' },
                ExpressionAttributeValues: { ':new': { S: 'should-not-apply' } },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown TransactionCanceledException')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
      const err = e as TransactionCanceledException
      expect(err.CancellationReasons![1].Code).toBe('ConditionalCheckFailed')
    }

    // Existing item unchanged
    const existing = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: existingPk } },
        ConsistentRead: true,
      }),
    )
    expect(existing.Item!.status.S).toBe('active')

    // Non-existent key still absent
    const noexist = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: noexistPk } },
        ConsistentRead: true,
      }),
    )
    expect(noexist.Item).toBeUndefined()
  })
})

describe('TransactWriteItems - ConditionExpression parens', () => {
  // Each action type (Put, Update, Delete, ConditionCheck) is exercised
  // through the transactional code path with a parenthesised ConditionExpression.
  // Seeds existing items for the Update, Delete, and ConditionCheck cases.
  beforeAll(async () => {
    await Promise.all(
      [
        { pk: 'tw-cep-upd', status: 'active', score: '10' },
        { pk: 'tw-cep-del', status: 'active' },
        { pk: 'tw-cep-cc', status: 'active' },
        { pk: 'tw-cep-fail', status: 'active' },
      ].map((item) =>
        ddb.send(
          new PutItemCommand({
            TableName: hashTableDef.name,
            Item: {
              pk: { S: item.pk },
              status: { S: item.status },
              ...(item.score ? { score: { N: item.score } } : {}),
            },
          }),
        ),
      ),
    )
    await cleanupItems(hashTableDef.name, [{ pk: { S: 'tw-cep-put' } }])
  })

  it('Put with per-condition parens succeeds on fresh key', async () => {
    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-cep-put' }, status: { S: 'new' } },
              ConditionExpression:
                '(attribute_not_exists(pk)) AND (attribute_not_exists(#s))',
              ExpressionAttributeNames: { '#s': 'status' },
            },
          },
        ],
      }),
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cep-put' } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item!.status.S).toBe('new')
  })

  it('Update with full-expression wrap succeeds when condition holds', async () => {
    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: hashTableDef.name,
              Key: { pk: { S: 'tw-cep-upd' } },
              UpdateExpression: 'SET #s = :next',
              ConditionExpression: '(#s = :cur AND #sc > :min)',
              ExpressionAttributeNames: { '#s': 'status', '#sc': 'score' },
              ExpressionAttributeValues: {
                ':cur': { S: 'active' },
                ':next': { S: 'updated' },
                ':min': { N: '5' },
              },
            },
          },
        ],
      }),
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cep-upd' } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item!.status.S).toBe('updated')
  })

  it('Delete with non-redundant nested parens succeeds when condition holds', async () => {
    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Delete: {
              TableName: hashTableDef.name,
              Key: { pk: { S: 'tw-cep-del' } },
              ConditionExpression: '(attribute_exists(pk) AND (#s = :v))',
              ExpressionAttributeNames: { '#s': 'status' },
              ExpressionAttributeValues: { ':v': { S: 'active' } },
            },
          },
        ],
      }),
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cep-del' } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item).toBeUndefined()
  })

  it('ConditionCheck with per-condition parens passes when condition holds', async () => {
    // ConditionCheck-only transaction also needs a write action to be valid,
    // so we pair it with a harmless Put on a fresh key.
    const putKey = 'tw-cep-cc-put'
    await cleanupItems(hashTableDef.name, [{ pk: { S: putKey } }])

    await ddb.send(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            ConditionCheck: {
              TableName: hashTableDef.name,
              Key: { pk: { S: 'tw-cep-cc' } },
              ConditionExpression: '(attribute_exists(pk)) AND (#s = :v)',
              ExpressionAttributeNames: { '#s': 'status' },
              ExpressionAttributeValues: { ':v': { S: 'active' } },
            },
          },
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: putKey }, data: { S: 'witness' } },
            },
          },
        ],
      }),
    )

    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: putKey } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item!.data.S).toBe('witness')
    await cleanupItems(hashTableDef.name, [{ pk: { S: putKey } }])
  })

  it('cancels transaction when any parenthesised condition fails', async () => {
    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: hashTableDef.name,
                Key: { pk: { S: 'tw-cep-fail' } },
                UpdateExpression: 'SET #s = :next',
                ConditionExpression: '(#s = :wrong) AND (attribute_exists(pk))',
                ExpressionAttributeNames: { '#s': 'status' },
                ExpressionAttributeValues: {
                  ':wrong': { S: 'inactive' },
                  ':next': { S: 'should-not-apply' },
                },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown TransactionCanceledException')
    } catch (e) {
      expect(e).toBeInstanceOf(TransactionCanceledException)
      const err = e as TransactionCanceledException
      expect(err.CancellationReasons![0].Code).toBe('ConditionalCheckFailed')
    }

    // Item unchanged
    const check = await ddb.send(
      new GetItemCommand({
        TableName: hashTableDef.name,
        Key: { pk: { S: 'tw-cep-fail' } },
        ConsistentRead: true,
      }),
    )
    expect(check.Item!.status.S).toBe('active')
  })
})
