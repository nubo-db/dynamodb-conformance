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
  // Defensive: the invalid-index-key cases below must all fail validation and
  // write nothing. Clean up anyway so a too-lenient target that wrongly persists
  // them does not leak rows into later tests.
  { pk: { S: 'tw-idx-type' }, sk: { S: 'a' } },
  { pk: { S: 'tw-idx-nonscalar' }, sk: { S: 'b' } },
  { pk: { S: 'tw-idx-empty' }, sk: { S: 'c' } },
  { pk: { S: 'tw-idx-upd-type' }, sk: { S: 'a' } },
  { pk: { S: 'tw-idx-upd-nonscalar' }, sk: { S: 'b' } },
  { pk: { S: 'tw-idx-upd-empty' }, sk: { S: 'c' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, hashKeys)
  await cleanupItems(compositeTableDef.name, compositeKeys)
})

describe('TransactWriteItems - basic functionality', { tags: ['transactions', 'data-plane'] }, () => {
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

describe('TransactWriteItems - multiple items', { tags: ['transactions', 'data-plane'] }, () => {
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

// no negative-path: acceptance-mixed (asserts accepted and rejected cases)
describe('TransactWriteItems - validation', { tags: ['transactions', 'data-plane'] }, () => {
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

  // An item carrying a malformed key value (table or index key) is rejected, but
  // the error SHAPE depends on the fault, and the two halves are opposite traps:
  //
  //   - Wrong type / non-scalar: caught during transaction execution, so AWS
  //     cancels with a TransactionCanceledException whose reason Code is
  //     'ValidationError'. An engine that "validates up front" and returns a
  //     top-level ValidationException here diverges from real DynamoDB.
  //   - Empty string: caught by up-front input validation, so AWS returns a
  //     top-level ValidationException even inside a transaction. An engine that
  //     wraps this as a TransactionCanceledException diverges.
  //
  // Both directions are asserted below. Exact strings live in
  // tests/tier3/error-messages/transactWriteItems.test.ts.

  const expectCancelledForValidation = async (transactItems: unknown[]) => {
    try {
      await ddb.send(
        new TransactWriteItemsCommand({ TransactItems: transactItems as never }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(TransactionCanceledException)
      const txErr = err as TransactionCanceledException
      // name is TransactionCanceledException, NOT a top-level ValidationException.
      expect(txErr.name).toBe('TransactionCanceledException')
      expect(txErr.CancellationReasons?.[0]?.Code).toBe('ValidationError')
    }
  }

  it('Put with a wrong-typed table key cancels with a ValidationError reason', async () => {
    await expectCancelledForValidation([
      { Put: { TableName: hashTableDef.name, Item: { pk: { N: '5' } } } },
    ])
  })

  it('Put with a non-scalar table key cancels with a ValidationError reason', async () => {
    await expectCancelledForValidation([
      { Put: { TableName: hashTableDef.name, Item: { pk: { L: [{ S: 'x' }] } } } },
    ])
  })

  it('Put with a wrong-typed index key cancels with a ValidationError reason', async () => {
    await expectCancelledForValidation([
      {
        Put: {
          TableName: compositeTableDef.name,
          Item: { pk: { S: 'tw-idx-type' }, sk: { S: 'a' }, lsi1sk: { N: '5' } },
        },
      },
    ])
  })

  it('Put with a non-scalar index key cancels with a ValidationError reason', async () => {
    await expectCancelledForValidation([
      {
        Put: {
          TableName: compositeTableDef.name,
          Item: { pk: { S: 'tw-idx-nonscalar' }, sk: { S: 'b' }, lsi1sk: { L: [{ S: 'x' }] } },
        },
      },
    ])
  })

  it('Update setting a wrong-typed index key cancels with a ValidationError reason', async () => {
    await expectCancelledForValidation([
      {
        Update: {
          TableName: compositeTableDef.name,
          Key: { pk: { S: 'tw-idx-upd-type' }, sk: { S: 'a' } },
          UpdateExpression: 'SET lsi1sk = :v',
          ExpressionAttributeValues: { ':v': { N: '5' } },
        },
      },
    ])
  })

  it('Update setting a non-scalar index key cancels with a ValidationError reason', async () => {
    await expectCancelledForValidation([
      {
        Update: {
          TableName: compositeTableDef.name,
          Key: { pk: { S: 'tw-idx-upd-nonscalar' }, sk: { S: 'b' } },
          UpdateExpression: 'SET lsi1sk = :v',
          ExpressionAttributeValues: { ':v': { L: [{ S: 'x' }] } },
        },
      },
    ])
  })

  it('Put with an empty-string table key is a top-level ValidationException', async () => {
    // Not a TransactionCanceledException — expectDynamoError asserts the name is
    // ValidationException, which a cancellation wrapper would fail.
    await expectDynamoError(
      () => ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            { Put: { TableName: hashTableDef.name, Item: { pk: { S: '' } } } },
          ],
        }),
      ),
      'ValidationException',
      /empty string value/i,
    )
  })

  it('Put with an empty-string index key is a top-level ValidationException', async () => {
    await expectDynamoError(
      () => ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: compositeTableDef.name,
                Item: { pk: { S: 'tw-idx-empty' }, sk: { S: 'c' }, lsi1sk: { S: '' } },
              },
            },
          ],
        }),
      ),
      'ValidationException',
      /secondary index key/i,
    )
  })

  it('Update setting an empty-string index key is a top-level ValidationException', async () => {
    await expectDynamoError(
      () => ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Update: {
                TableName: compositeTableDef.name,
                Key: { pk: { S: 'tw-idx-upd-empty' }, sk: { S: 'c' } },
                UpdateExpression: 'SET lsi1sk = :v',
                ExpressionAttributeValues: { ':v': { S: '' } },
              },
            },
          ],
        }),
      ),
      'ValidationException',
      /secondary index key/i,
    )
  })

  // A malformed VALUE in the lookup Key of an Update / Delete / ConditionCheck
  // (the key-only validation path, distinct from a Put item key). Four-region
  // capture (2026-06-23): empty string -> top-level ValidationException; wrong-type
  // / non-scalar -> cancelled with a ValidationError reason. The two helpers are
  // opposite guards — expectDynamoError fails an engine that wraps the empty-string
  // case as a cancellation, expectCancelledForValidation fails one that surfaces the
  // type cases as a top-level error. ConditionCheck behaves like the others. Exact
  // strings: tests/tier3/error-messages/transactWriteItems.test.ts.
  const updKey = (key: unknown) => ({ Update: { TableName: hashTableDef.name, Key: { pk: key }, UpdateExpression: 'SET attr1 = :v', ExpressionAttributeValues: { ':v': { S: 'x' } } } })
  const delKey = (key: unknown) => ({ Delete: { TableName: hashTableDef.name, Key: { pk: key } } })
  const ccKey = (key: unknown) => ({ ConditionCheck: { TableName: hashTableDef.name, Key: { pk: key }, ConditionExpression: 'attribute_not_exists(pk)' } })

  const expectKeyTopLevelValidation = (transactItem: unknown) =>
    expectDynamoError(
      () => ddb.send(new TransactWriteItemsCommand({ TransactItems: [transactItem] as never })),
      'ValidationException',
      /empty string value/i,
    )

  it('Update with an empty-string Key is a top-level ValidationException', () =>
    expectKeyTopLevelValidation(updKey({ S: '' })))
  it('Delete with an empty-string Key is a top-level ValidationException', () =>
    expectKeyTopLevelValidation(delKey({ S: '' })))
  it('ConditionCheck with an empty-string Key is a top-level ValidationException', () =>
    expectKeyTopLevelValidation(ccKey({ S: '' })))

  it('Update with a wrong-typed Key cancels with a ValidationError reason', () =>
    expectCancelledForValidation([updKey({ N: '5' })]))
  it('Update with a non-scalar Key cancels with a ValidationError reason', () =>
    expectCancelledForValidation([updKey({ L: [{ S: 'x' }] })]))
  it('Delete with a wrong-typed Key cancels with a ValidationError reason', () =>
    expectCancelledForValidation([delKey({ N: '5' })]))
  it('Delete with a non-scalar Key cancels with a ValidationError reason', () =>
    expectCancelledForValidation([delKey({ L: [{ S: 'x' }] })]))
  it('ConditionCheck with a wrong-typed Key cancels with a ValidationError reason', () =>
    expectCancelledForValidation([ccKey({ N: '5' })]))
  it('ConditionCheck with a non-scalar Key cancels with a ValidationError reason', () =>
    expectCancelledForValidation([ccKey({ L: [{ S: 'x' }] })]))

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

describe('TransactWriteItems - ConditionExpression parens', { tags: ['transactions', 'data-plane'] }, () => {
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

describe('TransactWriteItems — ConsumedCapacity', { tags: ['transactions', 'data-plane'] }, () => {
  const keys = [{ pk: { S: 'tw-cap-1' } }, { pk: { S: 'tw-cap-2' } }]

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, keys)
  })

  it('charges 2 write capacity units per item', async () => {
    const res = await ddb.send(
      new TransactWriteItemsCommand({
        ReturnConsumedCapacity: 'TOTAL',
        TransactItems: [
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-cap-1' }, v: { N: '1' } },
            },
          },
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-cap-2' }, v: { N: '1' } },
            },
          },
        ],
      }),
    )
    const total = (res.ConsumedCapacity ?? []).reduce(
      (sum, c) => sum + (c.CapacityUnits ?? 0),
      0,
    )
    // Sub-1KB items cost 1 WCU each non-transactionally; a transaction doubles
    // that to 2 per item, so two items total 4.
    expect(total).toBe(4)
  })
})

// Capacity for the cases the unconditional test above does not cover: a conditional
// write, a standalone ConditionCheck, an idempotent replay, and a cancelled
// transaction. All values characterised against real DynamoDB (eu-west-2). See #27.
describe('TransactWriteItems — ConsumedCapacity: conditional, check, replay, cancel', { tags: ['transactions', 'data-plane'] }, () => {
  const hashKeys = [
    { pk: { S: 'tw-cap-uncond' } },
    { pk: { S: 'tw-cap-cond' } },
    { pk: { S: 'tw-cap-cc' } },
    { pk: { S: 'tw-cap-replay' } },
  ]
  // The ConditionCheck below needs an item that already exists, on a different table
  // from the write so the INDEXES breakdown can isolate the check's cost.
  const ccPresent = { pk: { S: 'tw-cap-cc-present' }, sk: { S: 'x' } }

  const totalCap = (cc?: { CapacityUnits?: number }[]) =>
    (cc ?? []).reduce((sum, c) => sum + (c.CapacityUnits ?? 0), 0)

  beforeAll(async () => {
    await ddb.send(
      new PutItemCommand({ TableName: compositeTableDef.name, Item: ccPresent }),
    )
  })

  afterAll(async () => {
    await cleanupItems(hashTableDef.name, hashKeys)
    await cleanupItems(compositeTableDef.name, [{ pk: ccPresent.pk, sk: ccPresent.sk }])
  })

  // A passing condition adds no read capacity: a conditional write costs the same
  // 2 WCU/item as an unconditional one.
  it('a passing conditional write costs the same 2 WCU/item as an unconditional one', async () => {
    const uncond = await ddb.send(
      new TransactWriteItemsCommand({
        ReturnConsumedCapacity: 'TOTAL',
        TransactItems: [
          { Put: { TableName: hashTableDef.name, Item: { pk: { S: 'tw-cap-uncond' }, v: { N: '1' } } } },
        ],
      }),
    )
    const cond = await ddb.send(
      new TransactWriteItemsCommand({
        ReturnConsumedCapacity: 'TOTAL',
        TransactItems: [
          {
            Put: {
              TableName: hashTableDef.name,
              Item: { pk: { S: 'tw-cap-cond' }, v: { N: '1' } },
              ConditionExpression: 'attribute_not_exists(pk)',
            },
          },
        ],
      }),
    )
    expect(totalCap(cond.ConsumedCapacity)).toBe(totalCap(uncond.ConsumedCapacity))
    expect(totalCap(cond.ConsumedCapacity)).toBe(2)
  })

  // A standalone ConditionCheck costs 2 write capacity units - the same as a write,
  // and billed as write, not read. The check sits on a separate table from the write
  // so the INDEXES per-table breakdown isolates its cost (a same-table check and write
  // collapse into one capacity line).
  it('a standalone ConditionCheck action costs 2 write capacity units', async () => {
    const res = await ddb.send(
      new TransactWriteItemsCommand({
        ReturnConsumedCapacity: 'INDEXES',
        TransactItems: [
          { Put: { TableName: hashTableDef.name, Item: { pk: { S: 'tw-cap-cc' }, v: { N: '1' } } } },
          {
            ConditionCheck: {
              TableName: compositeTableDef.name,
              Key: { pk: { S: 'tw-cap-cc-present' }, sk: { S: 'x' } },
              ConditionExpression: 'attribute_exists(pk)',
            },
          },
        ],
      }),
    )
    const checkEntry = (res.ConsumedCapacity ?? []).find(
      (c) => c.TableName === compositeTableDef.name,
    )
    expect(checkEntry?.CapacityUnits).toBe(2)
    expect(checkEntry?.WriteCapacityUnits).toBe(2)
  })

  // Idempotent replay accounting: the first call reports write capacity; a same-token
  // replay (in-window) reports read capacity for re-reading the stored result.
  //
  // The item is deliberately >1KB. A sub-1KB item cannot tell the two magnitudes
  // apart: a transactional write costs 2*ceil(size/1KB) and a transactional read
  // costs 2*ceil(size/4KB), and both are 2 below 1KB. At ~1.5KB they diverge —
  // write 2*ceil(1.5KB/1KB) = 4, read 2*ceil(1.5KB/4KB) = 2 — so the replay
  // reporting 2 proves it recomputes a transactional READ against the item size,
  // rather than relabelling the stored write magnitude (which would be 4) as read.
  // Values characterised against real DynamoDB (eu-west-2). See #68.
  it('reports write capacity on the first call and read capacity on a same-token replay', async () => {
    const token = `tw-cap-replay-${Date.now()}`
    // ~1.5KB value keeps the item in the (1KB, 2KB) band: write rounds to 2 units,
    // read to 1, so their transactional doublings (4 vs 2) are distinct.
    const item = {
      Put: { TableName: hashTableDef.name, Item: { pk: { S: 'tw-cap-replay' }, big: { S: 'x'.repeat(1536) } } },
    }
    const first = await ddb.send(
      new TransactWriteItemsCommand({ ClientRequestToken: token, ReturnConsumedCapacity: 'INDEXES', TransactItems: [item] }),
    )
    const replay = await ddb.send(
      new TransactWriteItemsCommand({ ClientRequestToken: token, ReturnConsumedCapacity: 'INDEXES', TransactItems: [item] }),
    )
    const firstEntry = (first.ConsumedCapacity ?? [])[0]
    const replayEntry = (replay.ConsumedCapacity ?? [])[0]
    // First: a transactional write of a ~1.5KB item - 4 WCU, no read capacity.
    expect(firstEntry?.WriteCapacityUnits).toBe(4)
    expect(firstEntry?.ReadCapacityUnits).toBeUndefined()
    // Replay: a transactional read of the stored result, recomputed against the
    // item size - 2 RCU, not the write magnitude of 4, and no write capacity.
    expect(replayEntry?.ReadCapacityUnits).toBe(2)
    expect(replayEntry?.WriteCapacityUnits).toBeUndefined()
  })

  // A cancelled conditional transaction surfaces no consumed capacity: the
  // TransactionCanceledException carries CancellationReasons but no ConsumedCapacity.
  // (AWS still bills the prepare phase; this pins what the response reports, not billing.)
  it('a cancelled conditional transaction reports no consumed capacity', async () => {
    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          ReturnConsumedCapacity: 'TOTAL',
          TransactItems: [
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'tw-cap-cancel' }, v: { N: '1' } },
                ConditionExpression: 'attribute_exists(pk)', // fails on a fresh key
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(TransactionCanceledException)
      expect('ConsumedCapacity' in (err as object)).toBe(false)
    }
  })
})
