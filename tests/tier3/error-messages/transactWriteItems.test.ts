import {
  TransactWriteItemsCommand,
  PutItemCommand,
  DynamoDBServiceException,
  ResourceNotFoundException,
  TransactionCanceledException,
} from '@aws-sdk/client-dynamodb'
import { ddb } from '../../../src/client.js'
import {
  hashTableDef,
  hashBTableDef,
  compositeTableDef,
  cleanupItems,
} from '../../../src/helpers.js'

const keysToCleanup = [
  { pk: { S: 'em-twi-dup' } },
  { pk: { S: 'em-twi-multi-1' } },
  { pk: { S: 'em-twi-multi-2' } },
  { pk: { S: 'em-twi-pos' } },
  { pk: { S: 'em-twi-pos-new' } },
]

// Defensive: the invalid-key-value cases below fail validation and write
// nothing; clean up anyway in case a too-lenient target persists them.
const compositeKeysToCleanup = [
  { pk: { S: 'em-twi-idx-type' }, sk: { S: 'a' } },
  { pk: { S: 'em-twi-idx-nonscalar' }, sk: { S: 'b' } },
  { pk: { S: 'em-twi-idx-empty' }, sk: { S: 'c' } },
  { pk: { S: 'em-twi-idx-upd-type' }, sk: { S: 'a' } },
  { pk: { S: 'em-twi-idx-upd-nonscalar' }, sk: { S: 'b' } },
  { pk: { S: 'em-twi-idx-upd-empty' }, sk: { S: 'c' } },
]

afterAll(async () => {
  await cleanupItems(hashTableDef.name, keysToCleanup)
  await cleanupItems(compositeTableDef.name, compositeKeysToCleanup)
})

describe('TransactWriteItems — exact error messages', { tags: ['transactions', 'data-plane', 'negative-path'] }, () => {
  it('empty TransactItems: full minimum-length error', async () => {
    try {
      await ddb.send(new TransactWriteItemsCommand({ TransactItems: [] }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        "1 validation error detected: Value '[]' at 'transactItems' failed to satisfy constraint: Member must have length greater than or equal to 1",
      )
    }
  })

  it('> 100 actions: anchored regex on the constraint phrase', async () => {
    // AWS echoes the entire TransactItems list into the validation message in
    // the same Java-toString shape used for BatchWriteItem. Pinning the dump
    // verbatim with .toBe() couples the assertion to SDK serialisation, which
    // has changed before. Anchored regex around the structural envelope
    // (`[<dump>]`) and the constraint phrase at the end lets the dump vary
    // without weakening what we actually care about.
    const items = Array.from({ length: 101 }, (_, i) => ({
      Put: {
        TableName: hashTableDef.name,
        Item: { pk: { S: `twi-${i}` } },
      },
    }))
    const expectedPattern = new RegExp(
      `^1 validation error detected: Value '\\[.+\\]' at 'transactItems' failed to satisfy constraint: Member must have length less than or equal to 100$`,
      's',
    )
    try {
      await ddb.send(new TransactWriteItemsCommand({ TransactItems: items }))
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toMatch(expectedPattern)
    }
  })

  it('duplicate target keys in same transaction: full multi-op error', async () => {
    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-dup' } },
              },
            },
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-dup' } },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(
        'Transaction request cannot include multiple operations on one item',
      )
    }
  })

  it('non-existent table: full ResourceNotFoundException message', async () => {
    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: '_conformance_does_not_exist_em_twi',
                Item: { pk: { S: 'x' } },
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(ResourceNotFoundException)
      expect((err as ResourceNotFoundException).name).toBe(
        'ResourceNotFoundException',
      )
      expect((err as ResourceNotFoundException).message).toBe(
        'Requested resource not found',
      )
    }
  })

  it('two failing actions: multi-reason TransactionCanceledException', async () => {
    // Integration: both actions in the transaction violate their conditions.
    // The cancellation summary lists every action's reason code in order, so
    // we can build the expected message from the same array we cross-check
    // against `CancellationReasons[].Code` — fully deterministic, no regex.
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-twi-multi-1' }, attr1: { S: 'exists' } },
      }),
    )
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-twi-multi-2' }, attr1: { S: 'exists' } },
      }),
    )

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-multi-1' }, attr1: { S: 'over' } },
                ConditionExpression: 'attribute_not_exists(pk)',
              },
            },
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-multi-2' }, attr1: { S: 'over' } },
                ConditionExpression: 'attribute_not_exists(pk)',
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(TransactionCanceledException)
      const txErr = err as TransactionCanceledException
      const expectedReasons = [
        'ConditionalCheckFailed',
        'ConditionalCheckFailed',
      ] as const
      expect(txErr.message).toBe(
        `Transaction cancelled, please refer cancellation reasons for specific reasons [${expectedReasons.join(', ')}]`,
      )
      expect(txErr.CancellationReasons?.map((r) => r.Code)).toEqual([
        ...expectedReasons,
      ])
    }
  })

  it('one passing, one failing: positional reason codes (None for the survivor)', async () => {
    // The cancellation summary reports a code per action *positionally* —
    // 'None' for actions that would have succeeded, the actual failure code
    // for actions that did not. Pin the full message and structure so the
    // suite catches emulators that emit a flat 'ConditionalCheckFailed' list
    // without per-action accounting.
    await ddb.send(
      new PutItemCommand({
        TableName: hashTableDef.name,
        Item: { pk: { S: 'em-twi-pos' }, attr1: { S: 'exists' } },
      }),
    )

    try {
      await ddb.send(
        new TransactWriteItemsCommand({
          TransactItems: [
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-pos-new' }, attr1: { S: 'fresh' } },
                ConditionExpression: 'attribute_not_exists(pk)',
              },
            },
            {
              Put: {
                TableName: hashTableDef.name,
                Item: { pk: { S: 'em-twi-pos' }, attr1: { S: 'over' } },
                ConditionExpression: 'attribute_not_exists(pk)',
              },
            },
          ],
        }),
      )
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(TransactionCanceledException)
      const txErr = err as TransactionCanceledException
      const expectedReasons = ['None', 'ConditionalCheckFailed'] as const
      expect(txErr.message).toBe(
        `Transaction cancelled, please refer cancellation reasons for specific reasons [${expectedReasons.join(', ')}]`,
      )
      expect(txErr.CancellationReasons?.map((r) => r.Code)).toEqual([
        ...expectedReasons,
      ])
      expect(txErr.CancellationReasons?.[1].Message).toBe(
        'The conditional request failed',
      )
    }
  })

  // Invalid key value (table or index) inside a transact Put/Update. The error
  // shape splits by fault, captured from real AWS eu-west-2:
  //   - wrong type / non-scalar: TransactionCanceledException, reason code
  //     'ValidationError', reason Message carrying the PutItem-style string;
  //   - empty string: top-level ValidationException (up-front input validation).
  // Index-key messages name gsi1, the alphabetically-first index lsi1sk keys.
  const expectCancelledReason = async (
    command: TransactWriteItemsCommand,
    reasonMessage: string,
  ) => {
    try {
      await ddb.send(command)
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(TransactionCanceledException)
      const txErr = err as TransactionCanceledException
      const expectedReasons = ['ValidationError'] as const
      expect(txErr.message).toBe(
        `Transaction cancelled, please refer cancellation reasons for specific reasons [${expectedReasons.join(', ')}]`,
      )
      expect(txErr.CancellationReasons?.map((r) => r.Code)).toEqual([
        ...expectedReasons,
      ])
      expect(txErr.CancellationReasons?.[0]?.Message).toBe(reasonMessage)
    }
  }

  const expectTopLevelValidation = async (
    command: TransactWriteItemsCommand,
    message: string,
  ) => {
    try {
      await ddb.send(command)
      expect.unreachable('should have thrown')
    } catch (err) {
      expect(err).toBeInstanceOf(DynamoDBServiceException)
      expect((err as DynamoDBServiceException).name).toBe('ValidationException')
      expect((err as DynamoDBServiceException).message).toBe(message)
    }
  }

  it('Put wrong-typed table key: cancelled with full ValidationError reason', async () => {
    await expectCancelledReason(
      new TransactWriteItemsCommand({
        TransactItems: [{ Put: { TableName: hashTableDef.name, Item: { pk: { N: '5' } } } }],
      }),
      'One or more parameter values were invalid: Type mismatch for key pk expected: S actual: N',
    )
  })

  it('Put non-scalar table key: cancelled with full ValidationError reason', async () => {
    await expectCancelledReason(
      new TransactWriteItemsCommand({
        TransactItems: [{ Put: { TableName: hashTableDef.name, Item: { pk: { L: [{ S: 'x' }] } } } }],
      }),
      'One or more parameter values were invalid: Type mismatch for key pk expected: S actual: L',
    )
  })

  it('Put wrong-typed index key: cancelled with full ValidationError reason', async () => {
    await expectCancelledReason(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: compositeTableDef.name,
              Item: { pk: { S: 'em-twi-idx-type' }, sk: { S: 'a' }, lsi1sk: { N: '5' } },
            },
          },
        ],
      }),
      'One or more parameter values were invalid: Type mismatch for Index Key lsi1sk Expected: S Actual: N IndexName: gsi1',
    )
  })

  it('Put non-scalar index key: cancelled with full ValidationError reason', async () => {
    await expectCancelledReason(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: compositeTableDef.name,
              Item: { pk: { S: 'em-twi-idx-nonscalar' }, sk: { S: 'b' }, lsi1sk: { L: [{ S: 'x' }] } },
            },
          },
        ],
      }),
      'One or more parameter values were invalid: Type mismatch for Index Key lsi1sk Expected: S Actual: L IndexName: gsi1',
    )
  })

  it('Update wrong-typed index key: cancelled with full ValidationError reason', async () => {
    await expectCancelledReason(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: compositeTableDef.name,
              Key: { pk: { S: 'em-twi-idx-upd-type' }, sk: { S: 'a' } },
              UpdateExpression: 'SET lsi1sk = :v',
              ExpressionAttributeValues: { ':v': { N: '5' } },
            },
          },
        ],
      }),
      'One or more parameter values were invalid: Type mismatch for Index Key lsi1sk Expected: S Actual: N IndexName: gsi1',
    )
  })

  it('Update non-scalar index key: cancelled with full ValidationError reason', async () => {
    await expectCancelledReason(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: compositeTableDef.name,
              Key: { pk: { S: 'em-twi-idx-upd-nonscalar' }, sk: { S: 'b' } },
              UpdateExpression: 'SET lsi1sk = :v',
              ExpressionAttributeValues: { ':v': { L: [{ S: 'x' }] } },
            },
          },
        ],
      }),
      'One or more parameter values were invalid: Type mismatch for Index Key lsi1sk Expected: S Actual: L IndexName: gsi1',
    )
  })

  it('Put empty-string table key: top-level ValidationException', async () => {
    await expectTopLevelValidation(
      new TransactWriteItemsCommand({
        TransactItems: [{ Put: { TableName: hashTableDef.name, Item: { pk: { S: '' } } } }],
      }),
      'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: pk',
    )
  })

  it('Put empty-string index key: top-level ValidationException', async () => {
    await expectTopLevelValidation(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Put: {
              TableName: compositeTableDef.name,
              Item: { pk: { S: 'em-twi-idx-empty' }, sk: { S: 'c' }, lsi1sk: { S: '' } },
            },
          },
        ],
      }),
      'One or more parameter values are not valid. A value specified for a secondary index key is not supported. The AttributeValue for a key attribute cannot contain an empty string value. IndexName: gsi1, IndexKey: lsi1sk',
    )
  })

  it('Update empty-string index key: top-level ValidationException', async () => {
    await expectTopLevelValidation(
      new TransactWriteItemsCommand({
        TransactItems: [
          {
            Update: {
              TableName: compositeTableDef.name,
              Key: { pk: { S: 'em-twi-idx-upd-empty' }, sk: { S: 'c' } },
              UpdateExpression: 'SET lsi1sk = :v',
              ExpressionAttributeValues: { ':v': { S: '' } },
            },
          },
        ],
      }),
      'One or more parameter values are not valid. The update expression attempted to update a secondary index key to a value that is not supported. The AttributeValue for a key attribute cannot contain an empty string value.',
    )
  })

  // Exact messages for a malformed lookup Key on transact Update / Delete /
  // ConditionCheck (the key-only validation path). Four-region capture (2026-06-23),
  // all invariant, so rung 1 .toBe(). Empty string carries the same message as a Put
  // item key; wrong-type and non-scalar cancel with the schema-mismatch reason, NOT
  // the Put 'Type mismatch for key' form.
  const emptyKeyMsg = 'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty string value. Key: pk'
  const schemaMismatchMsg = 'The provided key element does not match the schema'
  const cmd = (item: unknown) => new TransactWriteItemsCommand({ TransactItems: [item] as never })
  const updKey = (key: unknown) => ({ Update: { TableName: hashTableDef.name, Key: { pk: key }, UpdateExpression: 'SET attr1 = :v', ExpressionAttributeValues: { ':v': { S: 'x' } } } })
  const delKey = (key: unknown) => ({ Delete: { TableName: hashTableDef.name, Key: { pk: key } } })
  const ccKey = (key: unknown) => ({ ConditionCheck: { TableName: hashTableDef.name, Key: { pk: key }, ConditionExpression: 'attribute_not_exists(pk)' } })

  it('Update empty-string Key: top-level empty-value message', () =>
    expectTopLevelValidation(cmd(updKey({ S: '' })), emptyKeyMsg))
  it('Delete empty-string Key: top-level empty-value message', () =>
    expectTopLevelValidation(cmd(delKey({ S: '' })), emptyKeyMsg))
  it('ConditionCheck empty-string Key: top-level empty-value message', () =>
    expectTopLevelValidation(cmd(ccKey({ S: '' })), emptyKeyMsg))

  it('Update wrong-typed Key: cancelled with schema-mismatch reason', () =>
    expectCancelledReason(cmd(updKey({ N: '5' })), schemaMismatchMsg))
  it('Update non-scalar Key: cancelled with schema-mismatch reason', () =>
    expectCancelledReason(cmd(updKey({ L: [{ S: 'x' }] })), schemaMismatchMsg))
  it('Delete wrong-typed Key: cancelled with schema-mismatch reason', () =>
    expectCancelledReason(cmd(delKey({ N: '5' })), schemaMismatchMsg))
  it('Delete non-scalar Key: cancelled with schema-mismatch reason', () =>
    expectCancelledReason(cmd(delKey({ L: [{ S: 'x' }] })), schemaMismatchMsg))
  it('ConditionCheck wrong-typed Key: cancelled with schema-mismatch reason', () =>
    expectCancelledReason(cmd(ccKey({ N: '5' })), schemaMismatchMsg))
  it('ConditionCheck non-scalar Key: cancelled with schema-mismatch reason', () =>
    expectCancelledReason(cmd(ccKey({ L: [{ S: 'x' }] })), schemaMismatchMsg))

  // Empty-binary key values mirror the empty-string cases: a top-level
  // ValidationException that hoists out of the transaction, never a cancellation
  // reason. Real AWS names the same message with 'binary' for 'string'.
  const emptyBinKeyMsg = 'One or more parameter values are not valid. The AttributeValue for a key attribute cannot contain an empty binary value. Key: pk'
  const emptyBin = { B: new Uint8Array([]) }
  const updKeyB = (key: unknown) => ({ Update: { TableName: hashBTableDef.name, Key: { pk: key }, UpdateExpression: 'SET attr1 = :v', ExpressionAttributeValues: { ':v': { S: 'x' } } } })
  const delKeyB = (key: unknown) => ({ Delete: { TableName: hashBTableDef.name, Key: { pk: key } } })
  const ccKeyB = (key: unknown) => ({ ConditionCheck: { TableName: hashBTableDef.name, Key: { pk: key }, ConditionExpression: 'attribute_not_exists(pk)' } })

  it('Put empty-binary item key: top-level empty-value message', () =>
    expectTopLevelValidation(cmd({ Put: { TableName: hashBTableDef.name, Item: { pk: emptyBin } } }), emptyBinKeyMsg))
  it('Update empty-binary Key: top-level empty-value message', () =>
    expectTopLevelValidation(cmd(updKeyB(emptyBin)), emptyBinKeyMsg))
  it('Delete empty-binary Key: top-level empty-value message', () =>
    expectTopLevelValidation(cmd(delKeyB(emptyBin)), emptyBinKeyMsg))
  it('ConditionCheck empty-binary Key: top-level empty-value message', () =>
    expectTopLevelValidation(cmd(ccKeyB(emptyBin)), emptyBinKeyMsg))
})
